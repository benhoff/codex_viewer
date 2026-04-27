[CmdletBinding()]
param(
    [int]$Interval,
    [switch]$RebuildOnStart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Write-AgentLog {
    param([string]$Message)
    Write-Host ("[agent-daemon] {0}" -f $Message)
}

function Get-CodexTrim {
    param([string]$Value)
    if ($null -eq $Value) {
        return ""
    }
    return $Value.Trim()
}

function Import-CodexEnvFile {
    param(
        [string]$Path,
        [hashtable]$ProtectedKeys
    )

    if (-not (Test-Path $Path -PathType Leaf)) {
        return
    }

    foreach ($line in [System.IO.File]::ReadLines($Path)) {
        $stripped = Get-CodexTrim $line
        if (-not $stripped -or $stripped.StartsWith("#")) {
            continue
        }
        if ($stripped.StartsWith("export ")) {
            $stripped = $stripped.Substring(7).TrimStart()
        }

        $separator = $stripped.IndexOf("=")
        if ($separator -lt 1) {
            continue
        }

        $key = Get-CodexTrim ($stripped.Substring(0, $separator))
        if (-not $key -or $ProtectedKeys.ContainsKey($key)) {
            continue
        }

        $value = Get-CodexTrim ($stripped.Substring($separator + 1))
        if ($value.Length -ge 2) {
            $isSingleQuoted = $value.StartsWith("'") -and $value.EndsWith("'")
            $isDoubleQuoted = $value.StartsWith('"') -and $value.EndsWith('"')
            if ($isSingleQuoted -or $isDoubleQuoted) {
                $value = $value.Substring(1, $value.Length - 2)
            } else {
                $commentIndex = $value.IndexOf(" #")
                if ($commentIndex -ge 0) {
                    $value = $value.Substring(0, $commentIndex).TrimEnd()
                }
            }
        }

        Set-Item -Path ("Env:{0}" -f $key) -Value $value
    }
}

function Import-CodexProjectEnv {
    param([string]$ProjectRootPath)

    $protected = @{}
    foreach ($item in Get-ChildItem Env:) {
        $protected[$item.Name] = $true
    }

    Import-CodexEnvFile (Join-Path $ProjectRootPath ".env") $protected
    $environmentName = Get-CodexTrim $env:CODEX_VIEWER_ENV
    if ($environmentName) {
        Import-CodexEnvFile (Join-Path $ProjectRootPath (".env.{0}" -f $environmentName)) $protected
    }
    Import-CodexEnvFile (Join-Path $ProjectRootPath ".env.local") $protected
    if ($environmentName) {
        Import-CodexEnvFile (Join-Path $ProjectRootPath (".env.{0}.local" -f $environmentName)) $protected
    }
}

function Get-CodexPythonInvocation {
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($null -ne $py) {
        return @{
            Command = $py.Source
            PrefixArgs = @("-3")
        }
    }

    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($null -ne $python) {
        return @{
            Command = $python.Source
            PrefixArgs = @()
        }
    }

    $python3 = Get-Command python3 -ErrorAction SilentlyContinue
    if ($null -ne $python3) {
        return @{
            Command = $python3.Source
            PrefixArgs = @()
        }
    }

    throw "Python 3 is required. Install Python and make sure 'py', 'python', or 'python3' is on PATH."
}

function Get-CodexEnvInt {
    param(
        [string]$Name,
        [int]$Default
    )

    $item = Get-Item -Path ("Env:{0}" -f $Name) -ErrorAction SilentlyContinue
    if ($null -eq $item -or [string]::IsNullOrWhiteSpace($item.Value)) {
        return $Default
    }
    return [int]$item.Value
}

Import-CodexProjectEnv $ProjectRoot
Set-Location $ProjectRoot

$depsPath = Join-Path $ProjectRoot ".deps"
if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
    $env:PYTHONPATH = $depsPath
} else {
    $env:PYTHONPATH = "{0};{1}" -f $depsPath, $env:PYTHONPATH
}
$env:CODEX_VIEWER_SYNC_MODE = "remote"

$python = Get-CodexPythonInvocation
$daemonArgs = @() + $python.PrefixArgs + @("-m", "agent_daemon", "daemon")
if ($PSBoundParameters.ContainsKey("Interval")) {
    $daemonArgs += @("--interval", [string]$Interval)
}
if ($RebuildOnStart) {
    $daemonArgs += "--rebuild-on-start"
}

$restartDelay = Get-CodexEnvInt "CODEX_VIEWER_AGENT_RESTART_DELAY" 5
$restartMaxDelay = Get-CodexEnvInt "CODEX_VIEWER_AGENT_RESTART_MAX_DELAY" 60

try {
    while ($true) {
        Write-AgentLog ("starting daemon from {0}" -f $ProjectRoot)
        & $python.Command @daemonArgs
        $status = $LASTEXITCODE

        if ($status -eq 0) {
            exit 0
        }

        if ($status -eq 75) {
            Write-AgentLog "daemon requested restart"
            Start-Sleep -Seconds 1
            continue
        }

        Write-AgentLog ("daemon exited with status={0}; retrying in {1}s" -f $status, $restartDelay)
        Start-Sleep -Seconds $restartDelay
        if ($restartDelay -lt $restartMaxDelay) {
            $restartDelay = [Math]::Min(($restartDelay * 2), $restartMaxDelay)
        }
    }
}
catch [System.Management.Automation.PipelineStoppedException] {
    exit 0
}
