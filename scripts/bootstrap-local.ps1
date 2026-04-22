[CmdletBinding()]
param(
    [switch]$SkipCss
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Write-BootstrapLog {
    param([string]$Message)
    Write-Host ("[bootstrap-local] {0}" -f $Message)
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

$python = Get-CodexPythonInvocation

Push-Location $ProjectRoot
try {
    Write-BootstrapLog "installing Python dependencies into .deps"
    $pipArgs = @() + $python.PrefixArgs + @(
        "-m",
        "pip",
        "install",
        "--upgrade",
        "--target",
        (Join-Path $ProjectRoot ".deps"),
        "-r",
        (Join-Path $ProjectRoot "requirements.txt")
    )
    & $python.Command @pipArgs
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    if ($SkipCss) {
        Write-BootstrapLog "skipping CSS build"
        exit 0
    }

    $cssPath = Join-Path $ProjectRoot "codex_session_viewer/static/app.css"
    if (Test-Path $cssPath) {
        Write-BootstrapLog "found prebuilt CSS; skipping Tailwind build"
        exit 0
    }

    $npm = Get-Command npm -ErrorAction SilentlyContinue
    if ($null -eq $npm) {
        Write-BootstrapLog "codex_session_viewer/static/app.css is missing and npm is not installed"
        Write-BootstrapLog "install Node.js and rerun this script, or use Docker instead"
        exit 1
    }

    Write-BootstrapLog "building CSS assets"
    & $npm.Source "ci"
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
    & $npm.Source "run" "build:css"
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
