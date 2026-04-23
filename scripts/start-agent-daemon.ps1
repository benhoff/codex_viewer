[CmdletBinding()]
param(
    [int]$Interval,
    [switch]$RebuildOnStart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ScriptPath = Join-Path $ProjectRoot "agent_daemon/start.ps1"

& $ScriptPath @PSBoundParameters
exit $LASTEXITCODE
