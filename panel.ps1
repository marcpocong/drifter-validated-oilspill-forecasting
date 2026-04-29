<#
.SYNOPSIS
    Defense-safe wrapper for the interactive launcher panel mode.

.DESCRIPTION
    This wrapper intentionally performs no science itself. It locates this
    repository from the script path, runs start.ps1 with -Panel, forwards any
    additional arguments, and exits with the launcher's exit code.
#>

param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ForwardedArguments
)

$ErrorActionPreference = "Stop"

$scriptPath = if ($PSCommandPath) {
    $PSCommandPath
} elseif ($MyInvocation.MyCommand.Path) {
    $MyInvocation.MyCommand.Path
} else {
    Join-Path (Get-Location).Path "panel.ps1"
}

$repoRoot = Split-Path -Parent $scriptPath
$startScript = Join-Path $repoRoot "start.ps1"
$previousLocation = Get-Location

try {
    Set-Location $repoRoot
    $powerShellHost = [System.Diagnostics.Process]::GetCurrentProcess().Path
    $startArguments = @(
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        $startScript,
        "-Panel"
    ) + @($ForwardedArguments)

    & $powerShellHost @startArguments
    $exitCode = $LASTEXITCODE
    if ($null -eq $exitCode) {
        $exitCode = 0
    }
}
catch {
    Write-Error $_
    $exitCode = 1
}
finally {
    Set-Location $previousLocation
}

exit $exitCode
