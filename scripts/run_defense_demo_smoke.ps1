param(
    [switch]$Quick,
    [switch]$RequireDocker,
    [switch]$RequireDashboard,
    [switch]$NoScience
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..")
Set-Location $repoRoot

if (-not (Test-Path ".\start.ps1") -or -not (Test-Path ".\config\launcher_matrix.json")) {
    throw "Could not locate the repo root from $scriptDir"
}

$env:DEFENSE_SMOKE_TEST = "1"
if (-not $env:INPUT_CACHE_POLICY) {
    $env:INPUT_CACHE_POLICY = "reuse_if_valid"
}
if ($NoScience) {
    $env:DEFENSE_NO_SCIENCE = "1"
}

$pythonArgs = @(".\scripts\defense_readiness_check.py")
if ($Quick) {
    $pythonArgs += "--quick"
} else {
    $modeArg = if ($RequireDocker) { "docker" } else { "local" }
    $pythonArgs += "--mode"
    $pythonArgs += $modeArg
}
if ($RequireDocker) {
    $pythonArgs += "--require-docker"
}
if ($RequireDashboard) {
    $pythonArgs += "--require-dashboard"
}
$pythonArgs += "--write-report"

Write-Host ""
Write-Host "Defense demo smoke runner" -ForegroundColor Cyan
Write-Host "Repo root: $repoRoot" -ForegroundColor DarkGray
Write-Host "Command: python $($pythonArgs -join ' ')" -ForegroundColor DarkGray
Write-Host ""

& python @pythonArgs
$exitCode = $LASTEXITCODE

$reportJson = Join-Path $repoRoot "output\defense_readiness\defense_readiness_report.json"
$reportMd = Join-Path $repoRoot "output\defense_readiness\defense_readiness_report.md"

if (Test-Path $reportJson) {
    $report = Get-Content $reportJson -Raw | ConvertFrom-Json
    Write-Host ""
    Write-Host "Defense readiness summary" -ForegroundColor Cyan
    Write-Host ("PASS: {0}" -f $report.summary.pass_count) -ForegroundColor Green
    Write-Host ("WARN: {0}" -f $report.summary.warn_count) -ForegroundColor Yellow
    Write-Host ("FAIL: {0}" -f $report.summary.fail_count) -ForegroundColor Red
    Write-Host ("Report: {0}" -f $reportMd) -ForegroundColor White

    if ($report.hard_failures.Count -gt 0) {
        Write-Host ""
        Write-Host "Hard failures:" -ForegroundColor Red
        foreach ($item in $report.hard_failures) {
            Write-Host ("- {0}" -f $item) -ForegroundColor Red
        }
    }

    if ($report.warnings.Count -gt 0) {
        Write-Host ""
        Write-Host "Warnings:" -ForegroundColor Yellow
        foreach ($item in $report.warnings) {
            Write-Host ("- {0}" -f $item) -ForegroundColor Yellow
        }
    }
} else {
    Write-Host "Readiness report was not created." -ForegroundColor Red
    if ($exitCode -eq 0) {
        $exitCode = 1
    }
}

exit $exitCode
