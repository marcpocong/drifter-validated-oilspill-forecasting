$ErrorActionPreference = "Continue"

Write-Host "========================================"
Write-Host "Automated Test Suite for Oil Spill Pipeline"
Write-Host "========================================"

# Ensure paths are resolved relative to the script location
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $ScriptDir) { $ScriptDir = $PWD.Path }
$RootDir = (Resolve-Path "$ScriptDir\..").Path

Set-Location $RootDir

$ComposeFile = Join-Path $RootDir "docker-compose.yml"

function Assert-File {
    param([string]$FilePath)
    if (-Not (Test-Path $FilePath)) {
        Write-Host "[FAIL] Expected output not found: $FilePath" -ForegroundColor Red
        exit 1
    } else {
        Write-Host "[PASS] Found $FilePath" -ForegroundColor Green
    }
}

Write-Host "[0/4] Checking and Starting Containers..."
# Use compose up without --build to reuse cached images if they exist.
docker-compose -f $ComposeFile up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to start containers. You may need to run 'docker-compose build' first." -ForegroundColor Red
    exit 1
}

Write-Host "[1/4] Running Phase 1 / 2 (OpenDrift Pipeline)..."
docker-compose -f $ComposeFile exec -T pipeline python -m src
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Pipeline container failed." -ForegroundColor Red
    exit 1
}

Write-Host "[2/4] Validating Phase 1 / 2 Outputs..."
$RunDate = docker-compose -f $ComposeFile exec -T pipeline python -c "import yaml; d=yaml.safe_load(open('config/settings.yaml'))['phase_1_start_date']; print(d[0] if isinstance(d, list) else d)"
$RunDate = $RunDate.Trim()
$CaseDir = Join-Path $RootDir "output\CASE_$RunDate"

Assert-File (Join-Path $CaseDir "validation\validation_ranking.csv")

Write-Host "[3/4] Running Phase 3 (PyGNOME Phase) [Default Case]..."
docker-compose -f $ComposeFile exec -T gnome python -m src
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] GNOME container failed." -ForegroundColor Red
    exit 1
}

Write-Host "Validating Phase 3 Outputs..."
Assert-File (Join-Path $CaseDir "gnome_comparison\gnome_heavy.nc")
Assert-File (Join-Path $CaseDir "gnome_comparison\gnome_light.nc")
Assert-File (Join-Path $CaseDir "weathering\budget_heavy.csv")
Assert-File (Join-Path $CaseDir "weathering\budget_light.csv")
Assert-File (Join-Path $CaseDir "diagnostics\diagnostic_report.csv")

Write-Host "[4/4] Testing Benchmark Runner Interface..."
# We will explicitly run the benchmark phase to ensure it generates spatial scoring logs successfully.
docker-compose -f $ComposeFile exec -e PIPELINE_PHASE=benchmark -T gnome python -m src
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Batch simulation route failed to initialize." -ForegroundColor Red
    exit 1
}

Write-Host "Validating Phase 3A Benchmark Outputs..."
Assert-File (Join-Path $CaseDir "benchmark")

Write-Host "`n========================================"
Write-Host "ALL TESTS PASSED SUCCESSFULLY!"
Write-Host "========================================"
exit 0
