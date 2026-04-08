<#
.SYNOPSIS
    Main entry point wrapper for the Drifter-Validated Oil Spill Forecasting system.
#>

$Host.UI.RawUI.WindowTitle = "Drifter-Validated Oil Spill Forecasting"

# Force UTF-8 encoding for console output and standard streams to fix emoji/log artifacts
$OutputEncoding = [Console]::OutputEncoding = [Console]::InputEncoding = [System.Text.Encoding]::UTF8

function Show-Menu {
    Clear-Host
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "   Drifter-Validated Oil Spill Forecasting" -ForegroundColor White
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  1. " -ForegroundColor Yellow -NoNewline; Write-Host "Help"
    Write-Host "  2. " -ForegroundColor Yellow -NoNewline; Write-Host "Run Full End-to-End Pipeline (Phases 1-5 validation)"
    Write-Host "  3. " -ForegroundColor Yellow -NoNewline; Write-Host "Exit"
    Write-Host ""
    $choice = Read-Host "Select an option (1-3)"

    switch ($choice) {
        '1' { Show-Help }
        '2' { Run-E2E }
        '3' { Exit-Script }
        default {
            Write-Host "Invalid option. Please choose 1, 2, or 3." -ForegroundColor Red
            Start-Sleep -Seconds 2
            Show-Menu
        }
    }
}

function Show-Help {
    Clear-Host
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "   HELP" -ForegroundColor White
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  OUTPUT FOLDER STRUCTURE" -ForegroundColor Yellow
    Write-Host "  ------------------------" -ForegroundColor DarkGray
    Write-Host "   output\[CASE]\validation\  " -ForegroundColor Blue -NoNewline; Write-Host "Phase 1 & 5 : " -ForegroundColor Magenta -NoNewline; Write-Host "Drifter maps, NCS scores, Phase 3B FSS metrics"
    Write-Host "   output\[CASE]\ensemble\    " -ForegroundColor Blue -NoNewline; Write-Host "Phase 2     : " -ForegroundColor Magenta -NoNewline; Write-Host "Ensemble member traces, probability_24h/48h/72h PNG+NC, and ensemble_manifest.json"
    Write-Host "   output\[CASE]\benchmark\   " -ForegroundColor Blue -NoNewline; Write-Host "Phase 3A    : " -ForegroundColor Magenta -NoNewline; Write-Host "OpenDrift vs PyGNOME Tracking benchmark"
    Write-Host "   output\[CASE]\weathering\  " -ForegroundColor Blue -NoNewline; Write-Host "Phase 4     : " -ForegroundColor Magenta -NoNewline; Write-Host "Oil weathering mass budgets and shoreline impact limits"
    Write-Host "   data\prepared\[CASE]\      " -ForegroundColor Blue -NoNewline; Write-Host "Prep        : " -ForegroundColor Magenta -NoNewline; Write-Host "Prepared forcing, processed ArcGIS layers, masks, and prepared_input_manifest.csv"
    Write-Host ""
    Write-Host "  WORKFLOW MODES" -ForegroundColor Yellow
    Write-Host "  --------------" -ForegroundColor DarkGray
    Write-Host "   prototype_2016      " -ForegroundColor Blue -NoNewline; Write-Host "Historical drifter-calibration workflow with case-local Phase 1 ranking"
    Write-Host "   mindoro_retro_2023  " -ForegroundColor Blue -NoNewline; Write-Host "Official spill-case workflow using prep + deterministic control + ensemble + Phase 3B"
    Write-Host ""
    Write-Host "  Override the mode for this launcher with:" -ForegroundColor Yellow
    Write-Host "   `$env:WORKFLOW_MODE = 'mindoro_retro_2023'" -ForegroundColor Green
    Write-Host "   `$env:BASELINE_SELECTION_PATH = 'config/phase1_baseline_selection.yaml'" -ForegroundColor Green
    Write-Host "   `$env:BASELINE_RECIPE_OVERRIDE = 'cmems_era5'" -ForegroundColor Green
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host ""
    Pause
    Show-Menu
}

function Run-E2E {
    Clear-Host
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "   STARTING END-TO-END PIPELINE ORCHESTRATOR" -ForegroundColor White
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host ""

    # Ensure required empty directories exist before mounting/running
    foreach ($dir in @("data", "output", "logs")) {
        if (!(Test-Path $dir)) {
            New-Item -ItemType Directory -Force -Path $dir | Out-Null
        }
    }
    
    $logFile = "logs\run_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
    Start-Transcript -Path $logFile -Append | Out-Null

    $startTime = Get-Date
    $exitCode = 0
    $workflowMode = if ($env:WORKFLOW_MODE) { $env:WORKFLOW_MODE } else { "prototype_2016" }

    Write-Host ">>> Starting E2E Pipeline Test..." -ForegroundColor Cyan
    Write-Host ">>> Workflow mode: $workflowMode" -ForegroundColor Cyan

    Write-Host "`n[1/6] Ensuring Docker containers are running..." -ForegroundColor Yellow
    docker-compose up -d

    Write-Host "`n[2/6] Running pipeline-only preparation stage..." -ForegroundColor Yellow
    docker-compose exec -e WORKFLOW_MODE="$workflowMode" -e PIPELINE_PHASE="prep" pipeline python -m src *>&1 | Out-Host
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Prep stage failed!" -ForegroundColor Red
        $exitCode = $LASTEXITCODE
    }

    # 3. RUN PHASE 1 & 2 / MINIMAL OFFICIAL TRACK
    if ($workflowMode -eq "mindoro_retro_2023") {
        Write-Host "`n[3/6] Running minimal official spill-case track: deterministic control + ensemble + Phase 3B..." -ForegroundColor Yellow
        Write-Host "       Benchmark and weathering are optional and will be skipped in this official path." -ForegroundColor DarkGray
    } else {
        Write-Host "`n[3/6] Running historical Phase 1 transport validation and Phase 2 ensemble..." -ForegroundColor Yellow
    }
    if ($exitCode -eq 0) {
        if ($workflowMode -eq "mindoro_retro_2023") {
            docker-compose exec -e WORKFLOW_MODE="$workflowMode" -e PIPELINE_PHASE="official_phase3b" pipeline python -m src *>&1 | Out-Host
        } else {
            docker-compose exec -e WORKFLOW_MODE="$workflowMode" -e PIPELINE_PHASE="1_2" pipeline python -m src *>&1 | Out-Host
        }
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Forecast pipeline failed!" -ForegroundColor Red
            $exitCode = $LASTEXITCODE
        }
    }

    # 4. RUN PHASE 3A
    if ($exitCode -eq 0 -and $workflowMode -ne "mindoro_retro_2023") {
        Write-Host "`n[4/6] Running Phase 3A: Cross-Model Benchmark..." -ForegroundColor Yellow
        docker-compose exec -e WORKFLOW_MODE="$workflowMode" -e PIPELINE_PHASE="benchmark" gnome python -m src *>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Phase 3A failed!" -ForegroundColor Red
            $exitCode = $LASTEXITCODE
        }
    }

    # 5. RUN PHASE 3
    if ($exitCode -eq 0 -and $workflowMode -ne "mindoro_retro_2023") {
        # Note: Runs in the 'gnome' container
        Write-Host "`n[5/6] Running Phase 3: Fate, Weathering & PyGNOME Comparisons..." -ForegroundColor Yellow
        docker-compose exec -e WORKFLOW_MODE="$workflowMode" -e PIPELINE_PHASE="3" gnome python -m src *>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Phase 3 failed!" -ForegroundColor Red
            $exitCode = $LASTEXITCODE
        }
    }

    # 6. RUN PHASE 3B
    if ($exitCode -eq 0 -and $workflowMode -ne "mindoro_retro_2023") {
        Write-Host "`n[6/6] Running Phase 3B: Observational FSS Validation..." -ForegroundColor Yellow
        docker-compose exec -e WORKFLOW_MODE="$workflowMode" -e PIPELINE_PHASE="3b" pipeline python -m src *>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) { 
            Write-Host "[ERROR] Phase 3B failed!" -ForegroundColor Red
            $exitCode = $LASTEXITCODE 
        }
    }

    $endTime = Get-Date

    if ($exitCode -ne 0) {
        Write-Host ""
        Write-Host "[ERROR] Master E2E Pipeline failed. Check the logs near the error." -ForegroundColor Red
        Stop-Transcript | Out-Null
        Write-Host "Log saved to: $logFile" -ForegroundColor DarkGray
        Pause
        Show-Menu
        return
    }

    Write-Host "`n======================================================" -ForegroundColor Cyan
    if ($workflowMode -eq "mindoro_retro_2023") {
        Write-Host "[SUCCESS] OFFICIAL MINIMAL PHASE 3B PATH COMPLETED SUCCESSFULLY!" -ForegroundColor Green
    } else {
        Write-Host "[SUCCESS] ALL E2E PHASES EXECUTED SUCCESSFULLY!" -ForegroundColor Green
    }
    Write-Host "======================================================" -ForegroundColor Cyan
    Write-Host "Please check the 'output/' directory to verify:"
    Write-Host " - data/prepared/[CASE]/  : Prepared forcing/ArcGIS manifest and input inventory" -ForegroundColor Gray
    Write-Host " - output/[CASE]/validation/ : NCS scores, deterministic maps, and Phase 3B FSS metrics"
    Write-Host " - output/[CASE]/ensemble/   : member_*.nc, probability_24h/48h/72h .png + .nc, ensemble_manifest.json"
    if ($workflowMode -eq "mindoro_retro_2023") {
        Write-Host " - output/[CASE]/forecast/   : deterministic_control_*.nc and forecast_manifest.json"
        Write-Host " - output/[CASE]/validation/ : phase3b_run_manifest.json, FSS CSVs, and summary"
        Write-Host " - output/[CASE]/weathering/ : Optional, not required for official Phase 3B" -ForegroundColor Gray
        Write-Host " - output/[CASE]/benchmark/  : Optional, not required for official Phase 3B" -ForegroundColor Gray
    } else {
        Write-Host " - output/[CASE]/weathering/ : Mass-balances limits and Shoreline impact CSVs"
        Write-Host " - output/[CASE]/benchmark/  : Benchmark tracking png and stats" -ForegroundColor Gray
    }

    Write-Host ""
    Write-Host "[SUCCESS] E2E Pipeline Completed!" -ForegroundColor Green
    Write-Host "Started : $($startTime.ToString('HH:mm:ss.ff'))" -ForegroundColor Cyan
    Write-Host "Finished: $($endTime.ToString('HH:mm:ss.ff'))" -ForegroundColor Cyan

    $duration = $endTime - $startTime
    Write-Host ("Total Runtime: {0:D2}h {1:D2}m {2:D2}s" -f $duration.Hours, $duration.Minutes, $duration.Seconds) -ForegroundColor Yellow
    Write-Host "Log saved to: $logFile" -ForegroundColor DarkGray

    Stop-Transcript | Out-Null

    Write-Host ""
    Pause
    Show-Menu
}

function Exit-Script {
    Write-Host ""
    Write-Host "Goodbye." -ForegroundColor DarkGray
    exit 0
}

# Start the script loop
Show-Menu
