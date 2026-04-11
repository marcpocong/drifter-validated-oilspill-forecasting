# Quickstart

## 1. Start The Containers

```bash
docker-compose up -d
```

## 2. Inspect The Launcher Safely First

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
```

These commands do not rerun expensive science. They show the current launcher catalog and the safe read-only entry IDs.

## 3. Refresh The Current Reproducibility Package

```powershell
.\start.ps1 -Entry phase5_sync -NoPause
```

Equivalent direct command:

```bash
docker-compose exec -T -e PIPELINE_PHASE=phase5_launcher_and_docs_sync pipeline python -m src
```

## 4. Run The Read-Only Audit Utilities

```bash
docker-compose exec -T -e PIPELINE_PHASE=phase1_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase2_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=final_validation_package pipeline python -m src
```

## 5. Run Mindoro Phase 4 Only When Needed

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase4_oiltype_and_shoreline pipeline python -m src
```

This writes to `output/phase4/CASE_MINDORO_RETRO_2023/` and does not overwrite stored Phase 3 validation outputs.

## 6. Use Scientific Reruns Intentionally

Examples:

```powershell
.\start.ps1 -Entry mindoro_reportable_core -NoPause
.\start.ps1 -Entry dwh_reportable_bundle -NoPause
```

These are intentional rerun paths. Do not use them casually for status inspection.

## 7. Current Caution Notes

- Phase 1 is architecture-audited, but the final multi-year production rerun is still needed.
- Phase 2 is scientifically usable, but not scientifically frozen.
- Phase 4 is reportable now for Mindoro, but inherited-provisional from the upstream Phase 1/2 status.
- Prototype mode remains for debugging/regression only.
