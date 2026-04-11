# Drifter-Validated Oil Spill Forecasting System

Thesis workflow for transport validation, machine-readable forecast generation, public-observation scoring, external-case transfer validation, oil-type shoreline interpretation, and final reproducibility packaging using OpenDrift and PyGNOME.

## Plain-Language Status

- Phase 1: architecture audited, but the full 2016-2022 regional production rerun is still needed before the frozen baseline story is complete.
- Phase 2: scientifically usable as implemented, but not scientifically frozen.
- Mindoro Phase 3: scientifically informative and reportable, with strict March 6 retained as the hard sparse stress test and broader public support kept separate.
- DWH Phase 3C: external rich-data transfer validation success under the current case definition.
- Phase 4: scientifically reportable now for Mindoro, but inherited-provisional from the upstream Phase 1/2 freeze story.
- Phase 5: launcher, docs, and reproducibility/package synchronization layer.

## Workflow Lanes

- `prototype_2016`: legacy debug/regression workflow. This is preserved intentionally, but it is not the final Chapter 3 Phase 1 study.
- `mindoro_retro_2023`: main Philippine thesis lane for official forecast products, Phase 3 scoring, and Phase 4 oil-type shoreline interpretation.
- `dwh_retro_2010`: external rich-data transfer-validation lane for deterministic, ensemble, and PyGNOME comparator work.

## Current Launcher

The current launcher entrypoint is [start.ps1](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/start.ps1). It is driven by [config/launcher_matrix.json](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/config/launcher_matrix.json) and separates:

- scientific / reportable reruns
- sensitivity / appendix branches
- read-only packaging and audit utilities
- legacy prototype tracks

Safe first commands:

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
.\start.ps1 -Entry phase5_sync -NoPause
```

Intentional scientific reruns remain available, but they are no longer hidden behind a single stale "Mindoro full" option.

## Safe Phase Commands

Read-only utilities:

```bash
docker-compose exec -T -e PIPELINE_PHASE=phase1_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase2_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=final_validation_package pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase5_launcher_and_docs_sync pipeline python -m src
```

Mindoro Phase 4:

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase4_oiltype_and_shoreline pipeline python -m src
```

Intentional scientific reruns:

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=official_phase3b pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_run pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_ensemble_comparison pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_dwh_pygnome_comparator gnome python -m src
```

## Scientific Boundaries To Keep

- Do not treat the old three-case prototype logic as the final Phase 1 study.
- Keep historical/regional transport validation separate from spill-case validation.
- Do not relabel thresholded ensemble products: `mask_p50` and `mask_p90` semantics are unchanged.
- Do not mix Phase 4 oil-type sensitivity into Phase 2 or Phase 3 baseline products.
- Do not pretend Phase 1 or Phase 2 are fully frozen when they are not.

## Main Output Areas

- [output/phase1_finalization_audit](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/phase1_finalization_audit): read-only Phase 1 architecture audit.
- [output/phase2_finalization_audit](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/phase2_finalization_audit): read-only Phase 2 semantics/manifests audit.
- [output/CASE_MINDORO_RETRO_2023](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/CASE_MINDORO_RETRO_2023): official Mindoro deterministic, ensemble, and scoring outputs.
- [output/CASE_DWH_RETRO_2010_72H](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/CASE_DWH_RETRO_2010_72H): DWH transfer-validation outputs.
- [output/phase4/CASE_MINDORO_RETRO_2023](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/phase4/CASE_MINDORO_RETRO_2023): Mindoro Phase 4 oil budgets, shoreline arrival timing, shoreline segments, oil-type comparison, and verdict bundle.
- [output/final_validation_package](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/final_validation_package): frozen thesis validation package reused by later packaging work.
- [output/final_reproducibility_package](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/final_reproducibility_package): Phase 5 software, manifest, output, log, status, and launcher/package sync indexes.

## Git Hygiene

The repo now keeps bulky case output trees ignored by default, while allowing lightweight audit/package artifacts to remain trackable where appropriate:

- Phase 1 audit outputs
- Phase 2 audit outputs
- Mindoro Phase 4 summary artifacts
- final validation package summaries
- final reproducibility package summaries

Large raw data, scientific raster stacks, NetCDF outputs, and bulk case rerun artifacts remain excluded.

## Documentation Map

- [docs/PHASE_STATUS.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/PHASE_STATUS.md)
- [docs/ARCHITECTURE.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/ARCHITECTURE.md)
- [docs/OUTPUT_CATALOG.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/OUTPUT_CATALOG.md)
- [docs/QUICKSTART.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/QUICKSTART.md)
- [docs/COMMAND_MATRIX.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/COMMAND_MATRIX.md)
- [docs/LAUNCHER_USER_GUIDE.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/LAUNCHER_USER_GUIDE.md)

## Contact

For questions or issues, contact `arjayninosaguisa@gmail.com`.

## Status Stamp

- Last updated: 2026-04-11
- Current sync state: Phase 5 launcher/docs/package layer added
- Biggest remaining scientific blocker: the missing full 2016-2022 accepted/rejected drogued 72 h segment registry still blocks the final frozen Phase 1 baseline story
