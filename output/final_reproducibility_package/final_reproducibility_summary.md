# Final Reproducibility Summary

This package synchronizes launcher/menu behavior, documentation, and reproducibility indexes around the current local repo state without rerunning the expensive scientific branches by default.

## Launcher Entrypoint

- PowerShell entrypoint: `./start.ps1 -List -NoPause`
- Source-of-truth launcher matrix: `config/launcher_matrix.json`
- Safe read-only launcher IDs: `phase1_audit`, `phase2_audit`, `final_validation_package`, `phase5_sync`

## Phase Status Highlights

- `phase1` / `phase1_regional_baseline`: Architecture audited; the final 2016-2022 production rerun is still needed.
- `phase2` / `phase2_machine_readable_forecast`: Scientifically usable as implemented, but not yet frozen.
- `phase3a` / `A`: Mindoro Phase 3A remains scientifically informative as a comparator benchmark. 
- `phase3b` / `B1`: Mindoro strict March 6 is a hard sparse stress test and should not be treated as broad-support validation. fss_1km=0.0000, fss_3km=0.0000, fss_5km=0.0000, fss_10km=0.0000
- `phase3b` / `B2`: Mindoro broader public-support remains scientifically informative support material, not a replacement for B1. fss_1km=0.1722, fss_3km=0.2004, fss_5km=0.2166, fss_10km=0.2438
- `phase3c` / `C1`: DWH deterministic transfer validation is a reportable external-case success. fss_1km=0.5033, fss_3km=0.5523, fss_5km=0.5700, fss_10km=0.6018
- `phase3c` / `C2`: DWH ensemble p50 is reportable and leads the overall mean FSS comparison under the current case definition. fss_1km=0.4997, fss_3km=0.5299, fss_5km=0.5467, fss_10km=0.5790
- `phase3c` / `C3`: DWH PyGNOME remains reportable as a comparator, not as truth. fss_1km=0.3197, fss_3km=0.3495, fss_5km=0.3689, fss_10km=0.4068
- `phase4` / `mindoro_phase4`: Mindoro Phase 4 is scientifically reportable now, but inherited-provisional from upstream Phase 1/2 state.
- `phase5` / `phase5_sync`: Launcher, docs, and reproducibility packaging are synchronized around the current repo state without rerunning expensive science.

## Packaging Sync Scope

- Existing scientific Mindoro and DWH outputs were reused and not recomputed here.
- The existing `output/final_validation_package/` bundle was reused rather than rebuilt from scratch.
- Mindoro Phase 4 now participates in the reproducibility/package layer via the current `phase4_run_manifest.json` and verdict bundle.

## Key Artifacts

- Phase status registry: `output/final_reproducibility_package/final_phase_status_registry.csv`
- Reproducibility manifest: `output/final_reproducibility_package/final_reproducibility_manifest.json`
- Packaging sync memo: `output/final_reproducibility_package/phase5_packaging_sync_memo.md`
- Launcher guide: `output/final_reproducibility_package/launcher_user_guide.md`
