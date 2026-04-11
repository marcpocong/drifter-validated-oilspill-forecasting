# Phase 4 Methodology Sync Memo

This Phase 4 run finalizes the oil-type fate and shoreline-impact workflow on top of the current reportable transport framework without changing the stored Phase 3 validation outputs.

## Workflow Decisions

- Existing weathering path audit: `partially_implemented_but_needs_refactor`.
- Reused weathering path: `yes`.
- Selected transport branch: `CASE_MINDORO_RETRO_2023/convergence_after_shoreline/elements_100000` (`shoreline_aware_convergence_recommended`).
- Selected transport loading audit: `output/CASE_MINDORO_RETRO_2023/convergence_after_shoreline/elements_100000/forecast/phase2_loading_audit.json`.
- Transport forcing reuse mode: `phase2_loading_audit_deterministic_control_used_paths`.
- Canonical shoreline artifact reused: `data_processed/grids/shoreline_segments.gpkg`.
- Scenario registry: `config/phase4_oil_scenarios.csv` with scenarios `lighter_oil`, `fixed_base_medium_heavy_proxy`, `heavier_oil`.

## Honesty / Provenance

- Phase 1 frozen story complete: `False`.
- Phase 2 scientifically usable: `True`.
- Phase 2 scientifically frozen: `False`.
- Provisional inherited from transport: `True`.
- Official recipe family locally available: `['cmems_era5', 'hycom_era5']`.
- Legacy recipe drift still present in runtime/config space: `True`.
- Scenario mass-balance follow-up required: `True`.

## DWH Appendix Hook

- DWH Phase 4 pilot status: deferred in this patch so Mindoro Phase 4 could be finalized cleanly first.
