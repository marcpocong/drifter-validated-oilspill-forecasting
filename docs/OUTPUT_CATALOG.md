# Output Catalog

## Read-Only Audit Outputs

### `output/phase1_finalization_audit/`

Purpose:
Read-only Phase 1 architecture audit against the stable Chapter 3 target.

Expected files:

- `phase1_finalization_status.csv`
- `phase1_finalization_status.json`
- `phase1_finalization_memo.md`
- `phase1_final_verdict.md`

Interpretation:

- use this directory to understand whether the Phase 1 architecture is in place
- do not use it as evidence that the full 2016-2022 regional rerun has already been completed

### `output/phase2_finalization_audit/`

Purpose:
Read-only Phase 2 semantics/manifests audit.

Expected files:

- `phase2_finalization_status.csv`
- `phase2_finalization_status.json`
- `phase2_finalization_memo.md`
- `phase2_output_catalog.csv`
- `phase2_final_verdict.md`

Interpretation:

- use this directory to understand whether Phase 2 is scientifically usable now
- do not use it as evidence that the upstream Phase 1 frozen baseline story is already complete

## Phase 4 Outputs

### `output/phase4/CASE_MINDORO_RETRO_2023/`

Purpose:
Dedicated Mindoro Phase 4 oil-type and shoreline-impact bundle.

Expected files:

- `phase4_oil_budget_timeseries_<scenario>.csv`
- `phase4_oil_budget_summary.csv`
- `phase4_shoreline_arrival.csv`
- `phase4_shoreline_segments.csv`
- `phase4_oiltype_comparison.csv`
- `phase4_run_manifest.json`
- `qa_phase4_shoreline_impacts.png`
- `qa_phase4_oiltype_comparison.png`
- `phase4_methodology_sync_memo.md`
- `phase4_final_verdict.md`

Interpretation:

- this directory is separate from Phase 2 and Phase 3 baseline validation products
- the manifest records inherited provisional status from the current transport framework
- the shoreline outputs are tied to the real shoreline-aware workflow, not to a fake standalone shoreline layer

## Frozen Thesis Validation Package

### `output/final_validation_package/`

Purpose:
Frozen thesis validation bundle built from completed scientific outputs without recomputing them.

Representative files:

- `final_validation_manifest.json`
- `final_validation_case_registry.csv`
- `final_validation_main_table.csv`
- `final_validation_benchmark_table.csv`
- `final_validation_observation_table.csv`
- `final_validation_limitations.csv`
- `final_validation_claims_guardrails.md`
- `final_validation_chapter_sync_memo.md`
- `final_validation_interpretation_memo.md`
- `final_validation_summary.md`

Interpretation:

- this remains the thesis-facing summary bundle
- Phase 5 reuses it rather than replacing it

## Final Reproducibility Package

### `output/final_reproducibility_package/`

Purpose:
Phase 5 launcher/docs/reproducibility/package synchronization layer built from the current local repo state.

Expected files:

- `software_versions.csv`
- `final_case_registry.csv`
- `final_config_snapshot_index.csv`
- `final_manifest_index.csv`
- `final_output_catalog.csv`
- `final_log_index.csv`
- `final_phase_status_registry.csv`
- `final_reproducibility_summary.md`
- `final_reproducibility_manifest.json`
- `phase5_packaging_sync_memo.md`
- `phase5_final_verdict.md`
- `launcher_user_guide.md`

Interpretation:

- use this directory to audit current reproducibility/package state
- this layer is intentionally non-scientific and does not recompute scientific scores
- the phase-status registry is the machine-readable summary of what is reportable, frozen, or inherited-provisional

## Case Output Trees

### `output/CASE_MINDORO_RETRO_2023/`

Contains:

- official deterministic forecast products
- ensemble products
- strict Phase 3B scoring outputs
- broader public-support outputs
- benchmark and sensitivity branches

### `output/CASE_DWH_RETRO_2010_72H/`

Contains:

- external-case setup products
- scientific forcing readiness products
- deterministic transfer-validation products
- ensemble comparison products
- PyGNOME comparator products

## Legacy Prototype Outputs

### `output/CASE_2016-09-01/`
### `output/CASE_2016-09-06/`
### `output/CASE_2016-09-17/`

Purpose:
Legacy prototype debugging/regression outputs.

Guardrail:

- these are not the final Phase 1 regional corpus
- these do not replace the accepted/rejected segment registry required for the final frozen baseline story

## Trackable vs Excluded Artifacts

Intentionally trackable where appropriate:

- small CSV/JSON/MD/PNG audit and package artifacts
- final validation summaries
- final reproducibility summaries
- Mindoro Phase 4 summary artifacts

Intentionally excluded:

- bulky raw data
- bulk case output trees
- large scientific rasters and NetCDF files
- local-only transient logs beyond summary indexes
