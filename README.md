# Drifter-Validated Oil Spill Forecasting System

Thesis workflow for oil-spill transport validation, forecast product generation, public-observation scoring, cross-model benchmarking, and external-case transfer validation using OpenDrift and PyGNOME.

## Project Overview

This repository now supports three distinct workflow lanes:

- `prototype_2016`: the original multi-date drifter-calibration workflow kept for debugging and regression checks.
- `mindoro_retro_2023`: the main Philippine thesis case, including strict and broader public-observation validation tracks plus benchmarking and sensitivity branches.
- `dwh_retro_2010`: the Deepwater Horizon external rich-data transfer-validation case, including deterministic, ensemble, and PyGNOME comparator branches.

The finished thesis structure reflected in the current codebase is:

1. Phase 1 = Transport Validation and Baseline Configuration Selection
2. Phase 2 = Standardized Machine-Readable Forecast Product Generation
3. Phase 3A = Mindoro Cross-Model Spatial Benchmarking with PyGNOME
4. Phase 3B1 = Mindoro Strict Single-Date Observation Stress Test (March 6)
5. Phase 3B2 = Mindoro Broader Public-Observation / Event-Corridor Support
6. Phase 3C = External Rich-Data Spill Transfer Validation (Deepwater Horizon 2010)
7. Phase 4 = Oil-Type Fate and Shoreline Impact Analysis
8. Phase 5 = Reproducibility, Packaging, and Deliverables

## Current Scientific Status

The repository is no longer in the older "Mindoro Phase 3B only" state. The current project state includes:

- completed Mindoro Phase 3A / Phase 3B benchmark, strict public-observation, broader-support, appendix, and sensitivity work
- completed DWH Phase 3C setup, scientific forcing readiness, deterministic run, ensemble comparison, and PyGNOME comparator
- completed read-only thesis packaging under `output/final_validation_package/`

Headline results from the frozen final package:

- Mindoro strict March 6 (B1): FSS(1/3/5/10 km) = `0.0000 / 0.0000 / 0.0000 / 0.0000`
- Mindoro broader-support appendix (B2): FSS(1/3/5/10 km) = `0.1722 / 0.2004 / 0.2166 / 0.2438`
- DWH deterministic event corridor (C1): FSS(1/3/5/10 km) = `0.5033 / 0.5523 / 0.5700 / 0.6018`
- DWH ensemble p50 event corridor (C2): FSS(1/3/5/10 km) = `0.4997 / 0.5299 / 0.5467 / 0.5790`
- DWH PyGNOME comparator event corridor (C3): FSS(1/3/5/10 km) = `0.3197 / 0.3495 / 0.3689 / 0.4068`

Interpretation guardrails:

- Mindoro remains the main Philippine case.
- Mindoro strict March 6 is a sparse hard stress test, not a broad-support summary.
- DWH public observation-derived masks remain the truth source for Phase 3C.
- PyGNOME is a comparator, not truth.
- On DWH, OpenDrift outperforms the PyGNOME comparator under the current case definition.
- On DWH, deterministic OpenDrift is strongest on the May 21-23 event corridor, while ensemble p50 is strongest on overall mean FSS.

## Technology Stack

- Python 3.10
- OpenDrift / OpenOil
- PyGNOME
- Docker / Docker Compose
- NumPy, Pandas, Xarray, Rasterio, GeoPandas, Shapely
- Matplotlib / Cartopy

## Containers and Execution Model

This repository uses a dual-container setup:

- `pipeline` container:
  - preparation and ingestion
  - OpenDrift transport / ensemble workflows
  - Mindoro public-observation and DWH scientific forcing / scientific scoring paths
  - final validation packaging
- `gnome` container:
  - PyGNOME benchmark and comparison paths
  - Mindoro PyGNOME public comparison
  - DWH PyGNOME comparator

All downloads and preprocessing are pipeline-owned. The gnome container reads prepared artifacts and fails fast if required inputs are missing.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Git
- About 8 GB RAM recommended for ensemble work

### Build and Start

```bash
docker-compose up -d --build
```

### Preparation

Run preparation before any read-only scoring or benchmark phase that depends on prepared inputs:

```bash
docker-compose exec -T -e PIPELINE_PHASE=prep pipeline python -m src
```

## Workflow Modes

The active workflow is controlled by `WORKFLOW_MODE`:

- `prototype_2016`
- `mindoro_retro_2023`
- `dwh_retro_2010`

Default mode is still defined in [config/settings.yaml](config/settings.yaml), but explicit environment overrides are recommended for reproducibility.

## Common Commands

### Prototype 2016

```bash
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prep pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=3 gnome python -m src
```

### Mindoro Main Thesis Case

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=prep pipeline python -m src

docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=official_phase3b pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_multidate_public pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=public_obs_appendix pipeline python -m src

docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=benchmark gnome python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=pygnome_public_comparison gnome python -m src
```

Selected Mindoro appendix and sensitivity phases:

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=recipe_sensitivity_r1_multibranch pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=init_mode_sensitivity_r1 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=source_history_reconstruction_r1 pipeline python -m src
```

### Deepwater Horizon External Case

```bash
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_setup pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=dwh_phase3c_scientific_forcing_ready pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_run pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_ensemble_comparison pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_dwh_pygnome_comparator gnome python -m src
```

### Final Read-Only Thesis Package

```bash
docker-compose exec -T -e PIPELINE_PHASE=final_validation_package pipeline python -m src
```

## Important Pipeline Phases

Key phase selectors currently wired in [src/__main__.py](src/__main__.py):

- `prep`
- `1_2`
- `official_phase3b`
- `benchmark`
- `phase3b_multidate_public`
- `public_obs_appendix`
- `pygnome_public_comparison`
- `phase3c_external_case_setup`
- `dwh_phase3c_forcing_adapter_and_non_scientific_smoke_forecast`
- `dwh_phase3c_scientific_forcing_ready`
- `phase3c_external_case_run`
- `phase3c_external_case_ensemble_comparison`
- `phase3c_dwh_pygnome_comparator`
- `final_validation_package`

## Project Structure

The repository is now organized around configuration, shared data, case-specific outputs, and final packaging rather than the older flat `validation/ensemble/weathering` story.

```text
drifter-validated-oilspill-forecasting-rc-v1.0/
|-- config/
|   |-- settings.yaml
|   |-- recipes.yaml
|   |-- ensemble.yaml
|   |-- oil.yaml
|   |-- phase1_baseline_selection.yaml
|   |-- case_mindoro_retro_2023.yaml
|   `-- case_dwh_retro_2010_72h.yaml
|-- data/
|   |-- drifters/
|   |-- forcing/
|   `-- shoreline/
|-- data_processed/
|   |-- grids/
|   |-- prepared/
|   `-- ...
|-- docker/
|   |-- pipeline/
|   `-- gnome/
|-- output/
|   |-- CASE_2016-09-01/
|   |-- CASE_2016-09-06/
|   |-- CASE_2016-09-17/
|   |-- CASE_MINDORO_RETRO_2023/
|   |   |-- forecast/
|   |   |-- ensemble/
|   |   |-- phase3b/
|   |   |-- phase3b_multidate_public/
|   |   |-- public_obs_appendix/
|   |   |-- pygnome_public_comparison/
|   |   |-- recipe_sensitivity_r1_multibranch/
|   |   |-- init_mode_sensitivity_r1/
|   |   `-- source_history_reconstruction_r1/
|   |-- CASE_DWH_RETRO_2010_72H/
|   |   |-- phase3c_external_case_setup/
|   |   |-- dwh_phase3c_scientific_forcing_ready/
|   |   |-- phase3c_external_case_run/
|   |   |-- phase3c_external_case_ensemble_comparison/
|   |   `-- phase3c_dwh_pygnome_comparator/
|   `-- final_validation_package/
|       |-- final_validation_main_table.csv
|       |-- final_validation_case_registry.csv
|       |-- final_validation_benchmark_table.csv
|       |-- final_validation_observation_table.csv
|       |-- final_validation_limitations.csv
|       |-- final_validation_claims_guardrails.md
|       |-- final_validation_chapter_sync_memo.md
|       |-- final_validation_interpretation_memo.md
|       `-- final_validation_summary.md
|-- src/
|   |-- core/
|   |-- helpers/
|   |-- models/
|   |-- services/
|   |   |-- validation.py
|   |   |-- ensemble.py
|   |   |-- benchmark.py
|   |   |-- pygnome_public_comparison.py
|   |   |-- phase3c_external_case_setup.py
|   |   |-- dwh_phase3c_scientific_forcing.py
|   |   |-- phase3c_external_case_run.py
|   |   |-- phase3c_external_case_ensemble_comparison.py
|   |   |-- phase3c_dwh_pygnome_comparator.py
|   |   `-- final_validation_package.py
|   `-- __main__.py
|-- tests/
|-- docker-compose.yml
|-- start.ps1
`-- README.md
```

## Output Directory Guide

### Mindoro

`output/CASE_MINDORO_RETRO_2023/` contains the main Philippine case outputs, including:

- deterministic forecast products
- ensemble products
- strict Phase 3B scoring outputs
- broader public-observation support outputs
- PyGNOME comparison outputs
- appendix and sensitivity outputs

### DWH

`output/CASE_DWH_RETRO_2010_72H/` contains the external transfer-validation case outputs, including:

- setup and observation-ingestion artifacts
- scientific forcing manifests
- deterministic scientific scoring outputs
- deterministic-vs-ensemble comparison outputs
- PyGNOME comparator outputs

### Final Package

`output/final_validation_package/` is the frozen thesis-usable summary layer built from finished scientific artifacts without rerunning the expensive branches.

## Configuration

Key configuration files:

- [config/settings.yaml](config/settings.yaml): default workflow mode and case-file mappings
- [config/case_mindoro_retro_2023.yaml](config/case_mindoro_retro_2023.yaml): official Mindoro case definition
- [config/case_dwh_retro_2010_72h.yaml](config/case_dwh_retro_2010_72h.yaml): DWH Phase 3C case definition
- [config/recipes.yaml](config/recipes.yaml): OpenDrift forcing recipes
- [config/ensemble.yaml](config/ensemble.yaml): ensemble perturbation settings
- [config/oil.yaml](config/oil.yaml): oil/weathering settings

## Reproducibility Notes

- `data/` is the input data lake and should be treated as read-only after download/prep.
- `output/` is reproducible results storage; case folders can be rebuilt from inputs and configs.
- `final_validation_package` is intentionally read-only with respect to completed scientific outputs.
- Smoke-only DWH artifacts remain separated from scientific-ready DWH outputs.

## Citation

If you use this project in research, update the citation metadata to match your final thesis submission:

```bibtex
@thesis{year_author,
  title={Drifter-Validated Oil Spill Forecasting System},
  author={Your Name},
  year={2026},
  school={Your Institution}
}
```

## Contact

For questions or issues, please contact: arjayninosaguisa@gmail.com

---

**Last Updated**: 2026-04-11  
**Status**: Final validation package complete; README now reflects the finished Mindoro + DWH + thesis-packaging state.
