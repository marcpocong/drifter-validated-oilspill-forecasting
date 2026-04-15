# Launcher User Guide

## Purpose

`start.ps1` is the current user-facing workflow entrypoint for this repo. It reads `config/launcher_matrix.json` and groups launcher entries into:

- reportable workflows
- support and archive workflows
- read-only audit and packaging workflows
- legacy support workflows

The launcher is the primary path for interactive runs. The Streamlit UI stays separate and launches directly.

## Current Startup Paths

List the current launcher entries:

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
```

Run one workflow interactively through the launcher:

```powershell
.\start.ps1 -Entry <entry_id>
```

Use `-NoPause` only when you intentionally want the launcher to finish without the final pause.

Run one phase prompt-free inside the container:

```bash
docker-compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
```

Launch the read-only UI directly:

```bash
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

## Main Parameters

- `-List`: print the current launcher catalog grouped by category
- `-Help`: print usage guidance and current guardrails
- `-Entry <entry_id>`: run one launcher entry from the matrix
- `-NoPause`: skip the final pause after the launcher finishes

Runtime environment controls:

- `FORCING_OUTAGE_POLICY=default|continue_degraded|fail_hard`
- `FORCING_SOURCE_BUDGET_SECONDS=<seconds>` with default `300`
- `INPUT_CACHE_POLICY=default|reuse_if_valid|force_refresh`

Interactive launcher runs ask once per entry for:

- the forcing wait budget, when the entry can hit forcing providers
- whether to reuse validated local input caches or force refresh, when eligible caches already exist

Prompt-free container runs with `-T` do not ask those questions. They resolve the startup policy silently and print it at process start for promptable phases.

## Entry Groups

Read-only audit and packaging entries:

- `phase1_audit`
- `phase2_audit`
- `final_validation_package`
- `phase5_sync`
- `trajectory_gallery`
- `trajectory_gallery_panel`
- `figure_package_publication`
- `prototype_legacy_final_figures`

Reportable entries:

- `phase1_production_rerun`
- `mindoro_phase3b_primary_public_validation`
- `mindoro_reportable_core`
- `dwh_reportable_bundle`

Support and archive entries:

- `mindoro_phase4_only`
- `mindoro_appendix_sensitivity_bundle`
- `phase1_mindoro_focus_pre_spill_experiment`
- `mindoro_march13_14_phase1_focus_trial`
- `mindoro_march6_recovery_sensitivity`
- `mindoro_march23_extended_public_stress_test`

Legacy support entries:

- `prototype_2021_bundle`
- `prototype_legacy_bundle`

Compatibility note:

- `mindoro_march13_14_noaa_reinit_stress_test` is still supported, but only as a legacy alias. Use `mindoro_phase3b_primary_public_validation` as the primary Mindoro B1 command.

## Recommended First Commands

Use these first when you want status, packaging, or UI-refresh work rather than a new scientific rerun:

```powershell
.\start.ps1 -Entry phase1_audit
.\start.ps1 -Entry phase2_audit
.\start.ps1 -Entry final_validation_package
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
.\start.ps1 -Entry figure_package_publication
```

Use these only when you intentionally want reportable reruns:

```powershell
.\start.ps1 -Entry phase1_production_rerun
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation
.\start.ps1 -Entry mindoro_reportable_core
.\start.ps1 -Entry dwh_reportable_bundle
```

## How The UI Fits

The Streamlit UI is intentionally not a launcher entry. It is a read-only exploration surface over packaged outputs.

If you want the freshest read-only surfaces before opening it, refresh one or more of these first:

```powershell
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
.\start.ps1 -Entry figure_package_publication
```

Then launch the UI directly:

```bash
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

## Runtime Behavior

- Non-interactive launcher runs default to `FORCING_SOURCE_BUDGET_SECONDS=300` and `INPUT_CACHE_POLICY=reuse_if_valid`.
- Persistent local input store means validated reusable inputs under `data/drifters`, `data/forcing`, `data/arcgis`, `data/historical_validation_inputs`, and `data/local_input_store`.
- Output-local forcing and raw folders are staging or legacy scratch areas, not the canonical reuse source.
- `INPUT_CACHE_POLICY=force_refresh` bypasses validated local reuse, fetches fresh copies, and rewrites the persistent local store for that run.
- Inventories record reuse action, provider/source URL, persistent local storage path, and validation status.

## Guardrails

- `phase1_production_rerun` stages `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml` only. It does not auto-overwrite `config/phase1_baseline_selection.yaml`.
- `mindoro_phase3b_primary_public_validation` is the canonical Mindoro March 13 -> March 14 B1 entry. It preserves the frozen March 3 -> March 6 case YAML and uses the separate amendment file.
- `mindoro_reportable_core` is the full Mindoro validation-chain rerun. The separate `phase1_mindoro_focus_pre_spill_experiment` provenance lane stays outside it by design.
- `Phase 3B` and `Phase 3C` remain validation-only lanes.
- Outside `prototype_2016`, `phase4_oiltype_and_shoreline`, `phase5_sync`, the galleries, and the UI are support layers rather than main thesis phases.
- DWH Phase 3C stays a separate external transfer-validation story with readiness-gated HYCOM GOFS 3.1 + ERA5 + CMEMS wave/Stokes forcing; observed masks remain truth and PyGNOME remains comparator-only.
- `prototype_2021` is the preferred debug lane, but it is still not the final Phase 1 study.
- `prototype_2016` remains legacy support only as `Phase 1 / 2 / 3A / 4 / 5`.

## Where To Look Next

- [docs/COMMAND_MATRIX.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/COMMAND_MATRIX.md) for phase-level prompt-free mappings
- [docs/QUICKSTART.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/QUICKSTART.md) for the shortest current run path
- [docs/UI_GUIDE.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/UI_GUIDE.md) for the Streamlit surface
