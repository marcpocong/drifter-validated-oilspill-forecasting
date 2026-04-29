# Defense Demo Runbook

## Before Defense

- Confirm the repo is at the intended commit and `output/defense_readiness/defense_readiness_report.md` exists.
- If Docker is part of the live demo, start it before opening the panel menu.
- Keep the launcher on the defense-safe path unless a panel member explicitly asks for a rerun preview.
- Panel mode and read-only entries do not rerun science.
- Remember the claim boundary order: Phase 1 provenance -> B1 primary validation -> Track A comparator support -> DWH transfer validation -> Phase 4 support/context -> legacy/archive support.

## What To Open First

Start with:

```powershell
.\panel.ps1
```

Equivalent entrypoint:

```powershell
.\start.ps1 -Panel
```

## Exact Commands

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -Entry b1_drifter_context_panel
.\start.ps1 -Entry figure_package_publication
```

Useful supporting commands:

```powershell
.\start.ps1 -Help -NoPause
.\start.ps1 -List -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -ExportPlan -NoPause
.\start.ps1 -Explain phase1_mindoro_focus_provenance -NoPause
.\start.ps1 -Explain dwh_reportable_bundle -NoPause
.\start.ps1 -Explain b1_drifter_context_panel -NoPause
```

## How To Open The Dashboard

Use panel option `1`, or start it directly from the pipeline container:

```powershell
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

## What To Say

If the panel asks about drifters:

> The drifters are transport-provenance evidence for the recipe, not the direct B1 truth mask.

If the panel asks about B1:

> B1 is the only main Philippine public-observation validation claim and supports coastal-neighborhood usefulness, not exact 1 km overlap or universal operational accuracy.

If the panel asks about PyGNOME:

> PyGNOME is same-case comparator support only.

If the panel asks about DWH:

> DWH is external transfer validation and does not recalibrate Mindoro.

## Phase 4 Support Note

- Thesis-facing oil-type support values are final beached percentages, first-arrival time, impacted shoreline segments, and QC status.
- Raw `total_beached_kg` in `output/phase4/CASE_MINDORO_RETRO_2023/phase4_oil_budget_summary.csv` is not a primary manuscript claim unless it is separately re-audited.

## Fallback Plans

If Docker is not running:

- Open `output/figure_package_publication/`
- Open `docs/PAPER_OUTPUT_REGISTRY.md`
- Open `docs/DATA_SOURCES.md`
- Open `output/defense_readiness/defense_readiness_report.md`

If Streamlit does not open:

- Open `output/figure_package_publication/`
- Open `output/panel_review_check/panel_results_match_check.md`
- Open `output/panel_review_check/panel_results_match_check.json`

If internet is unavailable:

- Explain that panel mode is designed from stored outputs only.
- Stay on the read-only panel surfaces and stored registries.

## Quick Recovery Sequence

1. `.\panel.ps1`
2. Panel option `2` for manuscript-number verification if needed.
3. Panel option `7` or `.\start.ps1 -Entry b1_drifter_context_panel`
4. Panel option `8` for the data sources and provenance registry if needed.
5. Panel option `3` or `.\start.ps1 -Entry figure_package_publication`
6. Open `output/defense_readiness/defense_readiness_report.md` if a tooling question comes up.
