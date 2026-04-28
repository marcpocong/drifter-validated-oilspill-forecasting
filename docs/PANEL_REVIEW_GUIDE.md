# Panel Review Guide

This guide is for panel members who want to inspect the stored thesis outputs, verify that the software agrees with the manuscript, and stay inside the defense-safe evidence boundaries.

The key rule is simple: panel mode is for review, not for launching fresh science.

## 1. Start Here

Use either of these commands:

```powershell
.\panel.ps1
.\start.ps1 -Panel
```

That opens the panel-safe launcher path instead of the full research launcher.

## 2. What The Panel Menu Safely Does

Panel mode is meant to help a reviewer do the practical checks first:

1. open the read-only dashboard
2. verify manuscript numbers against stored scorecards
3. rebuild publication figures from stored outputs only
4. refresh the final validation package from stored outputs only
5. refresh the reproducibility / docs package from stored outputs only
6. open the paper-to-output registry

None of those actions are meant to rerun expensive science.

## 3. Draft 20 Evidence Order

The current defense-facing order is:

1. focused Mindoro Phase 1 transport provenance
2. Phase 2 standardized forecast products
3. Mindoro `B1` primary public-observation validation
4. Mindoro `Track A` same-case PyGNOME comparator support only
5. DWH external transfer validation
6. Mindoro oil-type and shoreline support/context
7. legacy 2016 archive/support
8. reproducibility / governance / read-only package layer

Panel mode is built to respect that order instead of flattening everything into one undifferentiated list.

## 4. Key Claim Boundaries

- `B1` is the only main-text primary Mindoro validation row.
- The March 13 -> March 14 `B1` pair keeps the shared-imagery caveat explicit.
- `Track A` is same-case comparator support only.
- `PyGNOME` is comparator-only, never observational truth.
- `DWH` is external transfer validation, not Mindoro recalibration.
- Mindoro oil-type and shoreline outputs are support/context only.
- `prototype_2016` is legacy/archive support only.
- The 5,000-element personal experiment is intentionally outside the default panel path.

## 5. What Should Match The Paper

The verification step checks stored values for:

- Mindoro `B1`: FSS, mean FSS, forecast and observed cells, distance diagnostics, IoU, and Dice
- Mindoro `Track A`: OpenDrift-vs-PyGNOME comparator summaries
- DWH: event-corridor FSS and IoU for `C1`, `C2 p50`, `C2 p90`, and `C3`
- Mindoro oil-type support values where machine-readable outputs exist

The verification outputs are written only to:

- `output/panel_review_check/panel_results_match_check.csv`
- `output/panel_review_check/panel_results_match_check.json`
- `output/panel_review_check/panel_results_match_check.md`
- `output/panel_review_check/panel_review_manifest.json`

## 6. Which Commands Are Panel-Safe

Main panel-safe entry points:

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
```

Read-only dashboard:

```powershell
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Read-only / packaging-only launcher entries:

```powershell
.\start.ps1 -Entry final_validation_package
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry figure_package_publication
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
```

## 7. Which Commands Are Researcher / Audit Reruns

These belong to the full launcher and are not the default defense path:

```powershell
.\start.ps1 -Entry phase1_mindoro_focus_provenance
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation
.\start.ps1 -Entry dwh_reportable_bundle
.\start.ps1 -Entry mindoro_reportable_core
.\start.ps1 -Entry phase1_regional_reference_rerun
.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle
```

Those commands can rerun major workflow phases and are better treated as intentional researcher or audit actions.

## 8. Why `B1` Is The Main Mindoro Row

`B1` is the promoted March 13 -> March 14 `R1_previous` row carried into the thesis-facing validation argument.

- It is the only main-text primary Mindoro validation row.
- The March 13 -> March 14 `R0` branch is preserved for archive/provenance.
- The other March-family rows remain useful context, but they are not replacements for `B1`.

If a panelist asks which Mindoro row should be compared with the paper first, the answer is `B1`.

## 9. Why The Shared-Imagery Caveat Matters

The March 13 and March 14 NOAA/NESDIS products both cite March 12 WorldView-3 imagery.

That means the row is still useful, but it should be described honestly as a reinitialization-based validation pair rather than a fully independent day-to-day pair. The panel-facing surfaces keep that caveat visible on purpose.

## 10. How To Open The Full Launcher

From panel mode, choose:

- `A. Open full research launcher`

Or run:

```powershell
.\start.ps1
```

Use the full launcher only when you intentionally want the researcher/audit menu.
