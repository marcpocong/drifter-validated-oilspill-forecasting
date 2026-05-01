# Panel Review Guide

This guide is for panel members who want to inspect stored outputs, verify that the software matches the final manuscript alignment, and stay inside the final defended evidence boundaries.

The practical rule is simple: panel mode and read-only entries do not rerun science.
No uploaded manuscript file is required; use [FINAL_PAPER_ALIGNMENT.md](FINAL_PAPER_ALIGNMENT.md) as the concise paper-to-repo bridge.

## 1. Start Here

Use either command:

```powershell
.\panel.ps1
.\start.ps1 -Panel
```

That opens the panel-safe launcher path instead of the full research launcher.

## 2. What The Panel Menu Safely Does

1. Open the read-only dashboard
2. Verify stored manuscript numbers against stored scorecards
3. Rebuild publication figures from stored outputs only
4. Refresh the final validation package from stored outputs only
5. Refresh the reproducibility / docs package from stored outputs only
6. Open the paper-to-output registry
7. Inspect the focused Phase 1 drifter provenance behind `B1`
8. Open the data sources and provenance registry

None of those steps are meant to rerun expensive science or change thesis claims.

## Data Sources And Provenance

Use [DATA_SOURCES.md](DATA_SOURCES.md) or the dashboard's `Data Sources & Provenance` reference page to answer what external data sources were used, where they came from, what each source was used for, and where the related manifests or stored outputs live. This is read-only governance documentation backed by [config/data_sources.yaml](../config/data_sources.yaml); it does not promote new claims or rerun science.

Archive governance is documented in [ARCHIVE_GOVERNANCE.md](ARCHIVE_GOVERNANCE.md). Archived and legacy outputs stay inspectable without becoming main-paper evidence.

## 3. Final Manuscript Evidence Order

1. Focused Mindoro Phase 1 transport provenance
2. Phase 2 standardized deterministic and 50-member forecast products
3. Primary Mindoro March 13-14 validation case (`B1` internal alias)
4. Mindoro same-case OpenDrift-PyGNOME comparator (`Track A` internal alias)
5. DWH external transfer validation
6. Mindoro oil-type and shoreline support/context
7. Secondary 2016 drifter-track and legacy FSS support
8. Reproducibility/governance/read-only package layer

Panel mode is built to respect that order instead of flattening the repo into one undifferentiated list.

## 4. Key Claim Boundaries

- The Primary Mindoro March 13-14 validation case is the only main Philippine public-observation validation claim; `B1` is an internal alias.
- The primary case supports coastal-neighborhood usefulness, not exact 1 km overlap or universal operational accuracy.
- March 13-14 keeps the observation-independence note explicit.
- The Mindoro same-case OpenDrift-PyGNOME comparator is comparator-only support; `Track A` is an internal alias.
- PyGNOME is comparator-only and never the observational scoring reference.
- DWH is external transfer validation only, not Mindoro recalibration.
- Mindoro oil-type and shoreline outputs are support/context only.
- Secondary 2016 outputs are drifter-track and legacy FSS support only.
- The dashboard, publication package, validation packages, audits, and docs entries are read-only or packaging-only surfaces that do not recompute science.

## 5. Result Values Checklist

### Focused Phase 1 Provenance

- Workflow mode: `phase1_mindoro_focus_pre_spill_2016_2023`
- Historical window: `2016-01-01` to `2023-03-02`
- Focused validation box: `[118.751, 124.305, 10.620, 16.026]`
- Full strict accepted segments: `65`
- Ranked February-April subset: `19`
- Recipe winner: `cmems_gfs`
- `cmems_gfs` mean / median NCS: `4.5886 / 4.6305`
- `cmems_era5` mean / median NCS: `4.6237 / 4.5916`
- `hycom_gfs` mean / median NCS: `4.7027 / 4.9263`
- `hycom_era5` mean / median NCS: `4.7561 / 5.0106`

### Primary Mindoro March 13-14 Validation Case (`B1` Alias)

- FSS `1 / 3 / 5 / 10 km`: `0.0000 / 0.0441 / 0.1371 / 0.2490`
- Mean FSS: `0.1075`
- Public-observation seed: March 13 NOAA/NESDIS observation extent
- Public-observation target: March 14 NOAA/NESDIS observation extent
- March 13 and March 14 are independent day-specific public-observation products
- Forecast cells `5`; observed cells `22`; nearest distance `1414.21 m`; centroid distance `7358.16 m`
- `IoU = 0.0`; `Dice = 0.0`
- Interpretation: supports coastal-neighborhood usefulness, not exact 1 km overlap

### Mindoro Same-Case OpenDrift-PyGNOME Comparator (`Track A` Alias)

- OpenDrift `R1_previous`: forecast cells `5`; nearest distance `1414.21 m`; mean FSS `0.1075`
- PyGNOME deterministic comparator-only support: forecast cells `6`; nearest distance `6082.76 m`; FSS `0.0000 / 0.0000 / 0.0000 / 0.0244`; mean FSS `0.0061`

### DWH

- Case ID: `CASE_DWH_RETRO_2010_72H`
- Forcing stack: `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`
- `C1` event-corridor mean FSS: `0.5568`
- `C2 p50` event-corridor mean FSS: `0.5389`
- `C2 p90` event-corridor mean FSS: `0.4966`
- `C3` PyGNOME comparator event-corridor mean FSS: `0.3612`
- Interpretation: DWH remains external transfer validation only and does not recalibrate Mindoro

### Probability Semantics

- `prob_presence`: cellwise ensemble probability of presence
- `mask_p50`: `P >= 0.50`, preferred probabilistic footprint
- `mask_p90`: `P >= 0.90`, conservative support/comparison product only
- Never label `mask_p90` as a broad envelope

### Mindoro Oil-Type / Shoreline Support

- Light oil: final beached fraction `0.02%`; first arrival `4 h`; impacted segments `11`; QC pass
- Fixed-base medium-heavy proxy: final beached fraction `0.61%`; first arrival `4 h`; impacted segments `10`; QC flagged
- Heavier oil: final beached fraction `0.63%`; first arrival `4 h`; impacted segments `11`; QC pass

## 6. What The Verification Step Writes

The panel verification step reads stored outputs and writes only to:

- `output/panel_review_check/panel_results_match_check.csv`
- `output/panel_review_check/panel_results_match_check.json`
- `output/panel_review_check/panel_results_match_check.md`
- `output/panel_review_check/panel_review_manifest.json`

## 7. Which Commands Are Panel-Safe

Main panel-safe entry points:

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -List -NoPause
.\start.ps1 -ListRole primary_evidence -NoPause
.\start.ps1 -ListRole archive_provenance -NoPause
.\start.ps1 -Help -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -ExportPlan -NoPause
```

`-List` shows role-grouped launcher entries instead of flattening archive, legacy, support, and main evidence together. `-Explain` is safe: it prints the label, manuscript section, thesis role, claim boundary, run kind, rerun cost, `safe_default`, role flags, expected outputs, and alias requested/canonical IDs before any execution path.

macOS with Homebrew:

```bash
brew install powershell
pwsh ./panel.ps1
pwsh ./start.ps1 -Panel
pwsh ./start.ps1 -List -NoPause
pwsh ./start.ps1 -Help -NoPause
```

Read-only / packaging-only launcher entries:

```powershell
.\start.ps1 -Entry b1_drifter_context_panel
.\start.ps1 -Entry final_validation_package
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry figure_package_publication
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
```

Dashboard:

```bash
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

The default dashboard page order is `Overview / Final Manuscript Alignment`, `Data Sources & Provenance`, `Focused Mindoro Phase 1 Provenance`, `Primary Mindoro March 13-14 Validation Case (B1)`, `Mindoro Same-Case OpenDrift-PyGNOME Comparator (Track A)`, `DWH External Transfer Validation`, `Mindoro Oil-Type and Shoreline Support/Context`, `Secondary 2016 Drifter-Track And Legacy FSS Support`, `Archive/Provenance and Legacy Support`, and `Reproducibility / Governance / Audit`.
Dashboard pages organize stored outputs only and do not create new scientific results.

Data sources and provenance:

```powershell
docs\DATA_SOURCES.md
```

## 8. Inspecting Drifter Provenance Behind The Primary Mindoro Case (`B1` Alias)

Panel members can use panel option `7` or:

```powershell
.\start.ps1 -Entry b1_drifter_context_panel
```

That view stays stored-output-only.

- It explains the focused historical Phase 1 provenance behind the selected recipe.
- It does not turn drifters into the March 13-14 truth mask.
- It makes the claim boundary explicit: B1 drifter provenance is not the direct March 13-14 public-observation validation truth.
- It explicitly says when no direct March 13-14 2023 accepted drifter segment is stored for `B1`.

## 9. Inspecting Data Sources And Provenance

Panel members can use panel option `8` to view `docs/DATA_SOURCES.md`.

That registry is read-only. It never downloads inputs, reruns workflows, rewrites scientific outputs, or changes the claim boundary.

## 10. Why The Primary Mindoro March 13-14 Case Is The Main Mindoro Row

The Primary Mindoro March 13-14 validation case is the promoted `R1_previous` row carried into the main validation argument; `B1` remains the internal alias.

- It is the only main-text primary Mindoro validation row.
- It supports coastal-neighborhood usefulness, not exact 1 km overlap.
- The March 13-14 `R0` branch is preserved for archive/provenance.
- The other March-family rows remain useful context, but they are not replacements for `B1`.

## 11. Why The Observation Independence Note Matters

The March 13 and March 14 public products are independent NOAA-published day-specific public observations.

That means the row can be described as an independent day-to-day observation pair, while still being clear that the model evaluation is a reinitialization-based public-observation validation from the March 13 seed to the March 14 target.

## 12. Which Commands Are Researcher / Audit Reruns

These belong to the full launcher and are not the default defense path:

```powershell
.\start.ps1 -Entry phase1_mindoro_focus_provenance
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation
.\start.ps1 -Entry dwh_reportable_bundle
.\start.ps1 -Entry mindoro_reportable_core
.\start.ps1 -Entry phase1_regional_reference_rerun
.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle
```

Use them only when you intentionally want researcher or audit reruns. Expensive rerun entries ask for confirmation and warn that science or archive/comparator outputs may be recomputed; read-only and packaging entries state that no scientific rerun should occur.
