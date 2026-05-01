# Final Submission Alignment Contract

Repo-local source of truth for final paper-to-repo alignment. Use this contract when editing docs, registries, UI labels, expected-values configs, and figure-package crosswalks. Do not ask for manuscript PDFs, run scientific reruns, download remote data, delete archive/provenance/legacy outputs, or expand claim boundaries.

## Final Manuscript Title

Drifter-Validated 24–72 h Oil-Spill Forecasting for Philippine Coasts: Probability Footprints and Oil-Type Fate

## Evidence Order

1. Focused Mindoro Phase 1 transport provenance.
2. Standardized deterministic and 50-member forecast products.
3. Primary Mindoro March 13–14 public-observation validation case.
4. Mindoro same-case OpenDrift–PyGNOME comparator support.
5. Deepwater Horizon external transfer validation.
6. Mindoro oil-type and shoreline support/context.
7. Secondary 2016 drifter-track and legacy FSS support.
8. Reproducibility, governance, archive/provenance, and read-only package layer.

## Claim Boundaries

- The Primary Mindoro March 13–14 validation case is the only main Philippine public-observation validation claim.
- It supports bounded coastal-neighborhood usefulness, not exact 1 km spill-footprint reproduction, not universal operational accuracy, and not broad Philippine operational validation.
- March 13 and March 14 are independent NOAA-published day-specific public-observation products. March 13 is the seed/initialization observation; March 14 is the target/validation observation.
- Exact-grid success must not be claimed. IoU = `0.0` and Dice = `0.0` for the primary Mindoro row.
- PyGNOME is comparator-only and never observational truth.
- DWH is external transfer validation only and does not recalibrate or replace Mindoro.
- Mindoro oil-type and shoreline outputs are support/context only.
- Secondary 2016 outputs support observed-drifter-track behavior and legacy OpenDrift–PyGNOME FSS context only; they are not public-spill validation and do not replace Mindoro or DWH.
- Archived, experimental, legacy, and comparator-only outputs remain inspectable but must not be promoted into main paper claims.
- `prob_presence` is cellwise ensemble probability of presence.
- `mask_p50` is `P >= 0.50`, the preferred probabilistic footprint / majority-member surface.
- `mask_p90` is `P >= 0.90`, a conservative high-confidence core / support product only. Never describe `mask_p90` as a broad envelope.

## Key Final Values

### Focused Mindoro Phase 1

- `workflow_mode`: `phase1_mindoro_focus_pre_spill_2016_2023`
- Historical window: `2016-01-01` to `2023-03-02`
- Focused validation box: `[118.751, 124.305, 10.620, 16.026]`
- Full strict accepted segments: `65`
- February-April ranked subset: `19`
- Selected recipe: `cmems_gfs`

| Recipe | Mean NCS | Median NCS |
| --- | ---: | ---: |
| `cmems_gfs` | `4.5886` | `4.6305` |
| `cmems_era5` | `4.6237` | `4.5916` |
| `hycom_gfs` | `4.7027` | `4.9263` |
| `hycom_era5` | `4.7561` | `5.0106` |

### Mindoro Primary Validation

- Manuscript label: Primary Mindoro March 13–14 validation case.
- Internal alias allowed only as secondary context: `B1`.
- Seed observation: March 13 NOAA/NESDIS WorldView-3 public observation extent.
- Target observation: March 14 NOAA/NESDIS WorldView-3 public observation extent.
- Scoring product: promoted OpenDrift p50 / `mask_p50` branch.
- Adopted recipe: `cmems_gfs`.

| Metric | Value |
| --- | ---: |
| FSS 1 km | `0.0000` |
| FSS 3 km | `0.0441` |
| FSS 5 km | `0.1371` |
| FSS 10 km | `0.2490` |
| Mean FSS | `0.1075` |
| Forecast cells | `5` |
| Observed cells | `22` |
| Nearest distance | `1414.21 m` |
| Centroid distance | `7358.16 m` |
| IoU | `0.0` |
| Dice | `0.0` |

### Mindoro Same-Case OpenDrift–PyGNOME Comparator

- Manuscript label: Mindoro same-case OpenDrift–PyGNOME comparator.
- Internal alias allowed only as secondary context: `Track A`.
- Comparator-only; PyGNOME is not truth and not a second validation row.

| Branch | Forecast cells | Nearest distance | FSS 1/3/5/10 km | Mean FSS |
| --- | ---: | ---: | --- | ---: |
| OpenDrift promoted p50 branch | `5` | `1414.21 m` | `0.0000 / 0.0441 / 0.1371 / 0.2490` | `0.1075` |
| PyGNOME deterministic comparator | `6` | `6082.76 m` | `0.0000 / 0.0000 / 0.0000 / 0.0244` | `0.0061` |

### DWH External Transfer Validation

- Case ID: `CASE_DWH_RETRO_2010_72H`
- Forcing stack: HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.
- Truth semantics: daily date-composite public observation masks; exact sub-daily acquisition times are not claimed.

| Date / corridor | Deterministic | p50 | p90 | PyGNOME |
| --- | ---: | ---: | ---: | ---: |
| `2010-05-21` | `0.4538` | `0.4529` | `0.4930` | `0.2434` |
| `2010-05-22` | `0.4808` | `0.5408` | `0.4870` | `0.2539` |
| `2010-05-23` | `0.4146` | `0.4727` | `0.4442` | `0.2532` |
| Event corridor `2010-05-21` to `2010-05-23` | `0.5568` | `0.5389` | `0.4966` | `0.3612` |

| Product | Forecast cells | Observed cells | Area ratio | Centroid distance | IoU | Dice | Mean FSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Deterministic | `53980` | `44305` | `1.2184` | `72547.72 m` | `0.3362` | `0.5033` | `0.5568` |
| p50 | `51922` | `44305` | `1.1719` | `94270.47 m` | `0.3331` | `0.4997` | `0.5389` |
| p90 | `27776` | `44305` | `0.6269` | `68939.89 m` | `0.2938` | `0.4542` | `0.4966` |
| PyGNOME | `20639` | `44305` | `0.4658` | `58867.12 m` | `0.1903` | `0.3197` | `0.3612` |

### Mindoro Oil-Type Support/Context

| Scenario | Beached | First arrival | Impacted segments | QC |
| --- | ---: | ---: | ---: | --- |
| Light oil | `0.02%` | `4 h` | `11` | pass |
| Fixed-base medium-heavy proxy | `0.61%` | `4 h` | `10` | flagged |
| Heavier oil | `0.63%` | `4 h` | `11` | pass |

Support/context only; not primary validation and not a matched Mindoro fate-and-shoreline PyGNOME comparator.

### Secondary 2016 Direct Drifter-Track Support

- Cases: `CASE_2016-09-01`, `CASE_2016-09-06`, `CASE_2016-09-17`
- Observed reference: NOAA observed drifter track.
- PyGNOME role: deterministic comparator only.

| Case | OpenDrift mean sep | PyGNOME mean sep | Nearest OpenDrift ensemble mean sep | OD - PyGNOME | NCS OD/PyG/Ens |
| --- | ---: | ---: | ---: | ---: | --- |
| `2016-09-01` | `10.21 km` | `13.12 km` | `9.72 km` | `-2.92 km` | `4.32 / 5.55 / 4.11` |
| `2016-09-06` | `7.49 km` | `9.01 km` | `7.43 km` | `-1.52 km` | `2.63 / 3.16 / 2.61` |
| `2016-09-17` | `18.51 km` | `18.48 km` | `17.72 km` | `0.03 km` | `3.22 / 3.22 / 3.08` |

Across cases: OpenDrift `12.07 km` mean time-averaged separation, PyGNOME `13.54 km`, nearest OpenDrift ensemble member `11.62 km`; OpenDrift was `1.47 km` nearer on case-mean time-averaged separation. This is secondary support only and not a universal superiority claim.

### Legacy 2016 OpenDrift–PyGNOME Mean FSS Support

| Case | Deterministic | p50 | p90 |
| --- | ---: | ---: | ---: |
| `2016-09-01` | `0.504` | `0.407` | `0.454` |
| `2016-09-06` | `0.483` | `0.516` | `0.562` |
| `2016-09-17` | `0.714` | `0.342` | `0.377` |

These are model-to-model legacy comparator FSS values, not public-spill validation.

## Final Table-Label Contract

### Chapter 3 Tables

- Table 3.1. Core implementation components used in the final workflow
- Table 3.2. Core data classes and their roles in the workflow
- Table 3.3. Persistent local input policy adopted in the workflow
- Table 3.4. Workflow lanes, evidence roles, and use in the thesis
- Table 3.5. Mindoro domains and secondary 2016 support geography used in the workflow
- Table 3.6. Frozen Mindoro scoring-grid parameters from the stored grid artifact
- Table 3.7. Active Phase 1 provenance lane and adopted selection rules
- Table 3.8. Four-recipe family tested in the focused Mindoro provenance lane
- Table 3.9. Final deterministic and ensemble settings used for the Mindoro transport-core products
- Table 3.10. Standardized product families produced in Phase 2
- Table 3.11. Final Mindoro March 13–14 primary validation case definition
- Table 3.12. Final Mindoro manuscript labels
- Table 3.13. Frozen DWH external-case settings in the final implementation
- Table 3.14. Deepwater Horizon external-case output groups and interpretation boundaries
- Table 3.15. Mindoro oil-type support scenarios
- Table 3.16. Artifact classes and their allowed presentation surfaces
- Table 3.17. Reproducibility-control record groups used in the implemented workflow

### Chapter 4 Tables

- Table 4.1. Result groups, evidence roles, and interpretation boundaries
- Table 4.2. Focused Mindoro Phase 1 accepted-pool summary
- Table 4.3. Focused Mindoro Phase 1 recipe ranking
- Table 4.4. Standardized Forecast Products used in later scoring
- Table 4.5. Mindoro primary validation FSS by neighborhood window
- Table 4.6. Mindoro primary validation branch survival and displacement diagnostics
- Table 4.7. Mindoro primary validation overlap and neighborhood FSS diagnostics
- Table 4.8. Mindoro same-case OpenDrift–PyGNOME comparator detail
- Table 4.9. Deepwater Horizon daily and event-corridor mean FSS summary
- Table 4.10. Deepwater Horizon event-corridor geometry diagnostics
- Table 4.11. Secondary 2016 direct drifter-track benchmark summary
- Table 4.11A. Secondary 2016 scorecard summary values
- Table 4.11B. Secondary 2016 endpoint and ensemble-footprint diagnostics from the scorecards
- Table 4.12. Legacy 2016 OpenDrift-versus-PyGNOME mean FSS by case, support surface, and neighborhood window
- Table 4.13. Synthesis of principal findings and thesis use

### Appendix Tables

- Table A.1. Active Phase 1 provenance lane and adopted selection rules
- Table A.2. Four-recipe family tested in the focused Mindoro provenance lane
- Table B.1. Frozen Mindoro scoring-grid parameters
- Table B.2. Final deterministic and ensemble settings used for the Mindoro transport-core products
- Table B.3. Standardized product families produced in Phase 2
- Table B.4. Archived frozen Mindoro base-case definition retained for provenance only
- Table B.5. Final Mindoro track semantics used in the thesis package and interface
- Table C.1. Mindoro primary validation FSS by neighborhood window
- Table C.2. Mindoro primary validation branch survival and displacement diagnostics
- Table C.3. Mindoro primary validation overlap and neighborhood FSS diagnostics
- Table C.4. Mindoro same-case OpenDrift–PyGNOME comparator detail
- Table D.1. Secondary 2016 drifter-track benchmark summary
- Table D.2. Secondary 2016 endpoint and ensemble-containment diagnostics
- Table D.3. Legacy 2016 ensemble-comparator mean FSS by case, support surface, and neighborhood window
- Table E.1. Frozen DWH external-case settings in the final implementation
- Table E.2. DWH manuscript labels and interpretation rules
- Table E.3. Deepwater Horizon daily and event-corridor mean FSS summary
- Table E.4. Deepwater Horizon event-corridor area and overlap diagnostics
- Table F.1. Mindoro oil-type support scenarios
- Table F.2. Mindoro oil-budget summary across support scenarios
- Table G.1. Artifact classes and their allowed presentation surfaces
- Table G.2. Repository configuration and manifest records retained for reproducibility audit
- Table I.1. Gantt chart of the study workflow and timeline from February 2 to April 30

## Final Figure-Label Contract

- Figure 3.1. Revised defended workflow of the final study
- Figure 3.2. Study-box hierarchy for the Mindoro workflow and secondary 2016 support cases
- Figure 3.3. Phase 1 drifter-based transport-validation and recipe-selection procedure
- Figure 3.4. Official Mindoro 50-member ensemble design and probabilistic product-generation workflow
- Figure 3.5. Mindoro public-observation validation and comparator routing
- Figure 3.6. Common-grid forecast-observation scoring pipeline
- Figure 3.7. Artifact-governance classes and allowed presentation surfaces
- Figure 4.1. Focused Phase 1 accepted February–April segment map
- Figure 4.2. Focused Phase 1 recipe ranking chart
- Figure 4.3. Mindoro product-family board with deterministic, probability, and threshold surfaces
- Figure 4.4. Mindoro primary validation board
- Figure 4.4A. NOAA-published March 13 WorldView-3 analysis map
- Figure 4.4B. NOAA-published March 14 WorldView-3 analysis map
- Figure 4.4C. ArcGIS overlay of March 13 and March 14 observed oil-spill extents
- Figure 4.5. Mindoro same-case OpenDrift–PyGNOME spatial comparator board
- Figure 4.6. Mindoro same-case OpenDrift–PyGNOME comparator mean FSS summary
- Figure 4.7. DWH observed, deterministic, mask_p50, and PyGNOME event-corridor board
- Figure 4.8. DWH 24 h, 48 h, and 72 h mask_p50, mask_p90, and PyGNOME overview board
- Figure 4.9. DWH 48 h observed, deterministic, mask_p50, and PyGNOME board
- Figure 4.10. CASE_2016-09-01 secondary drifter-track benchmark map panel
- Figure 4.11. CASE_2016-09-06 secondary drifter-track benchmark map panel
- Figure 4.12. CASE_2016-09-17 secondary drifter-track benchmark map panel
- Figure 4.13. Legacy 2016 OpenDrift-versus-PyGNOME overall mean FSS chart

## Reviewer-Facing Output Roots

- `output/phase1_mindoro_focus_pre_spill_2016_2023`
- `output/Phase 3B March13-14 Final Output`
- `output/Phase 3C DWH Final Output`
- `output/phase4/CASE_MINDORO_RETRO_2023`
- `output/2016_drifter_benchmark`
- `output/2016 Legacy Runs FINAL Figures`
- `output/final_validation_package`
- `output/final_reproducibility_package`
- `output/figure_package_publication`
- `ui`

Curated final package paths should be reviewer-facing first. Raw `CASE_MINDORO_RETRO_2023` and `CASE_DWH_RETRO_2010_72H` generation paths may remain optional/staging/provenance paths, with clear `optional_missing` or archive/provenance notes where not tracked.

## Stale-Label Handling Rules

- Table 3.11 as "Mindoro deterministic product setup" is stale; use "Final Mindoro March 13–14 primary validation case definition."
- Table 3.12 as "Mindoro ensemble/probability products" is stale; use "Final Mindoro manuscript labels."
- Table 4.8 generic "Track A" wording is acceptable only if paired with "Mindoro same-case OpenDrift–PyGNOME comparator detail."
- Mindoro comparator values must be Table 4.8, not Table 4.9.
- DWH daily/event-corridor FSS must be Table 4.9.
- DWH geometry diagnostics must be Table 4.10.
- Secondary 2016 drifter-track support starts at Table 4.11, with 4.11A and 4.11B.
- Legacy 2016 OpenDrift–PyGNOME mean FSS is Table 4.12.
- Figure 4.1 is the focused Phase 1 accepted February-April segment map, not generic study-box context.
- Figure 4.2 is the focused Phase 1 recipe ranking chart, not generic geography reference.
- Figure 4.6 is Mindoro same-case OpenDrift–PyGNOME comparator mean FSS summary.
- Figure 4.13 is Legacy 2016 OpenDrift-versus-PyGNOME overall mean FSS chart.
- `B1` may be retained as an internal alias, but reviewer-facing labels should lead with "Primary Mindoro March 13–14 validation case."
- `Track A` may be retained as an internal alias, but reviewer-facing labels should lead with "Mindoro same-case OpenDrift–PyGNOME comparator."
- Never call PyGNOME truth.
- Never call DWH a Mindoro recalibration.
- Never call `mask_p90` a broad envelope.
- Never claim exact 1 km success for Mindoro.
