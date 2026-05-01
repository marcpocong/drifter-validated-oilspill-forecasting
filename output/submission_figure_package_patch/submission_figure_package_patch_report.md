# Submission Figure Package Patch Report

## Recovery Summary

Prompt 4 stopped before figure-package edits because `git pull --rebase origin main` failed with:

```text
error: cannot pull with rebase: You have unstaged changes.
error: Please commit or stash them.
```

Recovery inspected the dirty tree on `main` and classified the pre-existing files as safe alignment leftovers:

| Path | Classification | Resolution |
| --- | --- | --- |
| `config/panel_paper_expected_values.yaml` | SAFE: panel expected-values/final paper alignment | Committed in preflight cleanup |
| `config/paper_output_registry.yaml` | SAFE: tracked registry-alignment file, not an accidental untracked duplicate | Committed in preflight cleanup; not moved to holding |
| `scripts/figures/make_figure_4_5_mindoro_trackA_spatial_comparator_board.ps1` | SAFE: stored-output figure helper, no downloads or model reruns | Committed in preflight cleanup |
| `src/services/panel_review_check.py` | SAFE: no-rerun validation/reporting support | Committed in preflight cleanup |

Preflight commit: `dc62db3fb33e4733c01b0a59ed145ae3a6b4777a` (`chore: preserve pre-figure alignment artifacts`).

Preflight push: `origin HEAD:main` succeeded.

No scientific reruns, remote downloads, manuscript-PDF extraction, or archive/provenance deletion were performed.

## Final Figure 4 Mapping

| Final label | Path | Status |
| --- | --- | --- |
| Figure 4.1. Focused Phase 1 accepted February–April segment map | `output/figure_package_publication/figure_4_1_focused_phase1_accepted_feb_apr_segment_map.png` | Generated from stored outputs |
| Figure 4.2. Focused Phase 1 recipe ranking chart | `output/figure_package_publication/figure_4_2_focused_phase1_recipe_ranking_chart.png` | Generated from stored outputs |
| Figure 4.3. Mindoro product-family board with deterministic, probability, and threshold surfaces | `output/figure_package_publication/figure_4_3_mindoro_product_family_board.png` | Generated from stored outputs |
| Figure 4.4. Mindoro primary validation board | `output/figure_package_publication/figure_4_4_mindoro_primary_validation_board.png` | Existing stored figure alias |
| Figure 4.4A. NOAA-published March 13 WorldView-3 analysis map | `output/figure_package_publication/figure_4_4A_noaa_mar13_worldview3.png` | Existing stored figure |
| Figure 4.4B. NOAA-published March 14 WorldView-3 analysis map | `output/figure_package_publication/figure_4_4B_noaa_mar14_worldview3.png` | Existing stored figure |
| Figure 4.4C. ArcGIS overlay of March 13 and March 14 observed oil-spill extents | `output/figure_package_publication/figure_4_4C_arcgis_mar13_mar14_observed_overlay.png` | Existing stored figure |
| Figure 4.5. Mindoro same-case OpenDrift–PyGNOME spatial comparator board | `output/figure_package_publication/figure_4_5_mindoro_same_case_opendrift_pygnome_spatial_comparator_board.png` | Existing stored figure alias |
| Figure 4.6. Mindoro same-case OpenDrift–PyGNOME comparator mean FSS summary | `output/figure_package_publication/figure_4_6_mindoro_comparator_mean_fss_summary.png` | Generated from stored outputs |
| Figure 4.7. DWH observed, deterministic, mask_p50, and PyGNOME event-corridor board | `output/figure_package_publication/figure_4_7_dwh_eventcorridor_observed_deterministic_mask_p50_pygnome_board.png` | Existing stored figure alias |
| Figure 4.8. DWH 24 h, 48 h, and 72 h mask_p50, mask_p90, and PyGNOME overview board | `output/figure_package_publication/figure_4_8_dwh_24h_48h_72h_mask_p50_mask_p90_pygnome_overview_board.png` | Existing stored figure alias |
| Figure 4.9. DWH 48 h observed, deterministic, mask_p50, and PyGNOME board | `output/figure_package_publication/figure_4_9_dwh_48h_observed_deterministic_mask_p50_pygnome_board.png` | Existing stored figure alias |
| Figure 4.10. CASE_2016-09-01 secondary drifter-track benchmark map panel | `output/figure_package_publication/figure_4_10_case_2016_09_01_secondary_drifter_track_benchmark_map_panel.png` | Existing stored figure alias |
| Figure 4.11. CASE_2016-09-06 secondary drifter-track benchmark map panel | `output/figure_package_publication/figure_4_11_case_2016_09_06_secondary_drifter_track_benchmark_map_panel.png` | Existing stored figure alias |
| Figure 4.12. CASE_2016-09-17 secondary drifter-track benchmark map panel | `output/figure_package_publication/figure_4_12_case_2016_09_17_secondary_drifter_track_benchmark_map_panel.png` | Existing stored figure alias |
| Figure 4.13. Legacy 2016 OpenDrift-versus-PyGNOME overall mean FSS chart | `output/figure_package_publication/figure_4_13_legacy_2016_opendrift_pygnome_overall_mean_fss_chart.png` | Existing stored figure alias |

Optional missing final figures: none.

## Files Changed

- `output/figure_package_publication/publication_figure_registry.csv`
- `output/figure_package_publication/publication_figure_manifest.json`
- `output/figure_package_publication/publication_figure_captions.md`
- `output/figure_package_publication/publication_figure_inventory.md`
- `output/figure_package_publication/publication_figure_talking_points.md`
- Final Figure 4 alias/generated PNGs and generated JSON sidecars under `output/figure_package_publication`
- `docs/PAPER_TO_REPO_CROSSWALK.md`
- `docs/PAPER_OUTPUT_REGISTRY.md`
- `config/paper_to_output_registry.yaml`
- `scripts/figures/align_final_figure4_registry.py`
- `scripts/figures/make_final_figure4_stored_output_panels.ps1`

## Validators And Checks

Preflight checks before preserving dirty-tree leftovers:

| Check | Result |
| --- | --- |
| `python scripts/validate_paper_to_output_registry.py` | PASS: 57 entries, 0 errors, 0 warnings |
| `python scripts/panel_verify_paper_results.py` | SKIPPED: PyYAML unavailable, `ModuleNotFoundError: No module named 'yaml'` |
| Guardrail pytest bundle | PASS: 34 passed |

Figure-package checks after patching:

| Check | Result |
| --- | --- |
| `python scripts/validate_paper_to_output_registry.py` | PASS: 57 entries, 0 errors, 0 warnings |
| Custom stdlib final Figure 4 registry/manifest/path check | PASS: 16 final Figure 4 rows, 143 registry rows, 143 manifest figures |
| Custom stdlib selected final-label config check | PASS |
| Guardrail pytest bundle | PASS: 34 passed |
| `python -m pytest -q tests/test_figure_package_publication.py` | NOT RUN TO COMPLETION: collection failed because local dependency `numpy` is unavailable (`ModuleNotFoundError: No module named 'numpy'`) |

## Claim Boundaries Preserved

- Mindoro primary supports neighborhood agreement only, not exact overlap.
- Mindoro OpenDrift–PyGNOME products are comparator-only and do not use PyGNOME as truth.
- DWH figures are external transfer validation only.
- Secondary 2016 figures are support only, not public-spill validation.
- `mask_p90` is conservative high-confidence support only.

## Git Diff Summary Before Final Figure Commit

```text
27 files changed, 2192 insertions(+), 291 deletions(-)
M config/paper_to_output_registry.yaml
M docs/PAPER_OUTPUT_REGISTRY.md
M docs/PAPER_TO_REPO_CROSSWALK.md
A output/figure_package_publication/figure_4_1_focused_phase1_accepted_feb_apr_segment_map.png
A output/figure_package_publication/figure_4_1_focused_phase1_accepted_feb_apr_segment_map.json
A output/figure_package_publication/figure_4_2_focused_phase1_recipe_ranking_chart.png
A output/figure_package_publication/figure_4_2_focused_phase1_recipe_ranking_chart.json
A output/figure_package_publication/figure_4_3_mindoro_product_family_board.png
A output/figure_package_publication/figure_4_3_mindoro_product_family_board.json
A output/figure_package_publication/figure_4_4_mindoro_primary_validation_board.png
A output/figure_package_publication/figure_4_5_mindoro_same_case_opendrift_pygnome_spatial_comparator_board.png
A output/figure_package_publication/figure_4_6_mindoro_comparator_mean_fss_summary.png
A output/figure_package_publication/figure_4_6_mindoro_comparator_mean_fss_summary.json
A output/figure_package_publication/figure_4_7_dwh_eventcorridor_observed_deterministic_mask_p50_pygnome_board.png
A output/figure_package_publication/figure_4_8_dwh_24h_48h_72h_mask_p50_mask_p90_pygnome_overview_board.png
A output/figure_package_publication/figure_4_9_dwh_48h_observed_deterministic_mask_p50_pygnome_board.png
A output/figure_package_publication/figure_4_10_case_2016_09_01_secondary_drifter_track_benchmark_map_panel.png
A output/figure_package_publication/figure_4_11_case_2016_09_06_secondary_drifter_track_benchmark_map_panel.png
A output/figure_package_publication/figure_4_12_case_2016_09_17_secondary_drifter_track_benchmark_map_panel.png
A output/figure_package_publication/figure_4_13_legacy_2016_opendrift_pygnome_overall_mean_fss_chart.png
M output/figure_package_publication/publication_figure_captions.md
M output/figure_package_publication/publication_figure_inventory.md
M output/figure_package_publication/publication_figure_manifest.json
M output/figure_package_publication/publication_figure_registry.csv
M output/figure_package_publication/publication_figure_talking_points.md
A scripts/figures/align_final_figure4_registry.py
A scripts/figures/make_final_figure4_stored_output_panels.ps1
```

## Remaining Warnings

- The local publication figure-package pytest module could not be collected because `numpy` is not installed locally; no dependency was installed or downloaded.
- No scientific reruns, remote downloads, manuscript-PDF extraction, or deletion of archive/provenance/legacy outputs were performed.

Final push result is reported in Codex final response; this report is not edited after final push to avoid leaving a dirty working tree.
