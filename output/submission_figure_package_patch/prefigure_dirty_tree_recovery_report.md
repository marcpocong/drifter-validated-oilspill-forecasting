# Prefigure Dirty Tree Recovery Report

## Recovery Context

Prompt 4 stopped before patching because the required pre-edit command `git pull --rebase origin main` failed with unstaged local changes:

```text
error: cannot pull with rebase: You have unstaged changes.
error: Please commit or stash them.
```

## Dirty Files Found

Initial branch: `main`

Initial dirty state:

```text
 M config/panel_paper_expected_values.yaml
 M config/paper_output_registry.yaml
 M scripts/figures/make_figure_4_5_mindoro_trackA_spatial_comparator_board.ps1
 M src/services/panel_review_check.py
```

No staged changes were present.

## Classification

| Path | Classification | Reason |
| --- | --- | --- |
| `config/panel_paper_expected_values.yaml` | SAFE | Final-submission expected-values and claim-boundary alignment; uses stored outputs only and adds optional provenance fallbacks. |
| `config/paper_output_registry.yaml` | SAFE | Tracked file; changes repair final table labels and reviewer-facing stored-output paths. It was not moved to holding. |
| `scripts/figures/make_figure_4_5_mindoro_trackA_spatial_comparator_board.ps1` | SAFE | Figure-helper label repair only: Table 4.8 and final Figure 4.5 wording. No model rerun or download behavior changed. |
| `src/services/panel_review_check.py` | SAFE | No-rerun panel validator/reporting support for optional fallbacks, text values, and aggregate lookups. |

`config/paper_output_registry.yaml` is tracked and exists. `config/paper_to_output_registry.yaml` is also tracked and exists.

## Safe Checks

| Check | Result |
| --- | --- |
| `python scripts/validate_paper_to_output_registry.py` | PASS: 57 entries, 0 errors, 0 warnings. |
| `python -c "import yaml; print('PyYAML available')"` | Not available: `ModuleNotFoundError: No module named 'yaml'`. `python scripts/panel_verify_paper_results.py` was not run. |
| `python -m pytest -q tests/test_final_paper_consistency_guardrails.py tests/test_no_draft_version_labels.py tests/test_validate_launcher_matrix.py tests/test_launcher_matrix_metadata.py tests/test_defense_claim_boundaries.py` | PASS: 34 passed. |

## Diff Summary Before Preflight Commit

```text
 config/panel_paper_expected_values.yaml            | 1360 ++++++++++++++++++--
 config/paper_output_registry.yaml                  |   34 +-
 ...4_5_mindoro_trackA_spatial_comparator_board.ps1 |    4 +-
 src/services/panel_review_check.py                 |  148 ++-
 4 files changed, 1375 insertions(+), 171 deletions(-)
```

## Safety Statement

No scientific reruns, model simulations, remote downloads, manuscript-PDF extraction, or archive/provenance/legacy output deletions were performed during dirty-tree recovery.
