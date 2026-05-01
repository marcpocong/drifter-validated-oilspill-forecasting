# CI Guardrail Hotfix Report

- Current git HEAD at audit start: `93f4df094296e23a3fb8e19a88ae1a3f304d86ea`
- Generated UTC: `2026-05-01T14:43:44Z`
- Scope: repo-consistency CI guardrails only.
- No expensive science was rerun.

## Root Cause Found

The repo-consistency workflow depended on a `pwsh` shell for the lightweight validator step. That makes the cheap guardrail job fail before a Python fallback can run when PowerShell is unavailable or not exposed as the step shell. The workflow now runs Python validators in the default shell, validates the launcher matrix with the Python validator, and runs `start.ps1` through `pwsh` only when `pwsh` is available.

The local full-suite command remains over-broad for this environment because it collects science tests requiring packages such as NumPy, Pandas, Xarray, GeoPandas, and PyYAML. The repo-consistency CI remains restricted to cheap guardrail tests and validators.

## Files Changed

- `.github/workflows/repo-consistency.yml`
- `.gitignore`
- `scripts/validate_final_paper_guardrails.py`
- `tests/test_final_paper_consistency_guardrails.py`
- `output/ci_guardrail_hotfix/ci_guardrail_hotfix_report.md`
- `output/ci_guardrail_hotfix/ci_guardrail_hotfix_report.json`

## Validation Commands

| Command | Status | Notes |
| --- | --- | --- |
| `python -m pytest tests -q` | fail | Collection stops on missing heavy science dependencies in the active local Python environment. |
| `python -m pytest -q tests/test_final_paper_consistency_guardrails.py tests/test_no_draft_version_labels.py tests/test_validate_launcher_matrix.py tests/test_launcher_matrix_metadata.py tests/test_defense_claim_boundaries.py` | pass | 34 cheap guardrail tests passed. |
| `python scripts/validate_final_paper_guardrails.py` | pass | Includes exact final-paper alignment fact checks. |
| `python scripts/validate_archive_registry.py` | pass | Archive registry parsed and launcher references validated. |
| `python scripts/validate_paper_to_output_registry.py` | pass | 51 entries checked, 0 errors, 0 warnings. |
| `python scripts/validate_data_sources_registry.py` | pass | 13 sources checked with explicit roles and secret requirements. |
| `pwsh ./start.ps1 -ValidateMatrix -NoPause` | local limitation | `pwsh` is unavailable locally. |
| `python -m src.utils.validate_launcher_matrix --no-write` | pass | Python fallback validates 27 launcher entries without writing reports. |
| Runtime-constructed forbidden-label scan | pass | No tracked-file hits; scan did not persist or print the literal label. |

## Claim-Boundary Preservation

The hotfix preserves the final-paper guardrails: PyGNOME remains comparator-only, DWH remains external transfer validation only, Mindoro B1 March 13-14 remains the only main Philippine public-observation validation claim, p50 remains the preferred probabilistic footprint, and p90 remains conservative support/comparison only.
