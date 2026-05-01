# Submission Alignment Contract Bootstrap Audit

## Git Gate Failure - 2026-05-02

Required pre-edit command:

```powershell
git pull --rebase origin main
```

Exact failure:

```text
error: cannot pull with rebase: You have unstaged changes.
error: Please commit or stash them.
```

Per the prompt instructions, source-file edits, commit, and push were stopped after this failure. No force, reset, stash, or archive/provenance/legacy deletion commands were run.

Scope: read-only stale-label search plus this audit report. The search did not run scientific workflows, download data, delete archive/provenance/legacy outputs, or alter scientific outputs. Use `docs/FINAL_SUBMISSION_ALIGNMENT_CONTRACT.md` as the authority for the recommended follow-up prompts below.

## Prompt Queue

- Prompt 1: completed here. Create the contract, link it from `docs/FINAL_PAPER_ALIGNMENT.md`, and write this bootstrap audit.
- Prompt 2: update docs/config registries and expected-values crosswalks to the final table and figure labels only.
- Prompt 3: update reviewer-facing UI, launcher, README, and panel-guide labels so final names lead and `B1` / `Track A` remain aliases only.
- Prompt 4: update generated publication figure-package metadata/captions and figure scripts from stored outputs only.
- Prompt 5: run read-only guardrail/label tests and refresh documentation-only audit notes.

## Findings

| ID | Stale label found | Paths | Recommended next prompt |
| --- | --- | --- | --- |
| A1 | Table 3.11 still maps to Mindoro deterministic / Phase 2 forecast product setup. Final Table 3.11 is "Final Mindoro March 13–14 primary validation case definition." | `docs/PAPER_TO_REPO_CROSSWALK.md:20`; `config/paper_to_output_registry.yaml:169` | Prompt 2 |
| A2 | Table 3.12 still maps to Mindoro ensemble/probability products. Final Table 3.12 is "Final Mindoro manuscript labels." | `docs/PAPER_TO_REPO_CROSSWALK.md:21`; `config/paper_to_output_registry.yaml:193` | Prompt 2 |
| A3 | Figure 4.1 and Figure 4.2 still carry generic study-box/geography-reference meanings. Final Figure 4.1 is the focused Phase 1 accepted February-April segment map; final Figure 4.2 is the focused Phase 1 recipe ranking chart. | `docs/PAPER_TO_REPO_CROSSWALK.md:17`; `docs/PAPER_TO_REPO_CROSSWALK.md:18`; `config/paper_to_output_registry.yaml:110`; `config/paper_to_output_registry.yaml:130` | Prompt 2 |
| A4 | Mindoro comparator values are still numbered as Table 4.9 in one registry and one figure script. Final Mindoro comparator values belong to Table 4.8. | `docs/PAPER_OUTPUT_REGISTRY.md:26`; `config/paper_output_registry.yaml:91`; `scripts/figures/make_figure_4_5_mindoro_trackA_spatial_comparator_board.ps1:378` | Prompt 2 for registries; Prompt 4 for the figure script |
| A5 | DWH table numbers are shifted in `docs/PAPER_OUTPUT_REGISTRY.md` and `config/paper_output_registry.yaml`: DWH FSS appears as Table 4.10 and DWH geometry appears as Table 4.11. Final DWH FSS is Table 4.9 and final DWH geometry is Table 4.10. | `docs/PAPER_OUTPUT_REGISTRY.md:29`; `docs/PAPER_OUTPUT_REGISTRY.md:30`; `config/paper_output_registry.yaml:134`; `config/paper_output_registry.yaml:149` | Prompt 2 |
| A6 | Secondary 2016 support labels are still old legacy/FSS/fate labels in the crosswalk and machine registry. Final secondary 2016 starts at Table 4.11, with 4.11A, 4.11B, and legacy OpenDrift–PyGNOME mean FSS at Table 4.12. | `docs/PAPER_TO_REPO_CROSSWALK.md:69`; `docs/PAPER_TO_REPO_CROSSWALK.md:70`; `docs/PAPER_TO_REPO_CROSSWALK.md:71`; `docs/PAPER_TO_REPO_CROSSWALK.md:72`; `config/paper_to_output_registry.yaml:747`; `config/paper_to_output_registry.yaml:765`; `config/paper_to_output_registry.yaml:783`; `config/paper_to_output_registry.yaml:801` | Prompt 2 |
| A7 | Figure 4.13 still points to legacy fate/shoreline support. Final Figure 4.13 is the Legacy 2016 OpenDrift-versus-PyGNOME overall mean FSS chart. | `docs/PAPER_TO_REPO_CROSSWALK.md:76`; `config/paper_to_output_registry.yaml:870` | Prompt 2 / Prompt 4 |
| A8 | Reviewer-facing `B1` labels remain widespread. Keep IDs/track IDs where needed, but labels should lead with "Primary Mindoro March 13–14 validation case." | `README.md:44`; `README.md:55`; `PANEL_QUICK_START.md:36`; `ui/pages/__init__.py:39`; `ui/pages/mindoro_validation.py:118`; `ui/data_access.py:857`; `start.ps1:3051` | Prompt 3 |
| A9 | Reviewer-facing `Track A` labels remain widespread. Keep as an internal alias only when paired with "Mindoro same-case OpenDrift–PyGNOME comparator." | `ui/pages/__init__.py:40`; `ui/pages/cross_model_comparison.py:108`; `ui/data_access.py:866`; `docs/PAPER_TO_REPO_CROSSWALK.md:36`; `output/figure_package_publication/publication_figure_talking_points.md:7`; `output/Phase 3B March13-14 Final Output/final_output_manifest.json:780` | Prompt 3 for UI/docs; Prompt 4 for generated package metadata |

## Guardrail Hits Reviewed

- Searches for PyGNOME-as-truth, DWH-as-recalibration, `mask_p90` as broad envelope, and exact 1 km Mindoro success mostly returned boundary-safe negations or validator guardrails, not positive overclaims.
- Examples of safe negations: `start.ps1:3052`, `start.ps1:3053`, `docs/PANEL_REVIEW_GUIDE.md:109`, `scripts/validate_final_paper_guardrails.py:130`.
- Preserve these guardrails during future label updates.
