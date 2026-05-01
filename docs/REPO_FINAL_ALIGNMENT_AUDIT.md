# Repo Final Alignment Audit

- Audit date/time: `2026-05-01T13:14:47` (`Asia/Manila`)
- Current HEAD: `161d3c395eac073ea0c2b29c643c7f0ee0f010f9`
- Tracked files scanned: `1095`
- Text files scanned: `600`
- Machine-readable audit: `output/repo_cleanup_audit/final_alignment_audit.json`

This audit itself does not promote, demote, delete, reroute, or recompute scientific outputs. It records repository wording, launcher/config alignment, claim-boundary risks, and archive-routing candidates for later prompts.

## Files Scanned

All tracked files returned by `git ls-files` were scanned. Binary assets were included in path scans, and text-like tracked files were scanned by content. The full file list is stored in the JSON `files_scanned` array.

| Required scope | Count |
| --- | ---: |
| `README.md` | `1` |
| `PANEL_QUICK_START.md` | `1` |
| `docs/` | `30` |
| `config/` | `25` |
| `start.ps1` | `1` |
| `panel.ps1` | `1` |
| `ui/` | `30` |
| `src/` | `106` |
| `scripts/` | `23` |
| `tests/` | `82` |
| `output/*README*` | `4` |
| `output/*manifest*` | `33` |
| `thesis_outputs/` | `10` |

## Critical

- No forbidden runtime-constructed manuscript-label hits were found in tracked files.
- No uploaded-manuscript filename references were found by the document-filename pattern scan.
- No positive claim-boundary overclaim was identified for exact 1 km success, universal operational accuracy, PyGNOME as truth, DWH as Mindoro recalibration, p90 as a broad envelope, or oil-type support as primary validation. Matching hits were guardrails or negations.

## Needs Cleanup

- Launcher metadata and display paths used legacy evidence-placement vocabulary at audit time. Later cleanup should rename it to final manuscript section vocabulary without changing launcher routing or science.
- `README.md`, `PANEL_QUICK_START.md`, `docs/COMMAND_MATRIX.md`, `docs/PANEL_REVIEW_GUIDE.md`, `docs/PHASE_STATUS.md`, and `docs/THESIS_SURFACE_GOVERNANCE.md` contained older manuscript-current wording at audit time. The evidence order was aligned, but the heading language should be made final-paper-facing.
- Some stored outputs and services still carry `not_frozen`, `inherited_provisional`, or similar provenance/status wording. Treat this as a vocabulary/routing cleanup, not a reason to delete outputs or change scientific claims.
- `output/defense_readiness/*` contains stale launcher snapshots with older launcher-section labels and catalog wording. It should be regenerated after launcher vocabulary cleanup or routed as archived readiness provenance.

## Archive Routing

- `phase1_regional_reference`: route as archive-provenance / regional-reference support. Reason: Final paper uses the focused Mindoro Phase 1 provenance lane for B1; regional 2016-2022 work should remain preserved but not primary.
- `march_family_archive_rows`: route as archive-provenance / appendix-support / experimental-only, depending on launcher entry. Reason: Mindoro B1 March 13-14 is the only main Philippine public-observation validation claim.
- `prototype_2016_legacy_outputs`: route as legacy-support / secondary-support only. Reason: Secondary 2016 material supports history and comparator context but is not public-spill validation and not a replacement for Mindoro B1 or DWH.
- `generic_publication_package_legacy_items`: keep the package read-only, but route legacy/prototype figures to legacy-support indexes and final-paper figures to explicit final-paper registry entries. Reason: Some filenames include paper-style labels for legacy/prototype assets; metadata generally marks support-only, but later cleanup should make routing unmistakable.
- `defense_readiness_stale_launcher_snapshots`: route as regenerate or archive after launcher vocabulary migration. Reason: Snapshot contains old launcher display text and older catalog wording; do not delete, but route as stale provenance or refresh after cleanup.

## Already Aligned

- Repository title in README matches the final-paper title.
- README, PANEL_QUICK_START, and PHASE_STATUS present the final evidence order and key B1/DWH/oil-type values.
- Focused Phase 1 values and recipe ranking match the final-paper facts.
- Mindoro B1 values match the final-paper facts and include the neighborhood-usefulness limitation.
- Mindoro same-case PyGNOME and DWH PyGNOME language is comparator-only.
- DWH is routed as external transfer validation, not Mindoro recalibration.
- Oil-type and shoreline material is described as support/context only.
- Secondary 2016 drifter benchmark is marked secondary support only.
- No forbidden label hits were found in tracked files.
- No uploaded-manuscript filename references were found by the document-filename pattern scan.

## Launcher And Validation Notes

- Required `git grep` checks for universal operational accuracy and exact 1 km wording ran. Hits are guardrail statements, not positive overclaims.
- `pwsh ./start.ps1 -ValidateMatrix -NoPause` and `pwsh ./start.ps1 -List -NoPause` could not run because `pwsh` is not installed in this environment.
- Windows PowerShell fallback succeeded for `./start.ps1 -ValidateMatrix -NoPause`: 27 entries passed, 0 failed.
- Windows PowerShell fallback succeeded for `./start.ps1 -List -NoPause`, with the vocabulary cleanup noted above.
- Python JSON validation of `config/launcher_matrix.json` passed and found required final-paper route IDs present.

## Remediation Plan For Later Prompts

1. Rename launcher evidence-placement vocabulary to final manuscript section wording across config, schema, start.ps1, validation utilities, tests, and docs.
2. Update docs headings from older manuscript-current wording to Final Paper / Final-Paper Alignment, then adjust scripts/check_docs_against_manuscript_claims.py expectations.
3. Separate final-paper evidence status from repository provenance status for inherited-provisional/not_frozen fields without deleting stored outputs.
4. Add or refresh archive-routing metadata for Phase 1 regional reference, March-family experiments, prototype 2016/2021, and secondary 2016 drifter-track outputs.
5. Regenerate read-only defense readiness and launcher documentation snapshots after vocabulary cleanup, preserving old snapshots only as archive provenance if needed.

## Boundary Statement

The audit is read-only with respect to scientific interpretation. It records where later prompts should adjust wording, routing, and archive labels. It does not promote archived, experimental, legacy, comparator-only, or support/context outputs into the main final-paper claim, and it does not demote the currently aligned final-paper evidence paths.
