# Archive Governance

This repository preserves exploratory, legacy, comparator-only, and development outputs so the scientific history remains inspectable. Preservation is not promotion: archive entries are retained for audit, regression, and provenance, while the final manuscript claim remains anchored to the final-paper evidence order.

The machine-readable registry is [`config/archive_registry.yaml`](../config/archive_registry.yaml). It records each archive item's launcher entry, path patterns, status, claim boundary, rerun policy, and protected outputs.

## Archive Versus Thesis-Facing Evidence

Thesis-facing evidence is limited to the final manuscript lanes:

1. Focused Mindoro Phase 1 transport provenance.
2. Phase 2 standardized deterministic and 50-member forecast products.
3. Mindoro B1 March 13-14 primary public-observation validation.
4. Mindoro same-case OpenDrift-PyGNOME comparator support.
5. DWH external transfer validation.
6. Mindoro oil-type and shoreline support/context.
7. Secondary 2016 drifter-track and legacy FSS support.
8. Reproducibility, governance, and read-only package surfaces.

Archive entries differ in three ways:

- They have `thesis_facing: false`.
- They have `reportable: false` unless they are explicitly support/context in the final evidence order.
- Their `claim_boundary` states what they cannot be used to claim.

The archive does not change the final paper claim. It does not promote old March 3-6 Mindoro rows, experimental PhilSA or multisource branches, PyGNOME comparators, DWH transfer checks, or oil-type support into primary validation.

## Archive Status Values

- `archive_provenance`: Historical scientific work retained for traceability, not final-paper promotion.
- `legacy_support`: Older prototype or 2016 support surfaces retained for regression and appendix/support context.
- `experimental_only`: Exploratory branches hidden from normal panel routes; not thesis-facing and not reportable.
- `repository_only_development`: Debug or development artifacts listed for cleanup visibility without changing scientific outputs.

## Inspecting Archive Routes

Use the launcher role groups rather than raw workflow modes:

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -ListRole archive_provenance -NoPause
.\start.ps1 -ListRole legacy_support -NoPause
.\start.ps1 -Explain phase1_regional_reference_rerun -NoPause
.\start.ps1 -Explain phase1_production_rerun -NoPause
.\start.ps1 -Explain mindoro_march13_14_phase1_focus_trial -NoPause
```

On macOS or Linux with PowerShell 7:

```bash
pwsh ./start.ps1 -ListRole archive_provenance -NoPause
pwsh ./start.ps1 -ListRole legacy_support -NoPause
```

The unfiltered launcher list is grouped by thesis role. Archive/provenance, legacy, support, read-only, hidden-alias, and hidden-experimental routes stay inspectable, but they do not flatten into the main thesis evidence group.

Hidden aliases and hidden experimental entries remain resolvable by explicit ID for audit or compatibility, but they are not panel defaults and are not part of the main thesis evidence group. `-Explain <hidden_id>` prints both the requested ID and the canonical entry ID before any run confirmation.

The dashboard follows the same split. Panel-friendly navigation shows the final-paper evidence order first and routes preserved archive material through `Archive/Provenance and Legacy Support`; legacy/debug items stay visibly marked and do not appear on the main panel page as final-paper validation claims.

## Governance Rules

- Do not delete archive outputs simply because they are no longer thesis-facing.
- Do not cite archive or experimental outputs as primary paper evidence.
- Do not treat PyGNOME as observation truth; it remains comparator-only.
- Do not use DWH as Mindoro recalibration; it remains external transfer validation.
- Do not describe oil-type or shoreline outputs as primary validation.
- Do not use old March 3-6 base-case rows as a replacement for B1.
- Do not relabel `mask_p90` as a broad envelope; it is conservative support/comparison only.

## Validation

Run the launcher and archive checks before changing archive routing:

```powershell
.\start.ps1 -ValidateMatrix -NoPause
python scripts\validate_archive_registry.py
```

The validator checks that registry launcher entries exist unless marked `path_only`, archive entries are not thesis-facing, and hidden experimental launcher entries remain hidden, non-reportable, and outside the main evidence group.
