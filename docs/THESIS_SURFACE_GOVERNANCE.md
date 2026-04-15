# Thesis Surface Governance

## Purpose

This repo keeps one canonical artifact-surface vocabulary in `src/core/artifact_status.py` so the UI, publication registry, and validation/export packaging all agree on what is thesis-facing, support-only, archive-only, legacy/prototype support, or advanced-only.

Surface keys:

- `thesis_main`: eligible for thesis-facing home and main presentation surfaces.
- `comparator_support`: support, comparator, or context material that stays separate from the primary thesis claim.
- `archive_only`: preserved for provenance, reproducibility, and audit only.
- `legacy_support`: legacy or prototype support material that remains visible as support only.
- `advanced_only`: lower-level inspection material that should stay out of default thesis-facing surfaces.

Publication-package inventory fields:

- `thesis_surface`: whether a publication figure is part of the curated thesis-facing surface.
- `archive_only`: whether a publication figure is indexed as archive/provenance only.
- `legacy_support`: whether a publication figure belongs to a legacy/prototype support lane.
- `comparator_support`: whether a publication figure is comparator-only support.
- `display_order`: stable ordering for curated publication-package browsing.
- `page_target`: the intended dashboard/package landing page for the figure.
- `study_box_id`: optional stable identifier for shared study-box geography figures.
- `study_box_numbers`: explicit study-box number or number set shown by a study-context figure.
- `recommended_scope`: normalized scope such as `main_text`, `page_support`, `legacy_support`, `appendix_support`, or `archive_only`.

## Main Thesis Surfaces

Main thesis-facing surfaces should foreground only the artifacts that directly support the current thesis narrative:

- Mindoro March 13 -> March 14 `R1` is the only thesis-facing `B1` row.
- Track `A` is support/comparator context attached to `B1`, not a co-primary result.
- DWH `C1` and `C2` remain thesis-facing transfer-validation material, with `C3` staying comparator-only.
- Study Boxes `2` and `4` are the only thesis-facing study-context boxes; Study Boxes `1` and `3` are archive/advanced/support only.

Main thesis surfaces must not promote preserved archive rows just because an older manifest, figure flag, or board title still exists.

## Archive And Advanced Surfaces

Archive or advanced surfaces are the right place for preserved but non-promoted material:

- Mindoro March 13 -> March 14 `R0` is `archive_only`.
- Preserved March-family legacy rows, including `B2` and `B3`, are `archive_only`.
- Lower-level trajectory/context inspection stays `advanced_only` unless it has been explicitly promoted elsewhere.

## Why Preserve But Not Promote

Archive material is preserved because it still matters for:

- provenance and methods traceability
- reproducibility and audit
- explaining why the promoted row was chosen instead of silently erasing earlier rows

Archive material is not promoted because preservation is not the same as recommendation. The thesis-facing story is intentionally narrower than the full preserved repo history.
