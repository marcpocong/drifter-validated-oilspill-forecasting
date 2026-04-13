# Methodology Amendment: 2016 And Mindoro

## Purpose

This note aligns the thesis-facing wording in the repo with the current code, launcher behavior, and stored artifacts. It is a wording sync only. It does not rerun science, rewrite stored outputs, or promote any staged baseline artifact automatically.

## Prototype 2016

`prototype_2016` remains a legacy support-only lane. Thesis-facing, it is preserved as:

`Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5`

This lane records the earliest prototype stage of the study, keeps the selected drifter-of-record start as the authoritative release origin, and keeps PyGNOME comparator-only. It is not rewritten into a Mindoro-style spill-validation lane, and it does not gain thesis-facing `Phase 3B` or `Phase 3C`.

## Mindoro

Mindoro keeps two separate stories:

- A separate `phase1_mindoro_focus_pre_spill_2016_2023` rerun now used as the active Mindoro-specific recipe-provenance lane for the B1 story.
- The canonical spill-case validation story built from the frozen March 3 -> March 6 base case, official Phase 2 outputs, and the promoted March 13 -> March 14 B1 primary validation row.

Thesis-facing, the Mindoro sequence is:

separate focused drifter-based Phase 1 provenance -> Phase 2 -> Phase 3B primary validation

The focused Phase 1 rerun now supplies the active Mindoro-specific recipe provenance. It does not rewrite the stored B1 raw-generation history, does not claim that Phase 3B itself directly used drifters, and does not erase the broader 2016-2022 regional rerun, which remains preserved as reference/governance context.

## B1 And B2

March 13 -> March 14 is the canonical B1 primary validation row through `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`.

March 6 remains visible as the B2 legacy honesty row. It stays in the repo so the original provenance is not silently rewritten and so the methods/limitations story remains honest.

The shared-imagery caveat remains explicit: both public products cite March 12 WorldView-3 imagery, so March 13 -> March 14 is a reinitialization-based public-validation pair, not an independent day-to-day validation.

## PyGNOME

PyGNOME stays comparator-only in every lane discussed here.

- In `prototype_2016`, it remains legacy support/comparator evidence.
- In Mindoro, the March 13 -> March 14 PyGNOME lane is same-case supporting comparator evidence only.
- In DWH, PyGNOME remains comparator-only and DWH stays separate as Phase 3C.
