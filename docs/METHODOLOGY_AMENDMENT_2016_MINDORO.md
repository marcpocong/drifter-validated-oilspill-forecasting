# Methodology Amendment: 2016 And Mindoro

## Purpose

This note aligns the thesis-facing wording in the repo with the current code, launcher behavior, and stored artifacts. It is a wording sync only. It does not rerun science, rewrite stored outputs, or promote any staged baseline artifact automatically.

## Prototype 2016

`prototype_2016` remains a legacy support-only lane. Thesis-facing, it is preserved as:

`Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5`

This lane records the earliest prototype stage of the study, keeps the selected drifter-of-record start as the authoritative release origin, and keeps PyGNOME comparator-only. It is not rewritten into a Mindoro-style spill-validation lane, and it does not gain thesis-facing `Phase 3B` or `Phase 3C`.

## Mindoro

Mindoro keeps two separate stories:

- A separate `phase1_mindoro_focus_pre_spill_2016_2023` rerun used only as confirmation for the recipe story.
- The canonical spill-case validation story built from the frozen March 3 -> March 6 base case, official Phase 2 outputs, and the promoted March 13 -> March 14 B1 primary validation row.

Thesis-facing, the Mindoro sequence is:

separate focused Phase 1 confirmation -> Phase 2 -> Phase 3B primary validation

The focused Phase 1 rerun is confirmation-only. It does not replace canonical baseline governance, does not rewrite the stored B1 raw-generation history, and does not auto-promote any staged baseline artifact over `config/phase1_baseline_selection.yaml`.

## B1 And B2

March 13 -> March 14 is the canonical B1 primary validation row through `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`.

March 6 remains visible as the B2 legacy honesty row. It stays in the repo so the original provenance is not silently rewritten and so the methods/limitations story remains honest.

The shared-imagery caveat remains explicit: both public products cite March 12 WorldView-3 imagery, so March 13 -> March 14 is a reinitialization-based public-validation pair, not an independent day-to-day validation.

## PyGNOME

PyGNOME stays comparator-only in every lane discussed here.

- In `prototype_2016`, it remains legacy support/comparator evidence.
- In Mindoro, the March 13 -> March 14 PyGNOME lane is same-case supporting comparator evidence only.
- In DWH, PyGNOME remains comparator-only and DWH stays separate as Phase 3C.
