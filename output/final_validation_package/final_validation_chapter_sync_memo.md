# Final Validation Chapter 3 Sync Memo

## Recommended Chapter 3 Structure

1. Phase 1 = Transport Validation and Baseline Configuration Selection
2. Phase 2 = Standardized Machine-Readable Forecast Product Generation
3. Phase 3A = Mindoro March 13 -> March 14 Cross-Model Comparator
4. Phase 3B1 = Mindoro March 13 -> March 14 NOAA Reinit Primary Validation
5. Phase 3B2 = Mindoro Legacy March 6 Sparse Strict Reference
6. Phase 3B3 = Mindoro Legacy March 3-6 Broader-Support Reference
7. Phase 3C = External Rich-Data Spill Transfer Validation (Deepwater Horizon 2010)
8. Phase 4 = Oil-Type Fate and Shoreline Impact Analysis
9. Phase 5 = Reproducibility, Packaging, and Deliverables

## Insertion-Ready Methodology Amendment Note

The written methodology should be amended to reflect the current repository state without rewriting the frozen case-definition history. The original `config/case_mindoro_retro_2023.yaml` remains the preserved March 3 -> March 6 base-case definition, and it should still be described as the frozen starting case configuration for the Mindoro retrospective workflow. However, the thesis-facing primary validation row is now the promoted Phase 3B1 March 13 -> March 14 public-validation pair, which is carried through the separate amendment path rather than by silently overwriting the original base-case YAML. This framing preserves provenance: the repo keeps the original case-definition artifact intact, but the reported main validation result is the later March 13 -> March 14 row because it is the clearest promoted next-day public-validation pair available in the current package.

That promotion should be described carefully. March 13 -> March 14 is a reinitialization-based public-validation row, not a retrospective claim that the original March 3 release forecast independently reproduced a fully new satellite day without amendment. The March 6 row must therefore remain visible in the methodology as a retained honesty/reference case. It should be presented as the legacy sparse strict reference, useful for showing the hardest early-case edge condition and for documenting the older reporting path, but it should no longer be described as the main Mindoro validation row. In short, the methods story changes at the level of thesis reporting priority, not at the level of frozen artifact deletion.

## Insertion-Ready Phase 1 Defense

Phase 1 should be defended as a historical and regional transport-calibration window spanning 2016-2022 rather than as a single-year prototype study. Its role is to assemble a strict drogued-only, non-overlapping 72-hour validation corpus within the fixed regional transport-validation box so that baseline recipe selection is anchored in a broader pre-2023 transport record rather than in one convenient demonstration year. This is why the repo separates the dedicated `phase1_regional_2016_2022` lane from the prototype lanes: the official Phase 1 evidence base is the historical/regional corpus and its staged baseline-selection outputs, not the older prototype-only story.

The 2021 cases should therefore be described only as support or demonstration context. They remain useful because they provide accepted-segment examples, debug continuity, and a compact way to inspect how the official Phase 1 recipe family behaves on recognizable trajectories. However, they do not redefine the official Phase 1 study window, and they should not be presented as if the thesis calibrated its final baseline on 2021 alone. The correct defense is that 2021 remains a support/demo subset within a wider 2016-2022 calibration frame, while the prototype lanes as a whole remain secondary to the dedicated regional rerun when making thesis-facing claims.

## Insertion-Ready Domain-Box Defense

The regional transport-validation box and the Mindoro spill scoring domain should be described as complementary domains serving different evidentiary functions. The regional transport-validation box is the fixed historical/regional audit window used in Phase 1 to assemble drifter-based transport evidence, evaluate recipe behavior, and support baseline selection on a geographically consistent corpus. Its purpose is calibration and transport-skill auditing across the broader regional circulation context.

The Mindoro spill scoring domain, by contrast, is the event-specific observation grid and scoring frame used to compare stored model footprints against observed spill masks in the spill-case validation phases. Its purpose is not to replace the regional calibration box, but to make the Mindoro case scientifically scoreable on the domain where the spill observations actually exist and where the case-specific validation claim is made. The methodological defense is therefore that the broader regional box answers the question of transport calibration, while the Mindoro scoring domain answers the different question of event-specific spill validation. Using both does not create a contradiction; it preserves scale-appropriate evidence handling.

## Insertion-Ready Limitations Paragraph

The promoted Mindoro Phase 3B1 row must be reported with two explicit limitations. First, both NOAA/NESDIS public products used in the March 13 -> March 14 pair cite March 12 WorldView-3 imagery, so this row should be interpreted as a promoted reinitialization-based public-validation pair with shared-imagery provenance rather than as a fully independent day-to-day satellite validation. Second, the broader workflow still carries an unfinished freeze story upstream of the spill-case lane: Phase 1 has a candidate historical/regional baseline staged, but if that candidate has not yet been manually promoted into the official default baseline path, then Phase 2 and downstream spill-case defaults remain scientifically usable yet methodologically provisional with respect to final baseline freeze. This limitation should be stated openly so that the thesis does not over-claim a completed calibration closure that the repository has not yet formally promoted.

## Packaging Guidance

- Keep Mindoro as the main Philippine case.
- Keep DWH as the rich-data external transfer-validation branch.
- Present Phase 3A as comparator-only benchmarking, not as a truth-source replacement.
- Preserve `config/case_mindoro_retro_2023.yaml` as the frozen March 3 -> March 6 case definition and carry the Phase 3B1 promotion through the amendment path instead.
- Present March 13 -> March 14 as the canonical Mindoro validation with the shared-imagery caveat stated explicitly.
- Keep March 6 and March 3-6 visible as legacy/reference material rather than deleting or hiding them.
- Keep drifters, observed masks, PyGNOME, DWH, and prototype lanes in separate evidentiary roles.
