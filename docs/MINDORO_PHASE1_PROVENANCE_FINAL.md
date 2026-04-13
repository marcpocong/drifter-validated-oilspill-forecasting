## Mindoro Phase 1 Provenance Final

Mindoro B1 now inherits its recipe provenance from the separate focused `phase1_mindoro_focus_pre_spill_2016_2023` drifter-based Phase 1 rerun.

Authoritative current provenance:
- Active workflow mode: `phase1_mindoro_focus_pre_spill_2016_2023`
- Active selected recipe: `cmems_era5`
- Recipe family actually tested in that focused lane: `cmems_era5`, `hycom_era5`
- Active baseline file: `config/phase1_baseline_selection.yaml`

Important honesty notes:
- This focused rerun searched through early 2023, but its accepted registry does not include near-2023 accepted segments.
- Archived NOAA/NCEI GFS access was unavailable during the focused rerun, so GFS-backed recipes were excluded there rather than silently treated as if they passed.
- Phase 3B does not directly ingest drifters. It inherits a recipe selected by the separate focused drifter-based Phase 1 rerun.
- The stored March 13 -> March 14 B1 science bundle keeps its original raw-generation history.

Governance result:
- Mindoro B1 provenance: focused Mindoro Phase 1 lane
- Broader `phase1_regional_2016_2022` lane: preserved regional reference/governance lane
- March 6 B2: preserved legacy honesty row
- PyGNOME on March 13 -> March 14: comparator-only
