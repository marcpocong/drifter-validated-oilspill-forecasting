# Final Validation Claims Guardrails

- Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents is represented by Mindoro B1, the March 13 -> March 14 NOAA reinit validation, and it should be described with the explicit March 12 WorldView-3 caveat.
- The separate focused 2016-2023 Mindoro drifter rerun now supplies the active cmems_era5 recipe provenance used to frame B1, but it does not replace the original B1 raw provenance.
- The broader 2016-2022 regional rerun is preserved as a reference/governance lane and is not the active provenance for B1.
- Mindoro B2 and B3 remain legacy/reference rows, with B2 framed as honesty-only, and they should not be silently rewritten as if they never existed.
- PyGNOME is a comparator, not truth, in both the promoted Mindoro cross-model lane and the DWH cross-model comparison.
- DWH observed masks are truth for Phase 3C.
- DWH currently demonstrates workflow transferability and meaningful spatial skill under real historical forcing.
- On DWH, OpenDrift outperforms PyGNOME under the current case definition.
- On DWH, ensemble p50 improves overall mean FSS while deterministic remains strongest on the May 21-23 event corridor.
- DWH Phase 3C is scientifically reportable even if some optional future extensions remain.
- Do not relabel legacy/reference or sensitivity products as if they were the new promoted primary validation row.
