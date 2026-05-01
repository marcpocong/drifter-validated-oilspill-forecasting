"""Align final Chapter 4 figure labels to stored publication-package paths.

This is a registry/crosswalk patcher only. It reads already stored figures and
CSV/JSON registries, then updates figure-package metadata and docs. It does not
run OpenDrift, PyGNOME, downloads, or manuscript-PDF extraction.
"""

from __future__ import annotations

import csv
import json
import subprocess
import struct
from pathlib import Path


ROOT = Path(".")
PACKAGE = ROOT / "output" / "figure_package_publication"
REGISTRY_PATH = PACKAGE / "publication_figure_registry.csv"
MANIFEST_PATH = PACKAGE / "publication_figure_manifest.json"
CAPTIONS_PATH = PACKAGE / "publication_figure_captions.md"
INVENTORY_PATH = PACKAGE / "publication_figure_inventory.md"
TALKING_POINTS_PATH = PACKAGE / "publication_figure_talking_points.md"
CONFIG_PATH = ROOT / "config" / "paper_to_output_registry.yaml"
CROSSWALK_PATH = ROOT / "docs" / "PAPER_TO_REPO_CROSSWALK.md"
PAPER_OUTPUT_MD_PATH = ROOT / "docs" / "PAPER_OUTPUT_REGISTRY.md"


def tracked_repo_paths() -> set[str]:
    output = subprocess.check_output(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        encoding="utf-8",
        errors="replace",
    )
    return {item.replace("\\", "/") for item in output.split("\0") if item}


def tracked_config_paths(item: dict, tracked: set[str]) -> list[str]:
    paths = [item["path"]]
    sidecar = str(Path(item["path"]).with_suffix(".json")).replace("\\", "/")
    if item["status"] == "generated_from_stored_outputs" and sidecar in tracked:
        paths.append(sidecar)
    for source in item["sources"]:
        normalized = source.replace("\\", "/")
        if normalized in tracked:
            paths.append(source)
    return paths


FINAL_FIGURES = [
    {
        "paper_id": "figure_4_1",
        "id": "figure_4_1_focused_phase1_accepted_feb_apr_segment_map",
        "label": "Figure 4.1. Focused Phase 1 accepted February–April segment map",
        "path": "output/figure_package_publication/figure_4_1_focused_phase1_accepted_feb_apr_segment_map.png",
        "case": "PHASE1_MINDORO_FOCUS_PRE_SPILL_2016_2023",
        "phase": "phase1_mindoro_focus_pre_spill_2016_2023",
        "date": "2016-01-01_to_2023-03-02",
        "model": "drifter_segments",
        "run_type": "single_segment_map",
        "view": "map",
        "status_key": "focused_phase1_transport_provenance",
        "status_label": "Focused Phase 1 transport provenance",
        "role": "transport_provenance",
        "surface": "thesis_main",
        "scope": "main_text",
        "order": 4010,
        "sources": [
            "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_accepted_segment_registry.csv",
            "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_ranking_subset_registry.csv",
        ],
        "interpretation": "Focused Phase 1 accepted segment map for the February–April ranked subset; transport provenance only.",
        "notes": "Generated from stored Phase 1 segment registries only; not a public-spill validation figure.",
        "status": "generated_from_stored_outputs",
        "boundary": "Transport provenance only.",
    },
    {
        "paper_id": "figure_4_2",
        "id": "figure_4_2_focused_phase1_recipe_ranking_chart",
        "label": "Figure 4.2. Focused Phase 1 recipe ranking chart",
        "path": "output/figure_package_publication/figure_4_2_focused_phase1_recipe_ranking_chart.png",
        "case": "PHASE1_MINDORO_FOCUS_PRE_SPILL_2016_2023",
        "phase": "phase1_mindoro_focus_pre_spill_2016_2023",
        "date": "2016-01-01_to_2023-03-02",
        "model": "recipe_ranking",
        "run_type": "single_recipe_ranking_chart",
        "view": "chart",
        "status_key": "focused_phase1_recipe_provenance",
        "status_label": "Focused Phase 1 recipe ranking",
        "role": "transport_provenance",
        "surface": "thesis_main",
        "scope": "main_text",
        "order": 4020,
        "sources": [
            "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_recipe_ranking.csv",
            "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_recipe_summary.csv",
        ],
        "interpretation": "Focused Phase 1 recipe ranking chart; lower raw NCS indicates better transport agreement.",
        "notes": "Generated from stored focused Phase 1 ranking tables only; recipe provenance, not oil-footprint truth.",
        "status": "generated_from_stored_outputs",
        "boundary": "Recipe provenance only.",
    },
    {
        "paper_id": "figure_4_3",
        "id": "figure_4_3_mindoro_product_family_board",
        "label": "Figure 4.3. Mindoro product-family board with deterministic, probability, and threshold surfaces",
        "path": "output/figure_package_publication/figure_4_3_mindoro_product_family_board.png",
        "case": "CASE_MINDORO_RETRO_2023",
        "phase": "phase2_official_product_family",
        "date": "2023-03-06",
        "model": "opendrift",
        "run_type": "product_family_board",
        "view": "board",
        "status_key": "mindoro_product_family_support",
        "status_label": "Mindoro standardized product family",
        "role": "product_support",
        "surface": "thesis_main",
        "scope": "main_text",
        "order": 4030,
        "sources": [
            "output/CASE_MINDORO_RETRO_2023/forecast/control_footprint_mask_2023-03-06T09-59-00Z.tif",
            "output/CASE_MINDORO_RETRO_2023/official_rerun_r1/R1_selected_previous/forecast_datecomposites/prob_presence_2023-03-06_datecomposite.tif",
            "output/CASE_MINDORO_RETRO_2023/official_rerun_r1/R1_selected_previous/forecast_datecomposites/mask_p50_2023-03-06_datecomposite.tif",
            "output/CASE_MINDORO_RETRO_2023/official_rerun_r1/R1_selected_previous/forecast_datecomposites/mask_p90_2023-03-06_datecomposite.tif",
            "output/final_reproducibility_package/final_output_catalog.csv",
        ],
        "interpretation": "Mindoro product-family board for deterministic, prob_presence, mask_p50, and mask_p90 surfaces.",
        "notes": "Generated from stored product rasters only; mask_p50 is preferred and mask_p90 is conservative high-confidence support only.",
        "status": "generated_from_stored_outputs",
        "boundary": "`mask_p50` preferred; `mask_p90` conservative support only.",
    },
    {
        "paper_id": "figure_4_4",
        "id": "figure_4_4_mindoro_primary_validation_board",
        "label": "Figure 4.4. Mindoro primary validation board",
        "path": "output/figure_package_publication/figure_4_4_mindoro_primary_validation_board.png",
        "case": "CASE_MINDORO_RETRO_2023",
        "phase": "phase3b_reinit_primary",
        "date": "2023-03-13_to_2023-03-14",
        "model": "opendrift",
        "run_type": "comparison_board",
        "view": "board",
        "status_key": "mindoro_primary_validation",
        "status_label": "Mindoro March 13-14 primary validation row",
        "role": "primary_validation",
        "surface": "thesis_main",
        "scope": "main_text",
        "order": 4040,
        "sources": [
            "output/Phase 3B March13-14 Final Output/publication/opendrift_primary/mindoro_primary_validation_board.png",
            "output/Phase 3B March13-14 Final Output/summary/opendrift_primary/march13_14_reinit_summary.csv",
        ],
        "interpretation": "Mindoro primary validation board; supports coastal-neighborhood usefulness, not exact overlap.",
        "notes": "Copied from stored Phase 3B final publication output; IoU and Dice remain zero and no exact-grid success is claimed.",
        "status": "existing",
        "boundary": "Primary Mindoro supports neighborhood agreement only, not exact overlap.",
    },
    {
        "paper_id": "figure_4_4a",
        "id": "figure_4_4A_noaa_mar13_worldview3",
        "label": "Figure 4.4A. NOAA-published March 13 WorldView-3 analysis map",
        "path": "output/figure_package_publication/figure_4_4A_noaa_mar13_worldview3.png",
        "case": "CASE_MINDORO_RETRO_2023",
        "phase": "phase3b_reinit_primary",
        "date": "2023-03-13",
        "model": "observation",
        "run_type": "support_noaa_analysis_map",
        "view": "single",
        "status_key": "mindoro_primary_validation",
        "status_label": "NOAA-published March 13 public observation",
        "role": "primary_validation_support",
        "surface": "thesis_main",
        "scope": "appendix_support",
        "order": 4041,
        "sources": [
            "output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4A_noaa_mar13_worldview3.png"
        ],
        "interpretation": "NOAA-published March 13 public seed observation support map.",
        "notes": "Stored support figure; March 13 is the seed observation, not a target validation score.",
        "status": "existing",
        "boundary": "Primary Mindoro supports neighborhood agreement only, not exact overlap.",
    },
    {
        "paper_id": "figure_4_4b",
        "id": "figure_4_4B_noaa_mar14_worldview3",
        "label": "Figure 4.4B. NOAA-published March 14 WorldView-3 analysis map",
        "path": "output/figure_package_publication/figure_4_4B_noaa_mar14_worldview3.png",
        "case": "CASE_MINDORO_RETRO_2023",
        "phase": "phase3b_reinit_primary",
        "date": "2023-03-14",
        "model": "observation",
        "run_type": "support_noaa_analysis_map",
        "view": "single",
        "status_key": "mindoro_primary_validation",
        "status_label": "NOAA-published March 14 public observation",
        "role": "primary_validation_support",
        "surface": "thesis_main",
        "scope": "appendix_support",
        "order": 4042,
        "sources": [
            "output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4B_noaa_mar14_worldview3.png"
        ],
        "interpretation": "NOAA-published March 14 public target observation support map.",
        "notes": "Stored support figure; March 14 is the independent target observation.",
        "status": "existing",
        "boundary": "Primary Mindoro supports neighborhood agreement only, not exact overlap.",
    },
    {
        "paper_id": "figure_4_4c",
        "id": "figure_4_4C_arcgis_mar13_mar14_observed_overlay",
        "label": "Figure 4.4C. ArcGIS overlay of March 13 and March 14 observed oil-spill extents",
        "path": "output/figure_package_publication/figure_4_4C_arcgis_mar13_mar14_observed_overlay.png",
        "case": "CASE_MINDORO_RETRO_2023",
        "phase": "phase3b_reinit_primary",
        "date": "2023-03-13_to_2023-03-14",
        "model": "observation",
        "run_type": "support_arcgis_observed_overlay",
        "view": "single",
        "status_key": "mindoro_primary_validation",
        "status_label": "March 13 and March 14 observed public extents",
        "role": "primary_validation_support",
        "surface": "thesis_main",
        "scope": "appendix_support",
        "order": 4043,
        "sources": [
            "output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4C_arcgis_mar13_mar14_observed_overlay.png"
        ],
        "interpretation": "Overlay of March 13 and March 14 observed oil-spill extents.",
        "notes": "Stored observation-pair support figure; does not imply exact forecast overlap.",
        "status": "existing",
        "boundary": "Primary Mindoro supports neighborhood agreement only, not exact overlap.",
    },
    {
        "paper_id": "figure_4_5",
        "id": "figure_4_5_mindoro_same_case_opendrift_pygnome_spatial_comparator_board",
        "label": "Figure 4.5. Mindoro same-case OpenDrift–PyGNOME spatial comparator board",
        "path": "output/figure_package_publication/figure_4_5_mindoro_same_case_opendrift_pygnome_spatial_comparator_board.png",
        "case": "CASE_MINDORO_RETRO_2023",
        "phase": "phase3a_reinit_crossmodel",
        "date": "2023-03-13_to_2023-03-14",
        "model": "opendrift_vs_pygnome",
        "run_type": "comparison_board",
        "view": "board",
        "status_key": "mindoro_crossmodel_comparator",
        "status_label": "Mindoro same-case comparator",
        "role": "comparator_only",
        "surface": "comparator_support",
        "scope": "main_text",
        "order": 4050,
        "sources": [
            "output/Phase 3B March13-14 Final Output/publication/comparator_pygnome/Figure_4_5_Mindoro_TrackA_OpenDrift_PyGNOME_spatial_board.png",
            "output/Phase 3B March13-14 Final Output/summary/comparator_pygnome/march13_14_reinit_crossmodel_summary.csv",
        ],
        "interpretation": "Mindoro same-case OpenDrift–PyGNOME spatial comparator board.",
        "notes": "Copied from stored comparator board; PyGNOME is comparator-only and never observational truth.",
        "status": "existing",
        "boundary": "Mindoro comparator-only; PyGNOME is not truth.",
        "comparator": True,
    },
    {
        "paper_id": "figure_4_6",
        "id": "figure_4_6_mindoro_comparator_mean_fss_summary",
        "label": "Figure 4.6. Mindoro same-case OpenDrift–PyGNOME comparator mean FSS summary",
        "path": "output/figure_package_publication/figure_4_6_mindoro_comparator_mean_fss_summary.png",
        "case": "CASE_MINDORO_RETRO_2023",
        "phase": "phase3a_reinit_crossmodel",
        "date": "2023-03-14",
        "model": "opendrift_vs_pygnome",
        "run_type": "mean_fss_summary_chart",
        "view": "chart",
        "status_key": "mindoro_crossmodel_comparator",
        "status_label": "Mindoro same-case comparator mean FSS",
        "role": "comparator_only",
        "surface": "comparator_support",
        "scope": "main_text",
        "order": 4060,
        "sources": [
            "output/Phase 3B March13-14 Final Output/summary/comparator_pygnome/march13_14_reinit_crossmodel_model_ranking.csv"
        ],
        "interpretation": "Mindoro same-case OpenDrift–PyGNOME comparator mean FSS summary.",
        "notes": "Generated from stored comparator ranking CSV only; PyGNOME is comparator-only and not a second validation row.",
        "status": "generated_from_stored_outputs",
        "boundary": "Mindoro comparator-only; PyGNOME is not truth.",
        "comparator": True,
    },
    {
        "paper_id": "figure_4_7",
        "id": "figure_4_7_dwh_eventcorridor_observed_deterministic_mask_p50_pygnome_board",
        "label": "Figure 4.7. DWH observed, deterministic, mask_p50, and PyGNOME event-corridor board",
        "path": "output/figure_package_publication/figure_4_7_dwh_eventcorridor_observed_deterministic_mask_p50_pygnome_board.png",
        "case": "CASE_DWH_RETRO_2010_72H",
        "phase": "phase3c_dwh_pygnome_comparator",
        "date": "2010-05-21_to_2010-05-23",
        "model": "opendrift_vs_pygnome",
        "run_type": "comparison_board",
        "view": "board",
        "status_key": "dwh_crossmodel_comparator",
        "status_label": "DWH event-corridor external transfer board",
        "role": "external_transfer",
        "surface": "thesis_main",
        "scope": "main_text",
        "order": 4070,
        "sources": [
            "output/Phase 3C DWH Final Output/publication/comparator_pygnome/dwh_2010-05-21_to_2010-05-23_eventcorridor_observed_deterministic_mask_p50_pygnome_board.png"
        ],
        "interpretation": "DWH event-corridor board for observed, deterministic, mask_p50, and PyGNOME products.",
        "notes": "Copied from stored DWH final output; DWH is external transfer validation only and PyGNOME is comparator-only.",
        "status": "existing",
        "boundary": "DWH external transfer; PyGNOME comparator-only where shown.",
        "comparator": True,
    },
    {
        "paper_id": "figure_4_8",
        "id": "figure_4_8_dwh_24h_48h_72h_mask_p50_mask_p90_pygnome_overview_board",
        "label": "Figure 4.8. DWH 24 h, 48 h, and 72 h mask_p50, mask_p90, and PyGNOME overview board",
        "path": "output/figure_package_publication/figure_4_8_dwh_24h_48h_72h_mask_p50_mask_p90_pygnome_overview_board.png",
        "case": "CASE_DWH_RETRO_2010_72H",
        "phase": "phase3c_dwh_pygnome_comparator",
        "date": "2010-05-21_to_2010-05-23",
        "model": "opendrift_vs_pygnome",
        "run_type": "comparison_board",
        "view": "board",
        "status_key": "dwh_crossmodel_comparator",
        "status_label": "DWH daily p50/p90/PyGNOME overview",
        "role": "external_transfer",
        "surface": "thesis_main",
        "scope": "main_text",
        "order": 4080,
        "sources": [
            "output/Phase 3C DWH Final Output/publication/comparator_pygnome/dwh_24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board.png"
        ],
        "interpretation": "DWH 24 h, 48 h, and 72 h mask_p50, mask_p90, and PyGNOME overview board.",
        "notes": "Copied from stored DWH final output; mask_p90 is conservative high-confidence support only.",
        "status": "existing",
        "boundary": "DWH external transfer; PyGNOME comparator-only where shown.",
        "comparator": True,
    },
    {
        "paper_id": "figure_4_9",
        "id": "figure_4_9_dwh_48h_observed_deterministic_mask_p50_pygnome_board",
        "label": "Figure 4.9. DWH 48 h observed, deterministic, mask_p50, and PyGNOME board",
        "path": "output/figure_package_publication/figure_4_9_dwh_48h_observed_deterministic_mask_p50_pygnome_board.png",
        "case": "CASE_DWH_RETRO_2010_72H",
        "phase": "phase3c_dwh_pygnome_comparator",
        "date": "2010-05-22",
        "model": "opendrift_vs_pygnome",
        "run_type": "comparison_board",
        "view": "board",
        "status_key": "dwh_crossmodel_comparator",
        "status_label": "DWH 48 h external transfer board",
        "role": "external_transfer",
        "surface": "thesis_main",
        "scope": "main_text",
        "order": 4090,
        "sources": [
            "output/Phase 3C DWH Final Output/publication/comparator_pygnome/dwh_2010-05-22_48h_observed_deterministic_mask_p50_pygnome_board.png"
        ],
        "interpretation": "DWH 48 h board for observed, deterministic, mask_p50, and PyGNOME products.",
        "notes": "Copied from stored DWH final output; DWH is external transfer validation only and PyGNOME is comparator-only.",
        "status": "existing",
        "boundary": "DWH external transfer; PyGNOME comparator-only where shown.",
        "comparator": True,
    },
    {
        "paper_id": "figure_4_10",
        "id": "figure_4_10_case_2016_09_01_secondary_drifter_track_benchmark_map_panel",
        "label": "Figure 4.10. CASE_2016-09-01 secondary drifter-track benchmark map panel",
        "path": "output/figure_package_publication/figure_4_10_case_2016_09_01_secondary_drifter_track_benchmark_map_panel.png",
        "case": "CASE_2016-09-01",
        "phase": "secondary_2016_drifter_benchmark",
        "date": "2016-09-01",
        "model": "opendrift_vs_pygnome",
        "run_type": "drifter_track_benchmark_map_panel",
        "view": "panel",
        "status_key": "secondary_2016_drifter_track_support",
        "status_label": "Secondary 2016 drifter-track benchmark",
        "role": "secondary_support",
        "surface": "support",
        "scope": "main_text",
        "order": 4100,
        "sources": [
            "output/2016_drifter_benchmark/case_boards/CASE_2016-09-01_drifter_track_benchmark.png",
            "output/2016_drifter_benchmark/scorecard.csv",
        ],
        "interpretation": "Secondary drifter-track benchmark map panel for CASE_2016-09-01.",
        "notes": "Copied from stored secondary 2016 benchmark board; secondary support only, not public-spill validation.",
        "status": "existing",
        "boundary": "Secondary/legacy support only.",
        "legacy": True,
    },
    {
        "paper_id": "figure_4_11",
        "id": "figure_4_11_case_2016_09_06_secondary_drifter_track_benchmark_map_panel",
        "label": "Figure 4.11. CASE_2016-09-06 secondary drifter-track benchmark map panel",
        "path": "output/figure_package_publication/figure_4_11_case_2016_09_06_secondary_drifter_track_benchmark_map_panel.png",
        "case": "CASE_2016-09-06",
        "phase": "secondary_2016_drifter_benchmark",
        "date": "2016-09-06",
        "model": "opendrift_vs_pygnome",
        "run_type": "drifter_track_benchmark_map_panel",
        "view": "panel",
        "status_key": "secondary_2016_drifter_track_support",
        "status_label": "Secondary 2016 drifter-track benchmark",
        "role": "secondary_support",
        "surface": "support",
        "scope": "main_text",
        "order": 4110,
        "sources": [
            "output/2016_drifter_benchmark/case_boards/CASE_2016-09-06_drifter_track_benchmark.png",
            "output/2016_drifter_benchmark/scorecard.csv",
        ],
        "interpretation": "Secondary drifter-track benchmark map panel for CASE_2016-09-06.",
        "notes": "Copied from stored secondary 2016 benchmark board; secondary support only, not public-spill validation.",
        "status": "existing",
        "boundary": "Secondary/legacy support only.",
        "legacy": True,
    },
    {
        "paper_id": "figure_4_12",
        "id": "figure_4_12_case_2016_09_17_secondary_drifter_track_benchmark_map_panel",
        "label": "Figure 4.12. CASE_2016-09-17 secondary drifter-track benchmark map panel",
        "path": "output/figure_package_publication/figure_4_12_case_2016_09_17_secondary_drifter_track_benchmark_map_panel.png",
        "case": "CASE_2016-09-17",
        "phase": "secondary_2016_drifter_benchmark",
        "date": "2016-09-17",
        "model": "opendrift_vs_pygnome",
        "run_type": "drifter_track_benchmark_map_panel",
        "view": "panel",
        "status_key": "secondary_2016_drifter_track_support",
        "status_label": "Secondary 2016 drifter-track benchmark",
        "role": "secondary_support",
        "surface": "support",
        "scope": "main_text",
        "order": 4120,
        "sources": [
            "output/2016_drifter_benchmark/case_boards/CASE_2016-09-17_drifter_track_benchmark.png",
            "output/2016_drifter_benchmark/scorecard.csv",
        ],
        "interpretation": "Secondary drifter-track benchmark map panel for CASE_2016-09-17.",
        "notes": "Copied from stored secondary 2016 benchmark board; secondary support only, not public-spill validation.",
        "status": "existing",
        "boundary": "Secondary/legacy support only.",
        "legacy": True,
    },
    {
        "paper_id": "figure_4_13",
        "id": "figure_4_13_legacy_2016_opendrift_pygnome_overall_mean_fss_chart",
        "label": "Figure 4.13. Legacy 2016 OpenDrift-versus-PyGNOME overall mean FSS chart",
        "path": "output/figure_package_publication/figure_4_13_legacy_2016_opendrift_pygnome_overall_mean_fss_chart.png",
        "case": "CASE_LEGACY_2016",
        "phase": "legacy_2016_fss_support",
        "date": "2016-09-01_to_2016-09-17",
        "model": "opendrift_vs_pygnome",
        "run_type": "overall_mean_fss_chart",
        "view": "chart",
        "status_key": "legacy_2016_fss_support",
        "status_label": "Legacy 2016 OpenDrift-versus-PyGNOME mean FSS",
        "role": "secondary_support",
        "surface": "support",
        "scope": "main_text",
        "order": 4130,
        "sources": [
            "output/prototype_2016_pygnome_similarity/qa_prototype_pygnome_scorecard.png",
            "output/2016 Legacy Runs FINAL Figures/summary/phase3a/prototype_pygnome_fss_by_case_window.csv",
        ],
        "interpretation": "Legacy 2016 OpenDrift-versus-PyGNOME overall mean FSS chart.",
        "notes": "Copied from stored legacy scorecard chart; legacy comparator support only, not public-spill validation.",
        "status": "existing",
        "boundary": "Secondary/legacy support only.",
        "legacy": True,
        "comparator": True,
    },
]


def png_dimensions(relative_path: str) -> tuple[int, int]:
    path = ROOT / relative_path
    with path.open("rb") as handle:
        if handle.read(8) != b"\x89PNG\r\n\x1a\n":
            raise ValueError(f"Not a PNG: {relative_path}")
        handle.read(4)
        if handle.read(4) != b"IHDR":
            raise ValueError(f"Missing IHDR: {relative_path}")
        return struct.unpack(">II", handle.read(8))


def surface_metadata(surface: str) -> tuple[str, str]:
    if surface == "comparator_support":
        return (
            "Support / comparator surface",
            "Support, comparator, or context material kept separate from the primary thesis claim.",
        )
    if surface == "support":
        return "Support surface", "Secondary or legacy support material with bounded interpretation."
    return "Thesis-facing main surface", "Eligible for thesis-facing home and main presentation surfaces."


def csv_row(item: dict) -> dict:
    width, height = png_dimensions(item["path"])
    surface_label, surface_description = surface_metadata(item["surface"])
    generated = item["status"] == "generated_from_stored_outputs"
    return {
        "figure_id": item["id"],
        "display_title": item["label"],
        "figure_family_code": "N",
        "figure_family_label": "Final Chapter 4 figure-label contract aliases",
        "case_id": item["case"],
        "phase_or_track": item["phase"],
        "date_token": item["date"],
        "model_names": item["model"],
        "run_type": item["run_type"],
        "scenario_id": "",
        "view_type": item["view"],
        "variant": "final_label_contract",
        "relative_path": item["path"],
        "file_path": "/app/" + item["path"],
        "pixel_width": str(width),
        "pixel_height": str(height),
        "short_plain_language_interpretation": item["interpretation"],
        "recommended_for_main_defense": "True",
        "recommended_for_paper": "True",
        "source_paths": " | ".join(item["sources"]),
        "notes": item["notes"],
        "status_key": item["status_key"],
        "status_label": item["status_label"],
        "status_role": item["role"],
        "status_reportability": "generated_from_stored_outputs" if generated else "existing_stored_output_alias",
        "status_official_status": "final_submission_label_contract",
        "status_frozen_status": "stored_outputs_only",
        "status_provenance": item["notes"],
        "status_panel_text": item["interpretation"],
        "status_dashboard_summary": item["interpretation"],
        "surface_key": item["surface"],
        "surface_label": surface_label,
        "surface_description": surface_description,
        "surface_home_visible": "True",
        "surface_panel_visible": "True",
        "surface_archive_visible": "False",
        "surface_advanced_visible": "True",
        "surface_recommended_visible": "True",
        "thesis_surface": "True",
        "archive_only": "False",
        "legacy_support": "True" if item.get("legacy") else "False",
        "comparator_support": "True" if item.get("comparator") else "False",
        "display_order": str(item["order"]),
        "page_target": "final_figure_4_contract",
        "study_box_id": "",
        "study_box_numbers": "",
        "study_box_label": "",
        "recommended_scope": item["scope"],
    }


def json_entry(row: dict) -> dict:
    entry = dict(row)
    for key in ("pixel_width", "pixel_height", "display_order"):
        entry[key] = int(entry[key])
    for key in (
        "recommended_for_main_defense",
        "recommended_for_paper",
        "surface_home_visible",
        "surface_panel_visible",
        "surface_archive_visible",
        "surface_advanced_visible",
        "surface_recommended_visible",
        "thesis_surface",
        "archive_only",
        "legacy_support",
        "comparator_support",
    ):
        entry[key] = str(entry[key]).lower() == "true"
    return entry


def update_registry_and_manifest(final_rows: list[dict]) -> None:
    final_ids = {row["figure_id"] for row in final_rows}
    with REGISTRY_PATH.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        rows = [row for row in reader if row.get("figure_id") not in final_ids]
    rows.extend(final_rows)
    with REGISTRY_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    manifest["figures"] = [fig for fig in manifest.get("figures", []) if fig.get("figure_id") not in final_ids]
    manifest["figures"].extend(json_entry(row) for row in final_rows)
    manifest["figure_families_generated"] = dict(manifest.get("figure_families_generated", {}))
    manifest["figure_families_generated"]["N"] = len(final_rows)
    manifest["recommended_paper_figures"] = list(
        dict.fromkeys(list(manifest.get("recommended_paper_figures", [])) + [row["figure_id"] for row in final_rows])
    )
    manifest["recommended_main_defense_figures"] = list(
        dict.fromkeys(
            list(manifest.get("recommended_main_defense_figures", [])) + [row["figure_id"] for row in final_rows]
        )
    )
    manifest["publication_package_built_from_existing_outputs_only"] = True
    manifest["expensive_scientific_reruns_triggered"] = False
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def append_markdown_sections(final_rows: list[dict]) -> None:
    lines = ["\n## Final Chapter 4 Figure-Label Contract\n"]
    by_id = {item["id"]: item for item in FINAL_FIGURES}
    for row in final_rows:
        item = by_id[row["figure_id"]]
        status = "generated from stored outputs" if item["status"] == "generated_from_stored_outputs" else "existing stored figure alias"
        lines.append(
            f"- `{row['figure_id']}` [{item['label']}]: {row['short_plain_language_interpretation']} "
            f"Path: `{row['relative_path']}`. Status: {status}. Provenance: {row['notes']}\n"
        )
    section = "".join(lines)
    marker = "\n## Final Chapter 4 Figure-Label Contract\n"
    for path in (CAPTIONS_PATH, INVENTORY_PATH, TALKING_POINTS_PATH):
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        text = text.split(marker)[0].rstrip() + section if marker in text else text.rstrip() + "\n" + section
        path.write_text(text.rstrip() + "\n", encoding="utf-8")


def update_config_registry() -> None:
    registry = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    registry["updated"] = "2026-05-02"
    entries = registry.get("entries", [])
    by_id = {entry.get("paper_item_id"): entry for entry in entries}
    tracked = tracked_repo_paths()
    for item in FINAL_FIGURES:
        entry = by_id.get(item["paper_id"])
        if not entry:
            continue
        entry["paper_label"] = item["label"].replace(". ", " - ", 1)
        entry["claim_boundary"] = item["boundary"]
        entry["repo_paths"] = tracked_config_paths(item, tracked)
        entry["required_repo_paths"] = [item["path"]]
        entry["output_exists"] = True
        if item["status"] == "generated_from_stored_outputs":
            entry["validation_method"] = "stored-output figure generation; no scientific rerun required"
        else:
            entry["validation_method"] = "stored publication-package alias; no scientific rerun required"
        entry["notes"] = item["notes"]
    CONFIG_PATH.write_text(json.dumps(registry, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def update_crosswalk_docs() -> None:
    rows = [
        f"| `{item['paper_id']}` | {item['label'].replace('. ', ' - ', 1)} | `{item['path']}` | {item['boundary']} |"
        for item in FINAL_FIGURES
    ]
    chapter4 = (
        "## Chapter 4 Figures\n\n"
        "| Paper item | Final paper label | Reviewer-facing trace target | Claim boundary |\n"
        "| --- | --- | --- | --- |\n"
        + "\n".join(rows)
        + "\n\nStale mappings removed: Figure 4.1 is no longer generic study-box context, "
        "Figure 4.2 is no longer generic geography/domain reference, Figure 4.6 is the Mindoro comparator mean FSS "
        "summary, and Figure 4.13 is the legacy 2016 overall mean FSS chart.\n\n"
    )
    text = CROSSWALK_PATH.read_text(encoding="utf-8")
    start = text.index("## Chapter 4 Figures")
    end = text.index("## Reviewer-Facing Output Roots")
    CROSSWALK_PATH.write_text(text[:start] + chapter4 + text[end:], encoding="utf-8")

    text = PAPER_OUTPUT_MD_PATH.read_text(encoding="utf-8")
    lines = text.splitlines()
    out: list[str] = []
    inserted = False
    for line in lines:
        if line.startswith("| `Figure 4.") or line.startswith("| `Figure 4`"):
            if not inserted:
                for item in FINAL_FIGURES:
                    item_label = item["label"].split(". ", 1)[1]
                    item_code = item["label"].split(". ", 1)[0]
                    status = "Generated from stored outputs." if item["status"] == "generated_from_stored_outputs" else "Existing stored figure alias."
                    out.append(f"| `{item_code}` | {item_label}. | `{item['path']}` | {status} {item['boundary']} |")
                inserted = True
            continue
        out.append(line)
    PAPER_OUTPUT_MD_PATH.write_text("\n".join(out) + "\n", encoding="utf-8")


def main() -> None:
    final_rows = [csv_row(item) for item in FINAL_FIGURES]
    update_registry_and_manifest(final_rows)
    append_markdown_sections(final_rows)
    update_config_registry()
    update_crosswalk_docs()
    print(f"Aligned {len(final_rows)} final Figure 4 registry rows.")


if __name__ == "__main__":
    main()
