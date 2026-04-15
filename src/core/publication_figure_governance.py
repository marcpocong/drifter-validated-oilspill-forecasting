"""Shared governance metadata for curated publication figures."""

from __future__ import annotations

from typing import Any, Mapping

from src.core.study_box_catalog import study_box_figure_metadata


def _contains(text: str, *tokens: str) -> bool:
    lowered = str(text or "").lower()
    return any(str(token).lower() in lowered for token in tokens)


def publication_figure_governance_columns(record: Mapping[str, Any]) -> dict[str, Any]:
    figure_id = str(record.get("figure_id") or "").strip().lower()
    figure_family_code = str(record.get("figure_family_code") or "").strip().upper()
    status_key = str(record.get("status_key") or "").strip()
    surface_key = str(record.get("surface_key") or "").strip()
    view_type = str(record.get("view_type") or "").strip().lower()

    thesis_surface = False
    archive_only = surface_key == "archive_only"
    legacy_support = surface_key == "legacy_support" or status_key in {"prototype_2016_support", "prototype_2021_support"}
    comparator_support = surface_key == "comparator_support" or status_key in {
        "mindoro_crossmodel_comparator",
        "dwh_crossmodel_comparator",
    }
    display_order = 9999
    page_target = "artifacts_logs"
    study_box_id = ""
    study_box_numbers = ""
    study_box_label = ""
    recommended_scope = "appendix_support"

    if status_key == "mindoro_primary_validation":
        page_target = "mindoro_validation_archive" if _contains(figure_id, "r0_overlay") else "mindoro_validation"
        if _contains(figure_id, "r0_overlay"):
            archive_only = True
            thesis_surface = False
            display_order = 910
            recommended_scope = "archive_only"
        elif _contains(figure_id, "mindoro_primary_validation_board"):
            thesis_surface = True
            display_order = 100
            recommended_scope = "main_text"
        elif _contains(figure_id, "march14_r1_previous_overlay"):
            thesis_surface = True
            display_order = 110
            recommended_scope = "page_support"
        elif _contains(figure_id, "march13_seed_vs_march14_target"):
            thesis_surface = True
            display_order = 120
            recommended_scope = "page_support"
        elif _contains(figure_id, "march14_target_mask_on_grid"):
            thesis_surface = True
            display_order = 130
            recommended_scope = "page_support"
        elif _contains(figure_id, "march13_seed_mask_on_grid"):
            thesis_surface = True
            display_order = 140
            recommended_scope = "page_support"

    elif status_key == "mindoro_crossmodel_comparator":
        page_target = "mindoro_validation_archive" if _contains(figure_id, "r0_") else "cross_model_comparison"
        comparator_support = True
        if _contains(figure_id, "r0_"):
            archive_only = True
            thesis_surface = False
            display_order = 920
            recommended_scope = "archive_only"
        elif _contains(figure_id, "mindoro_crossmodel_board"):
            thesis_surface = True
            display_order = 200
            recommended_scope = "page_support"
        elif _contains(figure_id, "march14_crossmodel_r1_overlay"):
            thesis_surface = True
            display_order = 210
            recommended_scope = "page_support"
        elif _contains(figure_id, "march14_crossmodel_pygnome_overlay"):
            thesis_surface = True
            display_order = 220
            recommended_scope = "page_support"
        elif _contains(figure_id, "mindoro_observed_masks_ensemble_pygnome_board"):
            thesis_surface = True
            display_order = 230
            recommended_scope = "page_support"
        elif _contains(figure_id, "mindoro_observed_masks_ensemble_pygnome_overlay"):
            thesis_surface = False
            display_order = 231
            recommended_scope = "appendix_support"

    elif status_key in {"mindoro_b1_r0_archive", "mindoro_legacy_march6", "mindoro_legacy_support"}:
        archive_only = True
        thesis_surface = False
        page_target = "mindoro_validation_archive"
        recommended_scope = "archive_only"
        if status_key == "mindoro_b1_r0_archive":
            display_order = 930
        elif status_key == "mindoro_legacy_march6":
            display_order = 940
        else:
            display_order = 950

    elif status_key == "mindoro_phase4_oil_budget":
        page_target = "phase4_oiltype_and_shoreline"
        if _contains(figure_id, "oil_budget_board"):
            thesis_surface = True
            display_order = 400
            recommended_scope = "page_support"
        elif _contains(figure_id, "oil_budget_summary"):
            thesis_surface = True
            display_order = 410
            recommended_scope = "page_support"
        else:
            thesis_surface = False
            display_order = 440
            recommended_scope = "appendix_support"

    elif status_key == "mindoro_phase4_shoreline":
        page_target = "phase4_oiltype_and_shoreline"
        if _contains(figure_id, "shoreline_impact_board"):
            thesis_surface = True
            display_order = 420
            recommended_scope = "page_support"
        elif _contains(figure_id, "shoreline_arrival"):
            thesis_surface = True
            display_order = 430
            recommended_scope = "page_support"
        elif _contains(figure_id, "shoreline_segment_impacts"):
            thesis_surface = True
            display_order = 432
            recommended_scope = "page_support"
        else:
            thesis_surface = False
            display_order = 445
            recommended_scope = "appendix_support"

    elif status_key == "mindoro_phase4_deferred":
        page_target = "phase4_crossmodel_status"
        thesis_surface = True
        display_order = 435
        recommended_scope = "page_support"

    elif status_key == "dwh_observation_truth_context":
        page_target = "dwh_transfer_validation"
        if _contains(figure_id, "2010_05_21_to_2010_05_23", "eventcorridor"):
            thesis_surface = True
            display_order = 300
            recommended_scope = "page_support"
        else:
            thesis_surface = False
            display_order = 340
            recommended_scope = "appendix_support"

    elif status_key == "dwh_deterministic_transfer":
        page_target = "dwh_transfer_validation"
        if _contains(figure_id, "2010_05_21_to_2010_05_23", "deterministic_footprint_overlay"):
            thesis_surface = True
            display_order = 305
            recommended_scope = "page_support"
        elif _contains(figure_id, "daily_deterministic_footprint_overview_board"):
            thesis_surface = True
            display_order = 310
            recommended_scope = "page_support"
        else:
            thesis_surface = False
            display_order = 341
            recommended_scope = "appendix_support"

    elif status_key == "dwh_ensemble_transfer":
        page_target = "dwh_transfer_validation"
        if _contains(figure_id, "2010_05_21_to_2010_05_23", "observed_deterministic_mask_p50_mask_p90_board"):
            thesis_surface = True
            display_order = 320
            recommended_scope = "page_support"
        elif _contains(figure_id, "2010_05_21_to_2010_05_23", "mask_p50_overlay"):
            thesis_surface = True
            display_order = 321
            recommended_scope = "page_support"
        else:
            thesis_surface = False
            display_order = 360
            recommended_scope = "appendix_support"

    elif status_key == "dwh_crossmodel_comparator":
        page_target = "dwh_transfer_validation"
        comparator_support = True
        if _contains(figure_id, "2010_05_21_to_2010_05_23", "observed_deterministic_mask_p50_pygnome_board"):
            thesis_surface = True
            display_order = 330
            recommended_scope = "page_support"
        elif _contains(figure_id, "2010_05_21_to_2010_05_23", "pygnome_footprint_overlay"):
            thesis_surface = True
            display_order = 331
            recommended_scope = "page_support"
        else:
            thesis_surface = False
            display_order = 370
            recommended_scope = "appendix_support"

    elif status_key == "dwh_trajectory_context":
        page_target = "dwh_transfer_validation"
        thesis_surface = False
        display_order = 380
        recommended_scope = "appendix_support"

    elif status_key == "thesis_study_box_reference":
        page_target = "phase1_recipe_selection"
        if _contains(figure_id, "thesis_study_boxes_reference_archive_full_context"):
            archive_only = True
            study_box_id = "thesis_study_boxes_archive_full_context"
            display_order = 960
            recommended_scope = "archive_only"
        elif _contains(figure_id, "focused_phase1_box_geography_reference"):
            archive_only = True
            study_box_id = "focused_phase1_validation_box"
            display_order = 970
            recommended_scope = "archive_only"
        elif _contains(figure_id, "scoring_grid_bounds_geography_reference"):
            archive_only = True
            study_box_id = "scoring_grid_display_bounds"
            display_order = 980
            recommended_scope = "archive_only"
        elif _contains(figure_id, "mindoro_case_domain_geography_reference"):
            thesis_surface = True
            study_box_id = "mindoro_case_domain"
            display_order = 510
            recommended_scope = "page_support"
        elif _contains(figure_id, "prototype_first_code_search_box_geography_reference"):
            thesis_surface = True
            study_box_id = "prototype_first_code_search_box"
            display_order = 520
            recommended_scope = "page_support"
        elif _contains(figure_id, "thesis_study_boxes_reference"):
            thesis_surface = True
            study_box_id = "thesis_study_boxes_reference"
            display_order = 500
            recommended_scope = "main_text"

    elif legacy_support:
        page_target = "legacy_2016_support"
        if figure_family_code == "M" and view_type == "board":
            thesis_surface = True
            recommended_scope = "legacy_support"
            if _contains(figure_id, "legacy_2016_drifter_track_triptych_board"):
                display_order = 600
            elif _contains(figure_id, "legacy_2016_drifter_vs_mask_p50_mask_p90_triptych_board"):
                display_order = 610
            elif _contains(figure_id, "legacy_2016_mask_p50_mask_p90_vs_pygnome_triptych_board"):
                display_order = 620
            else:
                display_order = 690
        else:
            thesis_surface = False
            display_order = 695
            recommended_scope = "appendix_support"

    study_box_metadata = study_box_figure_metadata(study_box_id)
    study_box_numbers = str(study_box_metadata["study_box_numbers"])
    study_box_label = str(study_box_metadata["study_box_label"])

    return {
        "thesis_surface": thesis_surface,
        "archive_only": archive_only,
        "legacy_support": legacy_support,
        "comparator_support": comparator_support,
        "display_order": int(display_order),
        "page_target": page_target,
        "study_box_id": study_box_id,
        "study_box_numbers": study_box_numbers,
        "study_box_label": study_box_label,
        "recommended_scope": recommended_scope,
    }
