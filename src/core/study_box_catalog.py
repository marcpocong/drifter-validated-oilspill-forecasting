"""Canonical study-box numbering and presentation metadata."""

from __future__ import annotations

from typing import Any

THESIS_FACING_STUDY_BOX_NUMBERS: tuple[str, ...] = ("2", "4")
ARCHIVE_ONLY_STUDY_BOX_NUMBERS: tuple[str, ...] = ("1", "3")

_STUDY_BOX_REFERENCE_METADATA: dict[str, dict[str, Any]] = {
    "focused_phase1_validation_box": {
        "study_box_id": "focused_phase1_validation_box",
        "study_box_numbers": "1",
        "box_label": "Focused Mindoro Phase 1 validation box",
        "study_box_label": "Study Box 1 - Focused Mindoro Phase 1 validation box",
        "thesis_surface_allowed": False,
    },
    "mindoro_case_domain": {
        "study_box_id": "mindoro_case_domain",
        "study_box_numbers": "2",
        "box_label": "Mindoro case-domain overview box (`mindoro_case_domain`)",
        "study_box_label": "Study Box 2 - Mindoro case-domain geography reference",
        "thesis_surface_allowed": True,
    },
    "scoring_grid_display_bounds": {
        "study_box_id": "scoring_grid_display_bounds",
        "study_box_numbers": "3",
        "box_label": "Mindoro scoring-grid display bounds",
        "study_box_label": "Study Box 3 - Mindoro scoring-grid bounds geography reference",
        "thesis_surface_allowed": False,
    },
    "prototype_first_code_search_box": {
        "study_box_id": "prototype_first_code_search_box",
        "study_box_numbers": "4",
        "box_label": "prototype_2016 first-code search box (historical-origin)",
        "study_box_label": "Study Box 4 - prototype_2016 first-code search-box geography reference",
        "thesis_surface_allowed": True,
    },
    "thesis_study_boxes_reference": {
        "study_box_id": "thesis_study_boxes_reference",
        "study_box_numbers": "2,4",
        "box_label": "Study Boxes 2 and 4 thesis-facing overview",
        "study_box_label": "Study Boxes 2 and 4 - Thesis study boxes reference figure",
        "thesis_surface_allowed": True,
    },
    "thesis_study_boxes_archive_full_context": {
        "study_box_id": "thesis_study_boxes_archive_full_context",
        "study_box_numbers": "1,2,3,4",
        "box_label": "Study Boxes 1-4 archived full-context overview",
        "study_box_label": "Study Boxes 1-4 - Archived full study-box reference figure",
        "thesis_surface_allowed": False,
    },
}


def parse_study_box_numbers(value: str | None) -> tuple[str, ...]:
    return tuple(token.strip() for token in str(value or "").split(",") if token.strip())


def study_box_figure_metadata(study_box_id: str | None) -> dict[str, Any]:
    normalized_id = str(study_box_id or "").strip()
    metadata = dict(_STUDY_BOX_REFERENCE_METADATA.get(normalized_id) or {})
    return {
        "study_box_id": normalized_id,
        "study_box_numbers": str(metadata.get("study_box_numbers") or ""),
        "box_label": str(metadata.get("box_label") or ""),
        "study_box_label": str(metadata.get("study_box_label") or ""),
        "thesis_surface_allowed": bool(metadata.get("thesis_surface_allowed", False)),
    }


def study_box_catalog_rows() -> list[dict[str, Any]]:
    return [study_box_figure_metadata(study_box_id) for study_box_id in _STUDY_BOX_REFERENCE_METADATA]
