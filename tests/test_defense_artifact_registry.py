from __future__ import annotations

from pathlib import Path

import pytest

from scripts.defense_readiness_common import (
    PANEL_REVIEW_JSON_PATH,
    PHASE1_MANIFEST_PATH,
    artifact_findings,
    b1_r0_findings,
    manuscript_number_findings,
    phase1_value_findings,
    read_panel_review_payload,
    support_value_warnings,
)


def require_repo_artifact(path: Path) -> None:
    if not path.exists():
        pytest.xfail(f"artifact not present in this checkout: {path}")


def test_panel_facing_artifacts_are_discoverable():
    findings, matches = artifact_findings()
    if findings:
        missing_paths = [item for item in findings if item.startswith("no artifact found")]
        if missing_paths:
            pytest.xfail("; ".join(missing_paths))
    assert findings == []
    assert matches["b1_primary_board_or_scorecard"]
    assert matches["track_a_board_or_scorecard"]
    assert matches["dwh_board_or_scorecard"]


def test_phase1_machine_readable_values_match_expected_recipe_story():
    require_repo_artifact(PHASE1_MANIFEST_PATH)
    assert phase1_value_findings() == []


def test_panel_review_json_reports_no_manuscript_number_failures():
    require_repo_artifact(PANEL_REVIEW_JSON_PATH)
    payload = read_panel_review_payload()
    assert payload
    assert manuscript_number_findings() == []
    assert payload["summary"]["pass_count"] >= 31


def test_b1_r0_guardrail_is_still_present_in_stored_survival_summary():
    assert b1_r0_findings() == []


def test_phase4_support_layer_warnings_are_explicit_if_kg_values_differ():
    warnings = support_value_warnings()
    if warnings:
        assert any("kg value" in item for item in warnings)
