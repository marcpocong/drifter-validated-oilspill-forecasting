from __future__ import annotations

import pytest

from launcher_ps_helpers import assert_clean_launcher_exit, assert_no_docker_execution, run_launcher


B1_ENTRY_ID = "mindoro_phase3b_primary_public_validation"
B1_BOUNDARY = "Only main Philippine public-observation validation claim"


@pytest.mark.parametrize(
    ("args", "expected_text"),
    [
        (
            ["-Help", "-NoPause"],
            [
                "LAUNCHER HELP",
                B1_ENTRY_ID,
                "B1 is the only main Philippine public-observation validation claim",
                "shared-imagery caveat",
            ],
        ),
        (
            ["-List", "-NoPause"],
            [
                "CURRENT LAUNCHER CATALOG",
                "Mindoro B1 primary public-validation rerun",
                B1_BOUNDARY,
                "shared-imagery caveat applies",
            ],
        ),
        (
            ["-ListRole", "primary_evidence", "-NoPause"],
            [
                "Filtered thesis role: Primary evidence",
                "Mindoro B1 primary public-validation rerun",
                B1_BOUNDARY,
                "shared-imagery caveat applies",
            ],
        ),
        (
            ["-Explain", B1_ENTRY_ID, "-NoPause"],
            [
                "ENTRY PREVIEW",
                f"Entry ID: {B1_ENTRY_ID}",
                f"Claim boundary: {B1_BOUNDARY}",
                "shared-imagery caveat applies",
            ],
        ),
    ],
)
def test_help_list_role_and_explain_smoke_without_docker(tmp_path, args, expected_text):
    result = run_launcher(args, tmp_path=tmp_path)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    for text in expected_text:
        assert text in result.output


def test_direct_entry_dry_run_prints_plan_without_workflow_execution(tmp_path):
    result = run_launcher(["-Entry", B1_ENTRY_ID, "-DryRun", "-NoPause"], tmp_path=tmp_path)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert f"Entry ID: {B1_ENTRY_ID}" in result.output
    assert "Steps that would run:" in result.output
    assert "Exact commands that would run:" in result.output
    assert B1_BOUNDARY in result.output
    assert "Dry run only. No Docker commands were executed" in result.output


def test_direct_entry_blank_confirmation_cancels_cleanly_without_docker(tmp_path):
    result = run_launcher(["-Entry", B1_ENTRY_ID], tmp_path=tmp_path, stdin="\n\n")

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "Cancelled. No workflow was executed." in result.output


def test_interactive_invalid_choice_then_quit_is_clean(tmp_path):
    result = run_launcher(["-NoPause"], tmp_path=tmp_path, stdin="not-a-menu-choice\nQ\n")

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "Invalid option 'not-a-menu-choice'. Allowed options:" in result.output
    assert "Goodbye." in result.output


def test_interactive_role_group_back_returns_to_launcher_home_then_quits(tmp_path):
    result = run_launcher(["-NoPause"], tmp_path=tmp_path, stdin="1\nB\nQ\n")

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "Main thesis evidence / reportable" in result.output
    assert result.output.count("Choose a role-based path:") >= 2
    assert "Goodbye." in result.output


def test_interactive_entry_cancel_returns_to_menu_then_quits_without_docker(tmp_path):
    result = run_launcher(
        ["-NoPause"],
        tmp_path=tmp_path,
        stdin=f"1\n{B1_ENTRY_ID}\nC\nQ\n",
    )

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert f"Entry ID: {B1_ENTRY_ID}" in result.output
    assert "Cancelled. No workflow was executed." in result.output
    assert result.output.count("Main thesis evidence / reportable") >= 2
    assert "Goodbye." in result.output
