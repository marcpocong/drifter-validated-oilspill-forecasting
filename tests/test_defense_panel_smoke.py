from __future__ import annotations

import os
import subprocess
import time

import pytest

from scripts.defense_readiness_common import (
    PANEL_PS_PATH,
    START_PS_PATH,
    compose_command,
    detect_compose_mode,
    diff_snapshots,
    file_tree_snapshot,
    run_command,
    start_ps_command,
)


SMOKE_ENV = {
    "DEFENSE_SMOKE_TEST": "1",
    "INPUT_CACHE_POLICY": "reuse_if_valid",
}
COMPOSE_MODE = detect_compose_mode()
REQUIRE_DOCKER = os.environ.get("DEFENSE_REQUIRE_DOCKER") == "1"


@pytest.mark.parametrize(
    ("args", "required_tokens"),
    [
        (("-Help", "-NoPause"), ("Panel-safe default path", "Guardrails:")),
        (("-List", "-NoPause"), ("CURRENT LAUNCHER CATALOG", "b1_drifter_context_panel")),
        (("-ListRole", "primary_evidence", "-NoPause"), ("Primary evidence", "mindoro_phase3b_primary_public_validation")),
        (("-ListRole", "read_only_governance", "-NoPause"), ("Read-only governance", "figure_package_publication")),
        (("-Explain", "mindoro_phase3b_primary_public_validation", "-NoPause"), ("Claim boundary:", "Run kind:", "Output warning:")),
        (("-Explain", "phase1_mindoro_focus_provenance", "-NoPause"), ("Claim boundary:", "Run kind:", "Output warning:")),
        (("-Explain", "dwh_reportable_bundle", "-NoPause"), ("Claim boundary:", "Run kind:", "Output warning:")),
        (("-Explain", "b1_drifter_context_panel", "-NoPause"), ("Claim boundary:", "Run kind:", "Output warning:")),
    ],
)
def test_launcher_smoke_commands_return_readable_output(args: tuple[str, ...], required_tokens: tuple[str, ...]):
    result = run_command(start_ps_command(*args), timeout=180, env=SMOKE_ENV)
    assert result.returncode == 0, result.combined_output
    for token in required_tokens:
        assert token in result.combined_output


def test_panel_no_pause_does_not_hang_or_prompt():
    result = run_command(start_ps_command("-Panel", "-NoPause"), timeout=120, env=SMOKE_ENV)
    assert result.returncode == 0, result.combined_output
    assert "PANEL REVIEW MODE" in result.combined_output
    assert "Non-interactive preview mode (-NoPause)." in result.combined_output
    assert "View B1 drifter provenance/context" in result.combined_output
    assert "View data sources and provenance registry" in result.combined_output


def test_panel_wrapper_no_pause_delegates_cleanly():
    result = run_command(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(PANEL_PS_PATH),
            "-NoPause",
        ],
        timeout=120,
        env=SMOKE_ENV,
    )
    assert result.returncode == 0, result.combined_output
    assert "PANEL REVIEW MODE" in result.combined_output
    assert "Use .\\panel.ps1 or .\\start.ps1 -Panel for the interactive defense menu." in result.combined_output


def run_interactive_launcher(input_text: str) -> tuple[int, str]:
    process = subprocess.Popen(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(START_PS_PATH),
        ],
        cwd=str(START_PS_PATH.parent),
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        for chunk in input_text.splitlines(keepends=True):
            assert process.stdin is not None
            process.stdin.write(chunk)
            process.stdin.flush()
            time.sleep(0.05)
        if process.stdin is not None:
            process.stdin.close()
        stdout, stderr = process.communicate(timeout=120)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
    combined_output = (stdout or "") + (stderr or "")
    return process.returncode, combined_output


def test_interactive_inspect_mode_accepts_visible_menu_numbers_and_stays_active():
    returncode, combined_output = run_interactive_launcher("1\nX\n3\n1\n\nQ\n")
    assert returncode == 0, combined_output
    assert "Inspect mode for Main thesis evidence / reportable" in combined_output
    assert combined_output.count("Inspect preview:") >= 2
    assert "Entry ID: dwh_reportable_bundle" in combined_output
    assert "Entry ID: phase1_mindoro_focus_provenance" in combined_output
    assert "Inspect mode remains active." in combined_output
    assert "Confirm [" not in combined_output
    assert "Starting Docker containers..." not in combined_output


def test_interactive_inspect_mode_invalid_reference_can_recover_to_valid_preview():
    returncode, combined_output = run_interactive_launcher("1\nX\n999\n3\n\nQ\n")
    assert returncode == 0, combined_output
    assert "Invalid inspect option '999'." in combined_output
    assert "[ERROR]" not in combined_output
    assert "Inspect preview:" in combined_output
    assert "Entry ID: dwh_reportable_bundle" in combined_output


def test_interactive_inspect_mode_compact_preview_omits_long_detail_by_default():
    returncode, combined_output = run_interactive_launcher("1\nX\n2\n\nQ\n")
    assert returncode == 0, combined_output
    assert "Inspect preview:" in combined_output
    assert "Entry ID: mindoro_phase3b_primary_public_validation" in combined_output
    assert "Step summary:" in combined_output
    assert "Output warning:" not in combined_output
    assert "Steps that would run:" not in combined_output
    assert "Notes:" not in combined_output


def test_interactive_inspect_mode_more_shows_full_detail_and_returns_to_inspect():
    returncode, combined_output = run_interactive_launcher("1\nX\n2\nM\n3\n\nQ\n")
    assert returncode == 0, combined_output
    assert "Inspect preview:" in combined_output
    assert "Full preview:" in combined_output
    assert "Output warning:" in combined_output
    assert "Steps that would run:" in combined_output
    assert "Notes:" in combined_output
    assert combined_output.count("Inspect mode remains active.") >= 2
    assert "Confirm [" not in combined_output
    assert "Starting Docker containers..." not in combined_output


def test_interactive_inspect_mode_alias_resolves_to_canonical_preview():
    returncode, combined_output = run_interactive_launcher("1\nX\nphase1_mindoro_focus_pre_spill_experiment\n\nQ\n")
    assert returncode == 0, combined_output
    assert "Inspect preview:" in combined_output
    assert "Entry ID: phase1_mindoro_focus_provenance" in combined_output
    assert "Requested alias: phase1_mindoro_focus_pre_spill_experiment" in combined_output


@pytest.mark.parametrize("back_input", ["\n", "B\n", "0\n"])
def test_interactive_inspect_mode_back_shortcuts_return_to_section_menu(back_input: str):
    returncode, combined_output = run_interactive_launcher(f"1\nX\n{back_input}Q\n")
    assert returncode == 0, combined_output
    assert "Inspect mode for Main thesis evidence / reportable" in combined_output
    assert combined_output.count("Main thesis evidence / reportable") >= 2
    assert "Goodbye." in combined_output


def test_interactive_inspect_mode_cancel_returns_to_section_with_notice():
    returncode, combined_output = run_interactive_launcher("1\nX\nC\nQ\n")
    assert returncode == 0, combined_output
    assert "Cancelled. No workflow was executed." in combined_output
    assert combined_output.count("Main thesis evidence / reportable") >= 2
    assert "[ERROR]" not in combined_output


def test_interactive_inspect_mode_supports_quit():
    returncode, combined_output = run_interactive_launcher("1\nX\nQ\n")
    assert returncode == 0, combined_output
    assert "Inspect mode for Main thesis evidence / reportable" in combined_output
    assert "No workflow will run from this prompt." in combined_output
    assert "Goodbye." in combined_output


def test_expensive_entry_stops_at_noninteractive_confirmation_guard():
    result = run_command(start_ps_command("-Entry", "phase1_mindoro_focus_provenance"), timeout=120, env=SMOKE_ENV)
    assert result.returncode != 0
    assert "requires interactive confirmation" in result.combined_output


def test_dry_run_resolves_entry_without_docker():
    result = run_command(
        start_ps_command("-Entry", "mindoro_phase3b_primary_public_validation", "-DryRun", "-NoPause"),
        timeout=180,
        env=SMOKE_ENV,
    )
    assert result.returncode == 0, result.combined_output
    assert "Dry run only. No Docker commands were executed" in result.combined_output
    assert "phase3b_extended_public" in result.combined_output
    assert "phase3b_extended_public_scored_march13_14_reinit" in result.combined_output
    assert "exec -T" in result.combined_output
    assert "Starting Docker containers..." not in result.combined_output


def test_direct_entry_confirmation_enter_cancels_cleanly_without_error():
    process = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(START_PS_PATH),
            "-Entry",
            "mindoro_phase3b_primary_public_validation",
        ],
        cwd=str(START_PS_PATH.parent),
        input="\n",
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    combined_output = (process.stdout or "") + (process.stderr or "")
    assert process.returncode == 0, combined_output
    assert "Cancelled. No workflow was executed." in combined_output
    assert "[ERROR]" not in combined_output
    assert "Starting Docker containers..." not in combined_output


def test_interactive_home_cancel_stays_clean_and_returns_to_launcher_home():
    process = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(START_PS_PATH),
        ],
        cwd=str(START_PS_PATH.parent),
        input="ZZZ\nC\nQ\n",
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    combined_output = (process.stdout or "") + (process.stderr or "")
    assert process.returncode == 0, combined_output
    assert "Invalid option 'ZZZ'." in combined_output
    assert "Cancelled. No workflow was executed." in combined_output
    assert combined_output.count("Choose a role-based path:") >= 2
    assert "[ERROR]" not in combined_output


def test_panel_option_list_keeps_b1_context_option_visible():
    result = run_command(start_ps_command("-Panel", "-NoPause"), timeout=120, env=SMOKE_ENV)
    assert result.returncode == 0, result.combined_output
    assert "7. View B1 drifter provenance/context" in result.combined_output
    assert "8. View data sources and provenance registry" in result.combined_output


def test_read_only_ui_launcher_reuses_existing_dashboard_process():
    start_text = START_PS_PATH.read_text(encoding="utf-8")

    assert "function Test-ReadOnlyUiHealth" in start_text
    assert "Read-only Streamlit UI is already running." in start_text
    assert "Found a stale Streamlit process in the pipeline container" in start_text
    assert "Use R / RESTART from panel mode" in start_text


def test_streamlit_runtime_probe_cleans_up_container_process_after_check():
    common_text = (START_PS_PATH.parent / "scripts" / "defense_readiness_common.py").read_text(encoding="utf-8")

    assert common_text.count("run_command(cleanup_command, timeout=30)") >= 2
    assert "process.terminate()" in common_text


@pytest.mark.skipif(not REQUIRE_DOCKER or not COMPOSE_MODE, reason="requires Docker-backed smoke run")
def test_read_only_entries_do_not_mutate_scientific_roots():
    scientific_roots = (
        "config",
        "data_processed/grids",
        "output/CASE_MINDORO_RETRO_2023",
        "output/CASE_DWH_RETRO_2010_72H",
        "output/phase1_mindoro_focus_pre_spill_2016_2023",
    )
    before = file_tree_snapshot(scientific_roots)

    for entry_id in (
        "b1_drifter_context_panel",
        "final_validation_package",
        "phase5_sync",
        "figure_package_publication",
        "trajectory_gallery_panel",
    ):
        result = run_command(start_ps_command("-Entry", entry_id, "-NoPause"), timeout=900, env=SMOKE_ENV)
        assert result.returncode == 0, result.combined_output

    after = file_tree_snapshot(scientific_roots)
    assert diff_snapshots(before, after) == []


@pytest.mark.skipif(not REQUIRE_DOCKER or not COMPOSE_MODE, reason="requires Docker-backed smoke run")
def test_panel_review_check_is_regeneratable_read_only():
    command = compose_command(COMPOSE_MODE, "exec", "-T", "pipeline", "python", "src/services/panel_review_check.py")
    result = run_command(command, timeout=240, env=SMOKE_ENV)
    assert result.returncode == 0, result.combined_output
    assert "Panel review verification complete." in result.combined_output
