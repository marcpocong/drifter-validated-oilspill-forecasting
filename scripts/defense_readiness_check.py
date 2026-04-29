from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT_TEXT = str(REPO_ROOT)
if REPO_ROOT_TEXT not in sys.path:
    sys.path.insert(0, REPO_ROOT_TEXT)

from scripts.defense_readiness_common import (
    ALLOWED_WRITE_ROOTS,
    DEFAULT_PIPELINE_IMPORT_MODULES,
    ENV_EXAMPLE_PATH,
    ENV_PATH,
    FAIL,
    GNOME_IMPORT_MODULES,
    OUTPUT_DIR,
    PAGE_IMPORT_MODULES,
    PANEL_PS_PATH,
    PANEL_REVIEW_JSON_PATH,
    PANEL_REVIEW_MANIFEST_PATH,
    PASS,
    START_PS_PATH,
    WARN,
    CheckResult,
    artifact_findings,
    b1_r0_findings,
    base_report_payload,
    build_streamlit_runtime_check,
    check_result_payload,
    command_passed,
    command_text,
    compose_command,
    detect_compose_mode,
    diff_snapshots,
    docker_service_names,
    documentation_findings,
    file_tree_snapshot,
    git_status_short,
    host_package_status,
    launcher_entry_map,
    manuscript_number_findings,
    page_registry_findings,
    phase1_value_findings,
    read_panel_review_payload,
    relpath,
    repo_relative_command_text,
    report_topic_status,
    run_command,
    run_gnome_import_check,
    run_pipeline_import_check,
    start_ps_command,
    summarize_checks,
    support_value_warnings,
    utc_now_iso,
    validate_launcher_matrix,
    write_report,
)


REPORT_JSON_PATH = OUTPUT_DIR / "defense_readiness_report.json"
REPORT_MD_PATH = OUTPUT_DIR / "defense_readiness_report.md"


def add_check(
    checks: list[CheckResult],
    *,
    check_id: str,
    topic: str,
    status: str,
    summary: str,
    details: list[str] | None = None,
    command: str = "",
    output_snippet: str = "",
    data: dict | None = None,
) -> None:
    checks.append(
        CheckResult(
            check_id=check_id,
            topic=topic,
            status=status,
            summary=summary,
            details=details or [],
            command=command,
            output_snippet=output_snippet,
            data=data or {},
        )
    )


def build_env() -> dict[str, str]:
    env = {
        "DEFENSE_SMOKE_TEST": "1",
        "INPUT_CACHE_POLICY": "reuse_if_valid",
    }
    if os.environ.get("FORCING_OUTAGE_POLICY", "").strip().lower() == "continue_degraded":
        env["FORCING_OUTAGE_POLICY"] = ""
    return env


def normalize_git_status_path(line: str) -> str:
    path_text = line[3:].strip()
    if path_text.startswith('"') and path_text.endswith('"'):
        path_text = path_text[1:-1]
    return path_text.replace("\\", "/")


def run_smoke_command(
    checks: list[CheckResult],
    commands_run: list[str],
    *,
    check_id: str,
    topic: str,
    command: list[str],
    timeout: int,
    env: dict[str, str],
    required_substrings: tuple[str, ...] = (),
    allow_nonzero: bool = False,
    success_summary: str = "",
    failure_summary: str = "",
) -> None:
    result = run_command(command, timeout=timeout, env=env)
    commands_run.append(result.command)
    if result.timed_out:
        add_check(
            checks,
            check_id=check_id,
            topic=topic,
            status=FAIL,
            summary=failure_summary or f"{check_id} timed out",
            command=result.command,
            output_snippet=result.combined_output,
            details=["command timed out before it returned"],
        )
        return

    missing = [item for item in required_substrings if item not in result.combined_output]
    success = (allow_nonzero or result.returncode == 0) and not missing
    status = PASS if success else FAIL
    details = []
    if result.returncode != 0:
        details.append(f"return code: {result.returncode}")
    if missing:
        details.append("missing output markers: " + ", ".join(missing))
    add_check(
        checks,
        check_id=check_id,
        topic=topic,
        status=status,
        summary=success_summary or f"{check_id} completed",
        details=details,
        command=result.command,
        output_snippet=result.combined_output,
    )


def run_panel_review_generation(
    checks: list[CheckResult],
    commands_run: list[str],
    *,
    compose_mode: dict[str, str],
) -> None:
    command = compose_command(compose_mode, "exec", "-T", "pipeline", "python", "src/services/panel_review_check.py")
    result = run_command(command, timeout=240, env=build_env())
    commands_run.append(result.command)
    if command_passed(result) and PANEL_REVIEW_JSON_PATH.exists() and PANEL_REVIEW_MANIFEST_PATH.exists():
        add_check(
            checks,
            check_id="panel_review_generation",
            topic="manuscript_numbers",
            status=PASS,
            summary="panel review check regenerated from stored outputs only",
            command=result.command,
            output_snippet=result.combined_output,
        )
        return

    add_check(
        checks,
        check_id="panel_review_generation",
        topic="manuscript_numbers",
        status=FAIL,
        summary="panel review check could not be regenerated read-only",
        command=result.command,
        output_snippet=result.combined_output,
        details=["expected output/panel_review_check/*.json and manifest after the run"],
    )


def run_safe_entry_batch(
    checks: list[CheckResult],
    commands_run: list[str],
    *,
    compose_mode: dict[str, str],
    quick: bool,
) -> tuple[list[str], list[str]]:
    del compose_mode
    pre_snapshot = file_tree_snapshot(tuple(sorted(ALLOWED_WRITE_ROOTS)))
    pre_git = git_status_short()
    scientific_roots = (
        "config",
        "data_processed/grids",
        "output/CASE_MINDORO_RETRO_2023",
        "output/CASE_DWH_RETRO_2010_72H",
        "output/phase1_mindoro_focus_pre_spill_2016_2023",
    )
    scientific_before = file_tree_snapshot(scientific_roots)

    safe_entries = ["b1_drifter_context_panel"]
    if not quick:
        safe_entries.extend(
            [
                "final_validation_package",
                "phase5_sync",
                "figure_package_publication",
                "trajectory_gallery_panel",
            ]
        )

    env = build_env()
    for entry_id in safe_entries:
        result = run_command(start_ps_command("-Entry", entry_id, "-NoPause"), timeout=900, env=env)
        commands_run.append(result.command)
        if result.returncode == 0 and not result.timed_out:
            add_check(
                checks,
                check_id=f"entry_{entry_id}",
                topic="launcher_entries",
                status=PASS,
                summary=f"{entry_id} completed without triggering a scientific rerun",
                command=result.command,
                output_snippet=result.combined_output,
            )
        else:
            add_check(
                checks,
                check_id=f"entry_{entry_id}",
                topic="launcher_entries",
                status=FAIL,
                summary=f"{entry_id} failed during read-only smoke",
                command=result.command,
                output_snippet=result.combined_output,
                details=[f"return code: {result.returncode}", "expected a prompt-free read-only/package-only completion"],
            )

    post_snapshot = file_tree_snapshot(tuple(sorted(ALLOWED_WRITE_ROOTS)))
    scientific_after = file_tree_snapshot(scientific_roots)
    changed_allowed = diff_snapshots(pre_snapshot, post_snapshot)
    changed_scientific = diff_snapshots(scientific_before, scientific_after)

    if changed_scientific:
        add_check(
            checks,
            check_id="no_science_guard",
            topic="no_science_guard",
            status=FAIL,
            summary="scientific or config roots changed during read-only smoke",
            details=changed_scientific[:80],
        )
    else:
        add_check(
            checks,
            check_id="no_science_guard",
            topic="no_science_guard",
            status=PASS,
            summary="scientific and config roots stayed unchanged during read-only smoke",
        )

    post_git = git_status_short()
    git_delta = [line for line in post_git if line not in pre_git]
    unexpected_git_delta = []
    for line in git_delta:
        normalized = normalize_git_status_path(line)
        if not any(
            normalized == root or normalized.startswith(root.rstrip("/") + "/")
            for root in ALLOWED_WRITE_ROOTS
        ):
            unexpected_git_delta.append(line)
    if unexpected_git_delta:
        add_check(
            checks,
            check_id="git_delta_after_safe_entries",
            topic="working_tree",
            status=WARN,
            summary="read-only smoke introduced new working-tree changes",
            details=unexpected_git_delta,
        )
    else:
        add_check(
            checks,
            check_id="git_delta_after_safe_entries",
            topic="working_tree",
            status=PASS,
            summary="read-only smoke only touched documented packaging, report, and log paths",
        )

    return changed_allowed, changed_scientific


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Final-defense readiness checker")
    parser.add_argument("--mode", choices=("local", "docker"), default="local")
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--require-docker", action="store_true")
    parser.add_argument("--require-dashboard", action="store_true")
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args(argv)

    checks: list[CheckResult] = []
    commands_run: list[str] = []
    compose_mode = detect_compose_mode()
    changed_allowed: list[str] = []
    changed_scientific: list[str] = []
    report = base_report_payload(
        mode_name=args.mode,
        quick=args.quick,
        require_docker=args.require_docker,
        require_dashboard=args.require_dashboard,
    )

    if not START_PS_PATH.exists():
        raise SystemExit(f"Missing launcher entrypoint: {START_PS_PATH}")

    matrix_issues = validate_launcher_matrix()
    add_check(
        checks,
        check_id="launcher_matrix_schema",
        topic="launcher_matrix",
        status=PASS if not matrix_issues else FAIL,
        summary="launcher matrix schema and thesis-boundary rules checked",
        details=matrix_issues,
    )

    doc_issues = documentation_findings()
    add_check(
        checks,
        check_id="claim_boundary_docs",
        topic="claim_boundaries",
        status=PASS if not doc_issues else FAIL,
        summary="defense-facing docs scanned for claim-boundary wording and readability",
        details=doc_issues,
    )

    artifact_issues, artifact_matches = artifact_findings()
    add_check(
        checks,
        check_id="artifact_registry",
        topic="artifacts",
        status=PASS if not artifact_issues else FAIL,
        summary="panel-facing stored artifacts and manifests discovered",
        details=artifact_issues or [f"{key}: {value[0]}" for key, value in artifact_matches.items() if value],
        data={"matches": artifact_matches},
    )

    phase1_issues = phase1_value_findings()
    b1_r0_issues = b1_r0_findings()
    add_check(
        checks,
        check_id="phase1_and_b1_values",
        topic="stored_values",
        status=PASS if not phase1_issues and not b1_r0_issues else FAIL,
        summary="Phase 1 selection values and B1 R0/B1 guardrail values verified from stored files",
        details=phase1_issues + b1_r0_issues,
    )

    support_warnings = support_value_warnings()
    add_check(
        checks,
        check_id="phase4_support_values",
        topic="stored_values",
        status=WARN if support_warnings else PASS,
        summary="support/context Phase 4 machine-readable values reviewed",
        details=support_warnings,
    )

    page_issues = page_registry_findings()
    add_check(
        checks,
        check_id="dashboard_page_registry",
        topic="dashboard",
        status=PASS if not page_issues else FAIL,
        summary="dashboard page registry and B1 drifter provenance page boundaries checked from source",
        details=page_issues,
    )

    host_imports = host_package_status()
    missing_host = [f"{name}: {status}" for name, status in host_imports.items() if status != PASS]
    add_check(
        checks,
        check_id="host_python_packages",
        topic="package_imports",
        status=WARN if missing_host else PASS,
        summary="host Python package availability checked",
        details=missing_host or ["host Python has the expected dashboard/test packages"],
    )

    env = build_env()
    run_smoke_command(
        checks,
        commands_run,
        check_id="launcher_help",
        topic="launcher_commands",
        command=start_ps_command("-Help", "-NoPause"),
        timeout=120,
        env=env,
        required_substrings=("Panel-safe default path", "Guardrails:"),
        success_summary="start.ps1 -Help -NoPause returned readable launcher help",
        failure_summary="start.ps1 -Help -NoPause failed",
    )
    run_smoke_command(
        checks,
        commands_run,
        check_id="launcher_list",
        topic="launcher_commands",
        command=start_ps_command("-List", "-NoPause"),
        timeout=120,
        env=env,
        required_substrings=("CURRENT LAUNCHER CATALOG", "b1_drifter_context_panel"),
        success_summary="start.ps1 -List -NoPause returned the launcher catalog",
        failure_summary="start.ps1 -List -NoPause failed",
    )
    run_smoke_command(
        checks,
        commands_run,
        check_id="launcher_list_primary",
        topic="launcher_commands",
        command=start_ps_command("-ListRole", "primary_evidence", "-NoPause"),
        timeout=120,
        env=env,
        required_substrings=("Primary evidence", "mindoro_phase3b_primary_public_validation"),
        success_summary="primary_evidence role listing returned readable output",
    )
    run_smoke_command(
        checks,
        commands_run,
        check_id="launcher_list_read_only_governance",
        topic="launcher_commands",
        command=start_ps_command("-ListRole", "read_only_governance", "-NoPause"),
        timeout=120,
        env=env,
        required_substrings=("Read-only governance", "b1_drifter_context_panel", "figure_package_publication"),
        success_summary="read_only_governance role listing returned readable output",
    )

    explain_targets = {
        "mindoro_phase3b_primary_public_validation": ("Claim boundary:", "Run kind:", "Rerun cost:", "Output warning:"),
        "phase1_mindoro_focus_provenance": ("Claim boundary:", "Run kind:", "Rerun cost:", "Output warning:"),
        "dwh_reportable_bundle": ("Claim boundary:", "Run kind:", "Rerun cost:", "Output warning:"),
        "b1_drifter_context_panel": ("Claim boundary:", "Run kind:", "Rerun cost:", "Output warning:"),
    }
    for entry_id, required_tokens in explain_targets.items():
        run_smoke_command(
            checks,
            commands_run,
            check_id=f"explain_{entry_id}",
            topic="launcher_commands",
            command=start_ps_command("-Explain", entry_id, "-NoPause"),
            timeout=120,
            env=env,
            required_substrings=required_tokens,
            success_summary=f"-Explain {entry_id} returned the thesis-boundary preview",
        )

    panel_preview = run_command(start_ps_command("-Panel", "-NoPause"), timeout=120, env=env)
    commands_run.append(panel_preview.command)
    panel_missing = [
        token
        for token in ("PANEL REVIEW MODE", "Open read-only dashboard", "View B1 drifter provenance/context")
        if token not in panel_preview.combined_output
    ]
    add_check(
        checks,
        check_id="panel_no_pause",
        topic="panel_commands",
        status=PASS if command_passed(panel_preview) and not panel_missing else FAIL,
        summary="start.ps1 -Panel -NoPause returned a noninteractive panel summary",
        details=(["missing output markers: " + ", ".join(panel_missing)] if panel_missing else []),
        command=panel_preview.command,
        output_snippet=panel_preview.combined_output,
    )

    panel_wrapper = run_command(
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
        env=env,
    )
    commands_run.append(panel_wrapper.command)
    panel_wrapper_missing = [
        token
        for token in (
            "PANEL REVIEW MODE",
            "Use .\\panel.ps1 or .\\start.ps1 -Panel for the interactive defense menu.",
        )
        if token not in panel_wrapper.combined_output
    ]
    add_check(
        checks,
        check_id="panel_wrapper_no_pause",
        topic="panel_commands",
        status=PASS if command_passed(panel_wrapper) and not panel_wrapper_missing else FAIL,
        summary="panel.ps1 -NoPause returned the read-only panel wrapper preview",
        details=(["missing output markers: " + ", ".join(panel_wrapper_missing)] if panel_wrapper_missing else []),
        command=panel_wrapper.command,
        output_snippet=panel_wrapper.combined_output,
    )

    expensive_entry = run_command(start_ps_command("-Entry", "phase1_mindoro_focus_provenance"), timeout=120, env=env)
    commands_run.append(expensive_entry.command)
    add_check(
        checks,
        check_id="expensive_entry_confirmation_guard",
        topic="launcher_safety",
        status=PASS
        if expensive_entry.returncode != 0 and "requires interactive confirmation" in expensive_entry.combined_output
        else FAIL,
        summary="noninteractive execution of an expensive launcher entry stopped at the confirmation guard",
        command=expensive_entry.command,
        output_snippet=expensive_entry.combined_output,
    )

    if not compose_mode:
        add_check(
            checks,
            check_id="docker_compose_detection",
            topic="docker",
            status=FAIL if args.require_docker else WARN,
            summary="Docker Compose was not detected on this host",
            details=["checked both 'docker compose' and 'docker-compose'"],
        )
    else:
        add_check(
            checks,
            check_id="docker_compose_detection",
            topic="docker",
            status=PASS,
            summary=f"Docker Compose detected via {compose_mode['command']}",
        )
        config_result = run_command(compose_command(compose_mode, "config"), timeout=180, env=env)
        commands_run.append(config_result.command)
        add_check(
            checks,
            check_id="docker_compose_config",
            topic="docker",
            status=PASS if command_passed(config_result) else FAIL,
            summary="docker compose config completed",
            command=config_result.command,
            output_snippet=config_result.combined_output,
        )

        services = docker_service_names()
        expected_services = {"pipeline", "gnome"}
        missing_services = sorted(expected_services - set(services))
        add_check(
            checks,
            check_id="docker_compose_services",
            topic="docker",
            status=PASS if not missing_services else FAIL,
            summary="docker-compose.yml service list checked",
            details=missing_services or services,
        )

        if args.mode == "docker":
            up_result = run_command(compose_command(compose_mode, "up", "-d"), timeout=300, env=env)
            commands_run.append(up_result.command)
            add_check(
                checks,
                check_id="docker_compose_up",
                topic="docker",
                status=PASS if command_passed(up_result) else FAIL,
                summary="docker compose up -d completed",
                command=up_result.command,
                output_snippet=up_result.combined_output,
            )
            ps_result = run_command(compose_command(compose_mode, "ps"), timeout=120, env=env)
            commands_run.append(ps_result.command)
            add_check(
                checks,
                check_id="docker_compose_ps",
                topic="docker",
                status=PASS if command_passed(ps_result) else FAIL,
                summary="docker compose ps completed",
                command=ps_result.command,
                output_snippet=ps_result.combined_output,
            )

        pipeline_ok, pipeline_command, pipeline_lines = run_pipeline_import_check(compose_mode, DEFAULT_PIPELINE_IMPORT_MODULES)
        commands_run.append(pipeline_command)
        add_check(
            checks,
            check_id="pipeline_package_imports",
            topic="package_imports",
            status=PASS if pipeline_ok else FAIL,
            summary="pipeline container package imports checked",
            command=pipeline_command,
            details=pipeline_lines,
        )

        gnome_ok, gnome_command, gnome_lines = run_gnome_import_check(compose_mode, GNOME_IMPORT_MODULES)
        commands_run.append(gnome_command)
        add_check(
            checks,
            check_id="gnome_package_imports",
            topic="package_imports",
            status=PASS if gnome_ok else WARN,
            summary="gnome container import check completed",
            command=gnome_command,
            details=gnome_lines,
        )

        import_probe = run_command(
            compose_command(
                compose_mode,
                "exec",
                "-T",
                "pipeline",
                "python",
                "-c",
                "\n".join(
                    [
                        "import importlib",
                        "mods = " + repr(["ui.app", "ui.data_access", *list(PAGE_IMPORT_MODULES)]),
                        "for name in mods:",
                        "    importlib.import_module(name)",
                        "print('IMPORT_OK')",
                    ]
                ),
            ),
            timeout=240,
            env=env,
        )
        commands_run.append(import_probe.command)
        add_check(
            checks,
            check_id="dashboard_container_imports",
            topic="dashboard",
            status=PASS if command_passed(import_probe) and "IMPORT_OK" in import_probe.combined_output else FAIL,
            summary="pipeline container imported ui/app.py and the panel-facing page modules",
            command=import_probe.command,
            output_snippet=import_probe.combined_output,
        )

        if args.require_dashboard:
            runtime_ok, runtime_command, runtime_details = build_streamlit_runtime_check(compose_mode)
            commands_run.append(runtime_command)
            add_check(
                checks,
                check_id="streamlit_runtime_smoke",
                topic="dashboard",
                status=PASS if runtime_ok else FAIL,
                summary="headless Streamlit runtime smoke passed" if runtime_ok else "headless Streamlit runtime smoke failed",
                command=runtime_command,
                details=[runtime_details],
            )
        else:
            add_check(
                checks,
                check_id="streamlit_runtime_smoke",
                topic="dashboard",
                status=WARN,
                summary="Streamlit runtime smoke was not required for this run",
                details=["rerun with --require-dashboard to make the headless UI health check mandatory"],
            )

        if not args.quick:
            run_panel_review_generation(checks, commands_run, compose_mode=compose_mode)
            changed_allowed, changed_scientific = run_safe_entry_batch(
                checks,
                commands_run,
                compose_mode=compose_mode,
                quick=args.quick,
            )
        else:
            changed_allowed = []
            changed_scientific = []
            add_check(
                checks,
                check_id="safe_entry_batch",
                topic="launcher_entries",
                status=WARN,
                summary="quick mode skipped the longer read-only entry batch",
                details=["rerun without --quick to exercise final_validation_package, phase5_sync, figure_package_publication, and trajectory_gallery_panel"],
            )
    if not compose_mode and args.require_dashboard:
        add_check(
            checks,
            check_id="streamlit_runtime_smoke",
            topic="dashboard",
            status=FAIL,
            summary="dashboard runtime was required but Docker Compose is unavailable",
        )
        changed_allowed = []
        changed_scientific = []

    manuscript_issues = manuscript_number_findings()
    add_check(
        checks,
        check_id="manuscript_numbers",
        topic="manuscript_numbers",
        status=PASS if not manuscript_issues else FAIL,
        summary="panel review JSON report checked for stored manuscript-number discrepancies",
        details=manuscript_issues,
    )

    if not ENV_EXAMPLE_PATH.exists():
        add_check(
            checks,
            check_id="env_example",
            topic="environment",
            status=FAIL,
            summary=".env.example is missing",
        )
    else:
        env_details = [".env.example exists"]
        if ENV_PATH.exists():
            env_details.append(".env already exists and was not overwritten")
        else:
            env_details.append(".env is absent; the readiness checker did not create or overwrite it")
        add_check(
            checks,
            check_id="env_example",
            topic="environment",
            status=PASS,
            summary="environment template check completed",
            details=env_details,
        )

    report["commands_run"] = [repo_relative_command_text(command) for command in commands_run]
    report["files_changed_by_readiness_test"] = sorted(dict.fromkeys(changed_allowed))
    report["checks"] = [check_result_payload(check) for check in checks]
    report["summary"] = summarize_checks(checks)
    report["status_by_topic"] = report_topic_status(checks)
    report["hard_failures"] = [check.summary for check in checks if check.status == FAIL]
    report["warnings"] = [check.summary for check in checks if check.status == WARN]
    report["generated_at_utc"] = utc_now_iso()
    report["git_status_after"] = git_status_short()

    if args.write_report:
        write_report(REPORT_JSON_PATH, REPORT_MD_PATH, report)

    return 0 if report["summary"]["fail_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
