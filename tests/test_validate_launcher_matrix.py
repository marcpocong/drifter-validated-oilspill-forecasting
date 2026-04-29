from __future__ import annotations

import json
from pathlib import Path

from src.utils import validate_launcher_matrix as validator


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_current_launcher_matrix_validation_passes_without_docker_or_science():
    report = validator.audit_launcher_matrix(REPO_ROOT)

    assert report["status"] == validator.PASS
    assert report["global_issues"] == []
    assert report["summary"]["visible_entry_count"] == 21
    assert all(entry["status"] == validator.PASS for entry in report["entries"])
    assert "pipeline" in report["compose_services"]
    assert "gnome" in report["compose_services"]


def test_cli_no_write_reports_pass_without_creating_report_files(capsys):
    return_code = validator.main(["--repo-root", str(REPO_ROOT), "--no-write"])
    output = capsys.readouterr().out

    assert return_code == 0
    assert "Launcher matrix validation" in output
    assert "OVERALL: PASS" in output
    assert "Audit report written" not in output


def test_validation_detects_missing_handlers_services_docs_and_read_only_reruns(tmp_path):
    _write(
        tmp_path / "src" / "__main__.py",
        "\n".join(
            [
                "phase = ''",
                "if phase == 'phase5_launcher_and_docs_sync':",
                "    pass",
                "elif phase == 'phase1_production_rerun':",
                "    pass",
            ]
        )
        + "\n",
    )
    _write(
        tmp_path / "docker-compose.yml",
        "\n".join(
            [
                "services:",
                "  pipeline:",
                "    image: test",
            ]
        )
        + "\n",
    )
    _write(
        tmp_path / "config" / "settings.yaml",
        "\n".join(
            [
                "workflow_mode: mindoro_retro_2023",
                "workflow_case_files:",
                "  mindoro_retro_2023: config/case_mindoro_retro_2023.yaml",
            ]
        )
        + "\n",
    )
    _write(tmp_path / "config" / "case_mindoro_retro_2023.yaml", "case_id: test\n")
    _write(
        tmp_path / "docs" / "COMMAND_MATRIX.md",
        "\n".join(
            [
                "# Command Matrix",
                "",
                "## Launcher Entry Map",
                "",
                "| Entry ID | Thesis role | Run kind | Recommended for | Interactive command | Prompt-free phase mapping |",
                "| --- | --- | --- | --- | --- | --- |",
                "| `good_read_only` | read-only governance | `packaging_only` | auditor | `x` | `pipeline: phase5_launcher_and_docs_sync` |",
            ]
        )
        + "\n",
    )
    matrix = {
        "catalog_version": "test",
        "entrypoint": "start.ps1",
        "categories": [
            {"category_id": "read_only_packaging_help_utilities", "label": "Read only", "description": "Stored output tools"},
        ],
        "entries": [
            {
                "entry_id": "good_read_only",
                "menu_order": 1,
                "category_id": "read_only_packaging_help_utilities",
                "workflow_mode": "mindoro_retro_2023",
                "label": "Good read-only",
                "description": "Read-only stored outputs package.",
                "rerun_cost": "cheap_read_only",
                "safe_default": True,
                "thesis_role": "read_only_governance",
                "draft_section": "Evidence 8 / governance",
                "claim_boundary": "Read-only stored outputs package; it does not recompute science.",
                "run_kind": "packaging_only",
                "recommended_for": "auditor",
                "confirms_before_run": False,
                "steps": [
                    {
                        "phase": "phase5_launcher_and_docs_sync",
                        "service": "pipeline",
                        "description": "sync",
                    }
                ],
            },
            {
                "entry_id": "bad_read_only",
                "menu_order": 2,
                "category_id": "read_only_packaging_help_utilities",
                "workflow_mode": "missing_mode",
                "label": "Bad read-only",
                "description": "Read-only label with a rerun phase.",
                "rerun_cost": "cheap_read_only",
                "safe_default": True,
                "thesis_role": "read_only_governance",
                "draft_section": "Evidence 8 / governance",
                "claim_boundary": "Read-only stored outputs package; it does not recompute science.",
                "run_kind": "read_only",
                "recommended_for": "auditor",
                "confirms_before_run": False,
                "steps": [
                    {
                        "phase": "phase1_production_rerun",
                        "service": "missing_service",
                        "description": "should not be here",
                    },
                    {
                        "phase": "missing_handler",
                        "service": "pipeline",
                        "description": "missing handler",
                    },
                ],
            },
        ],
    }
    _write(tmp_path / "config" / "launcher_matrix.json", json.dumps(matrix, indent=2))

    report = validator.audit_launcher_matrix(tmp_path)
    bad_entry = next(entry for entry in report["entries"] if entry["entry_id"] == "bad_read_only")
    combined_issues = "\n".join([*report["global_issues"], *bad_entry["issues"]])

    assert report["status"] == validator.FAIL
    assert "workflow_mode 'missing_mode'" in combined_issues
    assert "phase 'missing_handler' has no explicit Python dispatcher handler" in combined_issues
    assert "service 'missing_service' is not defined in compose services" in combined_issues
    assert "read-only/package entry calls non-read-only phase(s)" in combined_issues
    assert "visible entry is missing from docs/COMMAND_MATRIX.md" in combined_issues
