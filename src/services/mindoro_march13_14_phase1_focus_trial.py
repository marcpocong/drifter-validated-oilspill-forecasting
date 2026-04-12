"""Experimental Mindoro March 13 -> March 14 replay using a staged Phase 1 Mindoro-focused candidate baseline."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.case_context import get_case_context
from src.services.phase3b_extended_public_scored_march13_14_reinit import (
    MARCH13_14_REINIT_DIR_NAME,
    PHASE3B_REINIT_APPENDIX_ONLY_ENV,
    PHASE3B_REINIT_LAUNCHER_ENTRY_ID_OVERRIDE_ENV,
    PHASE3B_REINIT_OUTPUT_DIR_NAME_ENV,
    PHASE3B_REINIT_PRIMARY_PUBLIC_VALIDATION_ENV,
    PHASE3B_REINIT_REPORTING_ROLE_OVERRIDE_ENV,
    PHASE3B_REINIT_TRACK_ID_OVERRIDE_ENV,
    PHASE3B_REINIT_TRACK_LABEL_OVERRIDE_ENV,
    PHASE3B_REINIT_TRACK_OVERRIDE_ENV,
    run_phase3b_extended_public_scored_march13_14_reinit,
)
from src.utils.io import BASELINE_SELECTION_PATH_ENV, get_case_output_dir

TRIAL_BASELINE_SELECTION_PATH_ENV = "MINDORO_PHASE1_FOCUS_TRIAL_BASELINE_SELECTION_PATH"
TRIAL_OUTPUT_DIR_NAME_ENV = "MINDORO_PHASE1_FOCUS_TRIAL_OUTPUT_DIR_NAME"
TRIAL_OUTPUT_DIR_NAME_DEFAULT = "phase3b_extended_public_scored_march13_14_reinit_phase1_mindoro_focus_pre_spill"
TRIAL_TRACK_DEFAULT = "mindoro_phase3b_primary_public_validation_reinit_phase1_mindoro_focus_pre_spill_trial"
TRIAL_TRACK_ID_DEFAULT = "mindoro_b1_trial_phase1_mindoro_focus_pre_spill"
TRIAL_TRACK_LABEL_DEFAULT = "Mindoro March 13 -> March 14 experimental Phase 1 Mindoro-focus trial"
TRIAL_REPORTING_ROLE_DEFAULT = "experimental_phase1_mindoro_focus_trial"
TRIAL_LAUNCHER_ENTRY_ID = "mindoro_march13_14_phase1_focus_trial"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _relative(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


@contextmanager
def _temporary_env(overrides: dict[str, str]):
    previous = {key: os.environ.get(key) for key in overrides}
    try:
        for key, value in overrides.items():
            os.environ[key] = str(value)
        yield
    finally:
        for key, old_value in previous.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


class MindoroMarch1314Phase1FocusTrialService:
    def __init__(self, *, repo_root: str | Path | None = None):
        self.repo_root = Path(repo_root or Path(__file__).resolve().parents[2])
        self.case = get_case_context()
        if self.case.workflow_mode != "mindoro_retro_2023" or not self.case.is_official:
            raise RuntimeError(
                "mindoro_march13_14_phase1_focus_trial requires WORKFLOW_MODE=mindoro_retro_2023."
            )

        self.candidate_baseline_path = self.repo_root / Path(
            os.environ.get(
                TRIAL_BASELINE_SELECTION_PATH_ENV,
                "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_baseline_selection_candidate.yaml",
            )
        )
        self.output_dir_name = str(
            os.environ.get(TRIAL_OUTPUT_DIR_NAME_ENV, TRIAL_OUTPUT_DIR_NAME_DEFAULT)
        ).strip() or TRIAL_OUTPUT_DIR_NAME_DEFAULT
        self.case_output_dir = get_case_output_dir(self.case.run_name)
        self.canonical_dir = self.case_output_dir / MARCH13_14_REINIT_DIR_NAME
        self.output_dir = self.case_output_dir / self.output_dir_name
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.paths = {
            "comparison_csv": self.output_dir / "march13_14_reinit_phase1_focus_trial_comparison.csv",
            "report_md": self.output_dir / "march13_14_reinit_phase1_focus_trial_report.md",
            "manifest_json": self.output_dir / "march13_14_reinit_phase1_focus_trial_manifest.json",
        }

    @staticmethod
    def _load_summary(path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"Expected March 13 -> March 14 summary not found: {path}")
        return pd.read_csv(path)

    @staticmethod
    def _as_float(value: Any) -> float:
        numeric = pd.to_numeric(value, errors="coerce")
        return 0.0 if pd.isna(numeric) else float(numeric)

    @staticmethod
    def _pick_best_branch(summary_df: pd.DataFrame) -> dict[str, Any]:
        sortable = summary_df.copy()
        for column in ("mean_fss", "fss_1km", "branch_precedence", "iou", "dice", "nearest_distance_to_obs_m"):
            if column in sortable.columns:
                sortable[column] = pd.to_numeric(sortable[column], errors="coerce")
        if "branch_precedence" not in sortable.columns:
            sortable["branch_precedence"] = range(1, len(sortable) + 1)
        ordered = sortable.sort_values(
            ["mean_fss", "fss_1km", "iou", "dice", "nearest_distance_to_obs_m", "branch_precedence"],
            ascending=[False, False, False, False, True, True],
        )
        return ordered.iloc[0].to_dict()

    @staticmethod
    def _mean_fss_delta(experimental_row: dict[str, Any], canonical_row: dict[str, Any]) -> float:
        return MindoroMarch1314Phase1FocusTrialService._as_float(
            experimental_row.get("mean_fss")
        ) - MindoroMarch1314Phase1FocusTrialService._as_float(canonical_row.get("mean_fss"))

    def _comparison_verdict(self, experimental_row: dict[str, Any], canonical_row: dict[str, Any]) -> str:
        delta = self._mean_fss_delta(experimental_row, canonical_row)
        if abs(delta) < 1e-12:
            return "tied"
        return "improved" if delta > 0 else "underperformed"

    def _build_comparison_table(
        self,
        *,
        canonical_summary: pd.DataFrame,
        experimental_summary: pd.DataFrame,
        canonical_recipe: str,
        experimental_recipe: str,
    ) -> pd.DataFrame:
        canonical_best = self._pick_best_branch(canonical_summary)
        experimental_best = self._pick_best_branch(experimental_summary)
        rows = []
        for lane_name, recipe, row in (
            ("canonical", canonical_recipe, canonical_best),
            ("experimental_phase1_mindoro_focus_trial", experimental_recipe, experimental_best),
        ):
            rows.append(
                {
                    "lane": lane_name,
                    "recipe": recipe,
                    "best_branch": row.get("branch_id", ""),
                    "mean_fss": row.get("mean_fss", ""),
                    "fss_1km": row.get("fss_1km", ""),
                    "fss_3km": row.get("fss_3km", ""),
                    "fss_5km": row.get("fss_5km", ""),
                    "fss_10km": row.get("fss_10km", ""),
                    "iou": row.get("iou", ""),
                    "dice": row.get("dice", ""),
                    "nearest_distance_to_obs_m": row.get("nearest_distance_to_obs_m", ""),
                    "forecast_nonzero_cells": row.get("forecast_nonzero_cells", ""),
                    "empty_forecast_reason": row.get("empty_forecast_reason", ""),
                }
            )
        comparison_df = pd.DataFrame(rows)
        comparison_df["comparison_verdict_vs_canonical"] = ""
        comparison_df.loc[
            comparison_df["lane"] == "experimental_phase1_mindoro_focus_trial",
            "comparison_verdict_vs_canonical",
        ] = self._comparison_verdict(experimental_best, canonical_best)
        comparison_df["mean_fss_delta_vs_canonical"] = ""
        comparison_df.loc[
            comparison_df["lane"] == "experimental_phase1_mindoro_focus_trial",
            "mean_fss_delta_vs_canonical",
        ] = self._mean_fss_delta(experimental_best, canonical_best)
        return comparison_df

    def _write_report(
        self,
        *,
        comparison_df: pd.DataFrame,
        canonical_recipe: str,
        experimental_recipe: str,
        canonical_summary_path: Path,
        experimental_summary_path: Path,
    ) -> Path:
        experimental_row = comparison_df.loc[
            comparison_df["lane"] == "experimental_phase1_mindoro_focus_trial"
        ].iloc[0]
        canonical_row = comparison_df.loc[comparison_df["lane"] == "canonical"].iloc[0]
        lines = [
            "# Mindoro March 13 -> March 14 Phase 1 Focus Trial",
            "",
            "- Status: experimental and non-canonical",
            f"- Candidate baseline path: `{_relative(self.repo_root, self.candidate_baseline_path)}`",
            f"- Experimental output directory: `{_relative(self.repo_root, self.output_dir)}`",
            f"- Canonical stored B1 directory: `{_relative(self.repo_root, self.canonical_dir)}`",
            f"- Canonical recipe used: `{canonical_recipe}`",
            f"- Experimental recipe used: `{experimental_recipe}`",
            f"- Canonical best branch: `{canonical_row['best_branch']}`",
            f"- Experimental best branch: `{experimental_row['best_branch']}`",
            f"- Comparison verdict: `{experimental_row['comparison_verdict_vs_canonical'] or 'unavailable'}`",
            f"- Mean FSS delta vs canonical: `{experimental_row['mean_fss_delta_vs_canonical']}`",
            "",
            "## Best-row comparison",
            "",
            "```csv",
            comparison_df.to_csv(index=False).strip(),
            "```",
            "",
            "## Guardrails",
            "",
            "- This trial does not overwrite `config/phase1_baseline_selection.yaml`.",
            "- This trial does not modify legacy 2016 prototype outputs.",
            "- This trial does not replace the canonical March 13 -> March 14 B1 outputs.",
            "- PyGNOME is not rerun here and remains comparator-only.",
            "",
            "## Source artifacts",
            "",
            f"- Canonical summary: `{_relative(self.repo_root, canonical_summary_path)}`",
            f"- Experimental summary: `{_relative(self.repo_root, experimental_summary_path)}`",
        ]
        self.paths["report_md"].write_text("\n".join(lines) + "\n", encoding="utf-8")
        return self.paths["report_md"]

    def run(self) -> dict[str, Any]:
        if not self.candidate_baseline_path.exists():
            raise FileNotFoundError(
                "Experimental Phase 1 Mindoro-focus candidate baseline not found: "
                f"{self.candidate_baseline_path}"
            )

        trial_env = {
            BASELINE_SELECTION_PATH_ENV: str(self.candidate_baseline_path),
            PHASE3B_REINIT_OUTPUT_DIR_NAME_ENV: self.output_dir_name,
            PHASE3B_REINIT_TRACK_OVERRIDE_ENV: TRIAL_TRACK_DEFAULT,
            PHASE3B_REINIT_TRACK_ID_OVERRIDE_ENV: TRIAL_TRACK_ID_DEFAULT,
            PHASE3B_REINIT_TRACK_LABEL_OVERRIDE_ENV: TRIAL_TRACK_LABEL_DEFAULT,
            PHASE3B_REINIT_REPORTING_ROLE_OVERRIDE_ENV: TRIAL_REPORTING_ROLE_DEFAULT,
            PHASE3B_REINIT_APPENDIX_ONLY_ENV: "false",
            PHASE3B_REINIT_PRIMARY_PUBLIC_VALIDATION_ENV: "false",
            PHASE3B_REINIT_LAUNCHER_ENTRY_ID_OVERRIDE_ENV: TRIAL_LAUNCHER_ENTRY_ID,
        }

        with _temporary_env(trial_env):
            trial_results = run_phase3b_extended_public_scored_march13_14_reinit()

        canonical_summary_path = self.canonical_dir / "march13_14_reinit_summary.csv"
        canonical_manifest_path = self.canonical_dir / "march13_14_reinit_run_manifest.json"
        experimental_summary_path = Path(str(trial_results["summary_csv"]))
        experimental_manifest_path = Path(str(trial_results["run_manifest_json"]))

        canonical_summary = self._load_summary(canonical_summary_path)
        experimental_summary = self._load_summary(experimental_summary_path)
        canonical_manifest = json.loads(canonical_manifest_path.read_text(encoding="utf-8"))
        experimental_manifest = json.loads(experimental_manifest_path.read_text(encoding="utf-8"))
        canonical_recipe = str(((canonical_manifest.get("recipe") or {}).get("recipe")) or "")
        experimental_recipe = str(((experimental_manifest.get("recipe") or {}).get("recipe")) or "")

        comparison_df = self._build_comparison_table(
            canonical_summary=canonical_summary,
            experimental_summary=experimental_summary,
            canonical_recipe=canonical_recipe,
            experimental_recipe=experimental_recipe,
        )
        comparison_df.to_csv(self.paths["comparison_csv"], index=False)
        report_path = self._write_report(
            comparison_df=comparison_df,
            canonical_recipe=canonical_recipe,
            experimental_recipe=experimental_recipe,
            canonical_summary_path=canonical_summary_path,
            experimental_summary_path=experimental_summary_path,
        )

        manifest = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "phase": "mindoro_march13_14_phase1_focus_trial",
            "workflow_mode": self.case.workflow_mode,
            "status": "completed",
            "non_canonical": True,
            "candidate_baseline_path": _relative(self.repo_root, self.candidate_baseline_path),
            "canonical_reference_dir": _relative(self.repo_root, self.canonical_dir),
            "experimental_output_dir": _relative(self.repo_root, self.output_dir),
            "comparison_verdict": str(
                comparison_df.loc[
                    comparison_df["lane"] == "experimental_phase1_mindoro_focus_trial",
                    "comparison_verdict_vs_canonical",
                ].iloc[0]
            ),
            "artifacts": {
                "comparison_csv": _relative(self.repo_root, self.paths["comparison_csv"]),
                "report_md": _relative(self.repo_root, report_path),
                "trial_run_manifest_json": _relative(self.repo_root, experimental_manifest_path),
                "trial_summary_csv": _relative(self.repo_root, experimental_summary_path),
                "canonical_run_manifest_json": _relative(self.repo_root, canonical_manifest_path),
                "canonical_summary_csv": _relative(self.repo_root, canonical_summary_path),
            },
        }
        _write_json(self.paths["manifest_json"], manifest)
        return {
            "output_dir": str(self.output_dir),
            "comparison_csv": str(self.paths["comparison_csv"]),
            "report_md": str(report_path),
            "manifest_json": str(self.paths["manifest_json"]),
            "canonical_recipe": canonical_recipe,
            "experimental_recipe": experimental_recipe,
            "comparison_verdict": manifest["comparison_verdict"],
            "trial_summary_csv": str(experimental_summary_path),
        }


def run_mindoro_march13_14_phase1_focus_trial() -> dict[str, Any]:
    return MindoroMarch1314Phase1FocusTrialService().run()
