"""Deterministic budget-only Phase 4 PyGNOME comparator pilot for prototype_2016."""

from __future__ import annotations

import json
import logging
import textwrap
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from src.services.figure_package_publication import (
    STYLE_CONFIG_PATH,
    load_publication_style_config,
    resolve_publication_typography,
)
from src.services.gnome_comparison import GNOME_AVAILABLE, GnomeComparisonService
from src.utils.io import get_forcing_files, load_drifter_data, resolve_recipe_selection, select_drifter_of_record

matplotlib.use("Agg")

PHASE = "prototype_legacy_phase4_pygnome_comparator"
CASE_IDS = ["CASE_2016-09-01", "CASE_2016-09-06", "CASE_2016-09-17"]
OUTPUT_SUBDIR = "phase4_pygnome_comparator"
SNAPSHOT_HOURS = (24, 48, 72)
SUPPORTED_SCENARIOS = ("light", "heavy")
COMPARABLE_COMPONENTS = (
    ("surface_pct", "surface"),
    ("evaporated_pct", "evaporated"),
    ("dispersed_pct", "dispersed"),
)
NONCOMPARABLE_COMPONENTS = (
    (
        "beached_pct",
        "beached",
        "PyGNOME shoreline/beaching is not mapped to the canonical prototype_2016 shoreline workflow in this pilot.",
    ),
)

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def _relative_to_repo(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = pd.DataFrame(rows)
    for column in columns:
        if column not in payload.columns:
            payload[column] = ""
    payload = payload[columns]
    payload.to_csv(path, index=False, lineterminator="\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _safe_float(value: Any) -> float:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return float("nan")
    return float(value)


class PrototypeLegacyPhase4PygnomeComparatorService:
    def __init__(
        self,
        repo_root: str | Path = ".",
        *,
        case_ids: list[str] | tuple[str, ...] | None = None,
    ):
        self.repo_root = Path(repo_root).resolve()
        self.case_ids = list(case_ids or CASE_IDS)
        self.gnome_service = GnomeComparisonService()
        self.style = load_publication_style_config(self.repo_root / STYLE_CONFIG_PATH)
        self.font_audit = resolve_publication_typography(self.style, self.repo_root)
        self.font_family = self.font_audit.actual_font_family

    def _case_output_dir(self, case_id: str) -> Path:
        return self.repo_root / "output" / case_id

    def _weathering_dir(self, case_id: str) -> Path:
        return self._case_output_dir(case_id) / "weathering"

    def _comparator_dir(self, case_id: str) -> Path:
        return self._case_output_dir(case_id) / OUTPUT_SUBDIR

    def _validation_ranking_csv(self, case_id: str) -> Path:
        return self._case_output_dir(case_id) / "validation" / "validation_ranking.csv"

    def _drifter_csv(self, case_id: str) -> Path:
        return self.repo_root / "data" / "drifters" / case_id / "drifters_noaa.csv"

    def _load_recipe_selection(self, case_id: str):
        return resolve_recipe_selection(
            ranking_csv=self._validation_ranking_csv(case_id),
            allow_fallback=False,
        )

    def _load_drifter_origin(self, case_id: str) -> dict[str, Any]:
        drifter_path = self._drifter_csv(case_id)
        if not drifter_path.exists():
            raise FileNotFoundError(f"Missing prototype_2016 drifter-of-record source: {drifter_path}")
        selection = select_drifter_of_record(load_drifter_data(drifter_path))
        return {
            "selected_id": str(selection.get("selected_id") or ""),
            "start_lat": float(selection["start_lat"]),
            "start_lon": float(selection["start_lon"]),
            "start_time": str(selection["start_time"]),
        }

    def _opendrift_budget_path(self, case_id: str, scenario_key: str) -> Path:
        return self._weathering_dir(case_id) / f"budget_{scenario_key}.csv"

    def _load_opendrift_budget(self, case_id: str, scenario_key: str) -> pd.DataFrame:
        path = self._opendrift_budget_path(case_id, scenario_key)
        if not path.exists():
            raise FileNotFoundError(f"Missing prototype_2016 Phase 4 OpenDrift/OpenOil budget CSV: {path}")
        df = pd.read_csv(path)
        if "hours_elapsed" not in df.columns:
            if "hour" in df.columns:
                df["hours_elapsed"] = df["hour"]
            else:
                raise ValueError(f"Budget CSV is missing hours_elapsed/hour column: {path}")
        df["hours_elapsed"] = pd.to_numeric(df["hours_elapsed"], errors="coerce").astype("Int64")
        required = {"surface_pct", "evaporated_pct", "dispersed_pct", "beached_pct"}
        missing = required.difference(df.columns)
        if missing:
            raise ValueError(f"Budget CSV is missing required percentage columns {sorted(missing)}: {path}")
        return df.dropna(subset=["hours_elapsed"]).copy().sort_values("hours_elapsed").reset_index(drop=True)

    def _align_budget_frames(self, openoil_df: pd.DataFrame, pygnome_df: pd.DataFrame) -> pd.DataFrame:
        left = openoil_df.copy()
        right = pygnome_df.copy()
        left["hours_elapsed"] = pd.to_numeric(left["hours_elapsed"], errors="coerce").astype("Int64")
        right["hours_elapsed"] = pd.to_numeric(right["hours_elapsed"], errors="coerce").astype("Int64")
        merged = left.merge(
            right,
            on="hours_elapsed",
            how="inner",
            suffixes=("_opendrift", "_pygnome"),
        )
        if merged.empty:
            raise RuntimeError("No overlapping hourly budget rows were available between OpenDrift/OpenOil and PyGNOME.")
        return merged.sort_values("hours_elapsed").reset_index(drop=True)

    def _load_existing_pygnome_budget_outputs(
        self,
        *,
        case_id: str,
        scenario_key: str,
    ) -> tuple[pd.DataFrame, Path, dict[str, Any]]:
        output_dir = self._comparator_dir(case_id)
        budget_csv = output_dir / f"pygnome_budget_{scenario_key}.csv"
        nc_path = output_dir / f"pygnome_{scenario_key}.nc"
        manifest_path = output_dir / "pygnome_phase4_run_manifest.json"
        if not budget_csv.exists() or not nc_path.exists() or not manifest_path.exists():
            missing = [str(path) for path in (budget_csv, nc_path, manifest_path) if not path.exists()]
            raise FileNotFoundError(
                "Read-only legacy Phase 4 board refresh requires existing stored PyGNOME comparator outputs. "
                f"Missing: {', '.join(missing)}"
            )
        budget_df = pd.read_csv(budget_csv)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) or {}
        scenario_manifest = dict((manifest.get("scenarios") or {}).get(scenario_key) or {})
        scenario_manifest.setdefault("status", "reused_existing")
        scenario_manifest.setdefault("relative_nc_path", _relative_to_repo(self.repo_root, nc_path))
        scenario_manifest.setdefault("relative_budget_csv_path", _relative_to_repo(self.repo_root, budget_csv))
        return budget_df, nc_path, scenario_manifest

    def _write_font_audit(self, output_dir: Path) -> None:
        _write_csv(
            output_dir / "font_audit.csv",
            [self.font_audit.as_row()],
            [
                "requested_font_family",
                "actual_font_family",
                "actual_font_path",
                "exact_requested_font_used",
                "fallback_used",
                "fallback_candidates",
            ],
        )

    def _write_board_layout_audit(self, output_dir: Path, rows: list[dict[str, Any]]) -> None:
        _write_csv(
            output_dir / "board_layout_audit.csv",
            rows,
            [
                "graphic_file",
                "board_family",
                "issue_types_found",
                "layout_fix_applied",
                "requested_font_family",
                "actual_font_resolved",
                "exact_arial_used",
                "fallback_needed",
                "text_shortened_or_wrapped",
                "filenames_stayed_same",
            ],
        )

    def _snapshot_rows(self, *, case_id: str, scenario_key: str, merged: pd.DataFrame) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for hour in SNAPSHOT_HOURS:
            subset = merged.loc[merged["hours_elapsed"].astype(int) == int(hour)]
            if subset.empty:
                raise RuntimeError(f"Missing matched hourly budget row at {hour} h for {case_id} {scenario_key}.")
            row = subset.iloc[0]
            for column, compartment in COMPARABLE_COMPONENTS:
                od_value = _safe_float(row[f"{column}_opendrift"])
                py_value = _safe_float(row[f"{column}_pygnome"])
                rows.append(
                    {
                        "case_id": case_id,
                        "scenario_key": scenario_key,
                        "hours_elapsed": int(hour),
                        "comparison_scope": "budget_snapshot",
                        "compartment": compartment,
                        "opendrift_pct": od_value,
                        "pygnome_pct": py_value,
                        "abs_percentage_point_diff": abs(od_value - py_value),
                        "comparable": True,
                        "comparable_reason": "Matched budget fraction from stored OpenDrift/OpenOil and deterministic PyGNOME pilot outputs.",
                    }
                )
            for column, compartment, reason in NONCOMPARABLE_COMPONENTS:
                rows.append(
                    {
                        "case_id": case_id,
                        "scenario_key": scenario_key,
                        "hours_elapsed": int(hour),
                        "comparison_scope": "budget_snapshot",
                        "compartment": compartment,
                        "opendrift_pct": _safe_float(row[f"{column}_opendrift"]),
                        "pygnome_pct": _safe_float(row[f"{column}_pygnome"]),
                        "abs_percentage_point_diff": "",
                        "comparable": False,
                        "comparable_reason": reason,
                    }
                )
        return rows

    def _time_series_metric_rows(self, *, case_id: str, scenario_key: str, merged: pd.DataFrame) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        end_hour = int(merged["hours_elapsed"].astype(int).max())
        end_subset = merged.loc[merged["hours_elapsed"].astype(int) == end_hour].iloc[0]
        for column, compartment in COMPARABLE_COMPONENTS:
            diffs = (
                pd.to_numeric(merged[f"{column}_opendrift"], errors="coerce")
                - pd.to_numeric(merged[f"{column}_pygnome"], errors="coerce")
            ).dropna()
            mae = float(np.abs(diffs).mean()) if not diffs.empty else float("nan")
            rmse = float(np.sqrt(np.square(diffs).mean())) if not diffs.empty else float("nan")
            rows.append(
                {
                    "case_id": case_id,
                    "scenario_key": scenario_key,
                    "compartment": compartment,
                    "comparable": True,
                    "hours_compared": int(len(diffs)),
                    "start_hour": int(merged["hours_elapsed"].astype(int).min()),
                    "end_hour": end_hour,
                    "mae_pct_points": mae,
                    "rmse_pct_points": rmse,
                    "end_horizon_opendrift_pct": _safe_float(end_subset[f"{column}_opendrift"]),
                    "end_horizon_pygnome_pct": _safe_float(end_subset[f"{column}_pygnome"]),
                    "end_horizon_abs_diff_pct_points": abs(
                        _safe_float(end_subset[f"{column}_opendrift"]) - _safe_float(end_subset[f"{column}_pygnome"])
                    ),
                    "notes": "Comparator-only descriptive metric. Not an observational skill metric.",
                }
            )
        for _, compartment, reason in NONCOMPARABLE_COMPONENTS:
            rows.append(
                {
                    "case_id": case_id,
                    "scenario_key": scenario_key,
                    "compartment": compartment,
                    "comparable": False,
                    "hours_compared": 0,
                    "start_hour": "",
                    "end_hour": end_hour,
                    "mae_pct_points": "",
                    "rmse_pct_points": "",
                    "end_horizon_opendrift_pct": "",
                    "end_horizon_pygnome_pct": "",
                    "end_horizon_abs_diff_pct_points": "",
                    "notes": reason,
                }
            )
        return rows

    def _overlay_line_style(self, scenario_key: str) -> str:
        return "-" if scenario_key == "light" else "--"

    def _plot_budget_time_series(
        self,
        *,
        case_id: str,
        scenario_key: str,
        scenario_label: str,
        openoil_df: pd.DataFrame,
        pygnome_df: pd.DataFrame,
        metrics_df: pd.DataFrame,
        output_path: Path,
    ) -> None:
        figure_facecolor = (self.style.get("layout") or {}).get("figure_facecolor") or "#ffffff"
        axes_facecolor = (self.style.get("layout") or {}).get("axes_facecolor") or "#f7fbfd"
        grid_color = (self.style.get("layout") or {}).get("grid_color") or "#cbd5e1"
        title_size = float((self.style.get("typography") or {}).get("title_size") or 16)
        panel_title_size = float((self.style.get("typography") or {}).get("panel_title_size") or 11)
        body_size = float((self.style.get("typography") or {}).get("body_size") or 9)
        note_size = float((self.style.get("typography") or {}).get("note_size") or 8)
        colors = {
            "surface_pct": "#165ba8",
            "evaporated_pct": "#f28c28",
            "dispersed_pct": "#2f855a",
            "beached_pct": "#8b5e3c",
        }
        labels = {
            "surface_pct": "Surface",
            "evaporated_pct": "Evaporated",
            "dispersed_pct": "Dispersed",
            "beached_pct": "Beached",
        }
        fig = plt.figure(figsize=(14.6, 9.2), dpi=220, facecolor=figure_facecolor)
        outer = fig.add_gridspec(3, 2, height_ratios=[1.0, 1.0, 0.42], left=0.06, right=0.98, top=0.90, bottom=0.06, hspace=0.30, wspace=0.22)
        axes = np.array(
            [
                [fig.add_subplot(outer[0, 0]), fig.add_subplot(outer[0, 1])],
                [fig.add_subplot(outer[1, 0]), fig.add_subplot(outer[1, 1])],
            ],
            dtype=object,
        )
        guide_ax = fig.add_subplot(outer[2, 0])
        metric_ax = fig.add_subplot(outer[2, 1])
        for ax in axes.flatten():
            ax.set_facecolor(axes_facecolor)
        hours_oo = pd.to_numeric(openoil_df["hours_elapsed"], errors="coerce")
        hours_py = pd.to_numeric(pygnome_df["hours_elapsed"], errors="coerce")
        for ax, column in zip(axes.flatten(), ("surface_pct", "evaporated_pct", "dispersed_pct", "beached_pct")):
            ax.plot(
                hours_oo,
                pd.to_numeric(openoil_df[column], errors="coerce"),
                color=colors[column],
                linewidth=2.5,
                linestyle="-",
                label="OpenDrift/OpenOil",
            )
            ax.plot(
                hours_py,
                pd.to_numeric(pygnome_df[column], errors="coerce"),
                color=colors[column],
                linewidth=2.0,
                linestyle="--",
                alpha=0.85,
                label="PyGNOME comparator",
            )
            ax.set_title(labels[column], fontsize=panel_title_size, fontweight="bold")
            ax.set_xlim(left=0)
            ax.set_ylim(bottom=0)
            ax.set_xlabel("Hours elapsed")
            ax.set_ylabel("Mass %")
            ax.grid(True, linestyle="--", color=grid_color, alpha=0.35)
            ax.axvline(x=72, color="#64748b", linestyle=":", linewidth=1.2)
            ax.legend(fontsize=body_size - 0.5, loc="best")

        comparable_metrics = metrics_df.loc[metrics_df["comparable"].astype(bool)].copy()
        guide_lines = [
            "Read the four panels as mass-fraction trajectories for the same stored scenario.",
            "OpenDrift/OpenOil is the stored baseline; PyGNOME stays comparator-only and support-only.",
            "Use the 72 h marker to compare end-state separation before reading the MAE/RMSE summary.",
            "The beached curve is shown for transparency only and is excluded from comparator metrics.",
        ]
        metric_lines = [
            f"Case: {case_id}",
            f"Scenario: {scenario_label}",
            "Scope: stored deterministic PyGNOME budget pilot with no shoreline comparison claim.",
        ]
        if not comparable_metrics.empty:
            metric_lines.extend(
                [
                    f"{row['compartment'].title()}: MAE {float(row['mae_pct_points']):.2f}, RMSE {float(row['rmse_pct_points']):.2f}"
                    for _, row in comparable_metrics.iterrows()
                ]
            )
        for ax, title, lines in ((guide_ax, "How to read this figure", guide_lines), (metric_ax, "Comparator Scope", metric_lines)):
            ax.axis("off")
            ax.text(0.0, 0.98, title, ha="left", va="top", fontsize=panel_title_size, fontweight="bold", color="#0f172a", transform=ax.transAxes)
            ax.text(
                0.0,
                0.72,
                "\n".join(textwrap.fill(f"- {line}" if ax is guide_ax else line, width=52, subsequent_indent="  " if ax is guide_ax else "") for line in lines),
                ha="left",
                va="top",
                fontsize=note_size,
                color="#334155",
                transform=ax.transAxes,
                bbox={"boxstyle": "round,pad=0.42", "facecolor": "#ffffff", "edgecolor": "#cbd5e1"},
            )
        fig.suptitle(
            f"{case_id.replace('CASE_', '')} Phase 4 budget comparator ({scenario_label})",
            fontsize=title_size,
            fontweight="bold",
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path)
        plt.close(fig)

    def _plot_budget_board(
        self,
        *,
        case_id: str,
        scenario_labels: dict[str, str],
        snapshot_df: pd.DataFrame,
        metrics_df: pd.DataFrame,
        output_path: Path,
    ) -> None:
        figure_facecolor = (self.style.get("layout") or {}).get("figure_facecolor") or "#ffffff"
        axes_facecolor = (self.style.get("layout") or {}).get("axes_facecolor") or "#f7fbfd"
        title_size = float((self.style.get("typography") or {}).get("title_size") or 16)
        panel_title_size = float((self.style.get("typography") or {}).get("panel_title_size") or 11)
        body_size = float((self.style.get("typography") or {}).get("body_size") or 9)

        fig = plt.figure(figsize=(15.4, 9.2), dpi=220, facecolor=figure_facecolor)
        grid = fig.add_gridspec(2, 3, height_ratios=[2.2, 0.95], width_ratios=[1.0, 1.0, 0.92], left=0.06, right=0.98, top=0.90, bottom=0.06, hspace=0.24, wspace=0.22)
        axes = {
            "light": fig.add_subplot(grid[0, 0]),
            "heavy": fig.add_subplot(grid[0, 1]),
            "guide": fig.add_subplot(grid[1, 0]),
            "scope": fig.add_subplot(grid[1, 1]),
            "metrics": fig.add_subplot(grid[:, 2]),
        }
        for key in ("light", "heavy"):
            axes[key].set_facecolor(axes_facecolor)
        colors = {"surface": "#165ba8", "evaporated": "#f28c28", "dispersed": "#2f855a"}
        snapshot = snapshot_df.loc[snapshot_df["comparable"].astype(bool)].copy()
        snapshot = snapshot.loc[snapshot["hours_elapsed"].astype(int) == 72].copy()
        for scenario_key in ("light", "heavy"):
            ax = axes[scenario_key]
            subset = snapshot.loc[snapshot["scenario_key"].astype(str) == scenario_key].copy()
            if subset.empty:
                ax.text(0.5, 0.5, "No matched 72 h budget rows", ha="center", va="center")
                ax.axis("off")
                continue
            subset["compartment"] = pd.Categorical(
                subset["compartment"],
                categories=[item[1] for item in COMPARABLE_COMPONENTS],
                ordered=True,
            )
            subset = subset.sort_values("compartment")
            x = np.arange(len(subset))
            ax.bar(
                x,
                subset["abs_percentage_point_diff"].astype(float),
                color=[colors[str(value)] for value in subset["compartment"].astype(str)],
                alpha=0.88,
            )
            ax.set_xticks(x)
            ax.set_xticklabels([str(value).title() for value in subset["compartment"].astype(str)])
            ax.set_ylabel("Abs. percentage-point difference")
            ax.set_title(
                f"{scenario_labels.get(scenario_key, scenario_key)} | 72 h snapshot",
                fontsize=panel_title_size,
                fontweight="bold",
            )
            ax.grid(True, axis="y", alpha=0.25)

        guide_lines = [
            "Read left to right: light oil 72 h snapshot, heavy oil 72 h snapshot, then the metric summary card.",
            "Bars show absolute percentage-point separation between stored OpenDrift/OpenOil and stored PyGNOME budgets.",
            "Comparator-only support: descriptive budget separation rather than observational skill validation.",
        ]
        scope_lines = [
            "Deterministic PyGNOME weathering pilot with matched case-specific grid wind/current forcing.",
            "Frozen light/heavy scenarios only; no base scenario exists in the stored prototype_2016 Phase 4 package.",
            "Shoreline comparison is unavailable in this pilot.",
            "Beached mass stays visible for transparency but is excluded from comparator metrics.",
        ]
        metric_lines = [
            "Metrics shown: absolute percentage-point difference at 24/48/72 h, plus MAE/RMSE across the normalized budget-fraction time series.",
        ]
        comparable_metrics = metrics_df.loc[metrics_df["comparable"].astype(bool)].copy()
        if not comparable_metrics.empty:
            by_scenario = []
            for scenario_key in ("light", "heavy"):
                subset = comparable_metrics.loc[comparable_metrics["scenario_key"].astype(str) == scenario_key]
                if subset.empty:
                    continue
                pieces = [
                    f"{row['compartment']}: MAE {float(row['mae_pct_points']):.2f}, RMSE {float(row['rmse_pct_points']):.2f}"
                    for _, row in subset.iterrows()
                ]
                by_scenario.append(f"{scenario_labels.get(scenario_key, scenario_key)} -> " + " | ".join(pieces))
            if by_scenario:
                metric_lines.extend(by_scenario)
        for key, title, lines, bullets in (
            ("guide", "How to read this board", guide_lines, True),
            ("scope", "Comparator Scope", scope_lines, True),
            ("metrics", "Metric Summary", metric_lines, False),
        ):
            ax = axes[key]
            ax.axis("off")
            ax.text(0.0, 0.98, title, ha="left", va="top", fontsize=panel_title_size, fontweight="bold", color="#0f172a", transform=ax.transAxes)
            ax.text(
                0.0,
                0.74,
                "\n".join(
                    textwrap.fill(f"- {line}" if bullets else line, width=50, subsequent_indent="  " if bullets else "")
                    for line in lines
                ),
                ha="left",
                va="top",
                fontsize=body_size,
                color="#334155",
                transform=ax.transAxes,
                bbox={"boxstyle": "round,pad=0.42", "facecolor": "#ffffff", "edgecolor": "#cbd5e1"},
            )

        fig.suptitle(
            f"{case_id.replace('CASE_', '')} Phase 4 budget-only PyGNOME comparator board",
            fontsize=title_size,
            fontweight="bold",
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path)
        plt.close(fig)

    def _scenario_manifest_entry(
        self,
        *,
        output_dir: Path,
        budget_csv: Path,
        png_path: Path,
        nc_path: Path,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(metadata)
        payload["relative_nc_path"] = _relative_to_repo(self.repo_root, nc_path)
        payload["relative_budget_csv_path"] = _relative_to_repo(self.repo_root, budget_csv)
        payload["relative_plot_path"] = _relative_to_repo(self.repo_root, png_path)
        payload["output_dir"] = _relative_to_repo(self.repo_root, output_dir)
        return payload

    def _run_case(self, case_id: str, *, reuse_existing_outputs_only: bool = False) -> dict[str, Any]:
        output_dir = self._comparator_dir(case_id)
        output_dir.mkdir(parents=True, exist_ok=True)

        selection = self._load_recipe_selection(case_id)
        origin = self._load_drifter_origin(case_id)
        forcing = get_forcing_files(selection.recipe, run_name=case_id)

        gnome_output_dir = output_dir
        self.gnome_service.output_dir = gnome_output_dir

        snapshot_rows: list[dict[str, Any]] = []
        metric_rows: list[dict[str, Any]] = []
        scenario_manifest: dict[str, Any] = {}
        scenario_labels: dict[str, str] = {}
        generated_pngs: list[str] = []
        generated_csvs: list[str] = []
        layout_audit_rows: list[dict[str, Any]] = []

        for scenario_key in SUPPORTED_SCENARIOS:
            oil_cfg = self.gnome_service.oils_cfg.get(scenario_key)
            if not oil_cfg:
                raise KeyError(f"Scenario '{scenario_key}' is not defined in config/oil.yaml.")
            scenario_label = str(oil_cfg.get("display_name") or scenario_key)
            scenario_labels[scenario_key] = scenario_label
            openoil_df = self._load_opendrift_budget(case_id, scenario_key)
            if reuse_existing_outputs_only:
                pygnome_df, nc_path, metadata = self._load_existing_pygnome_budget_outputs(
                    case_id=case_id,
                    scenario_key=scenario_key,
                )
            else:
                pygnome_df, nc_path, metadata = self.gnome_service.run_matched_phase4_weathering_scenario(
                    oil_key=scenario_key,
                    oil_cfg=oil_cfg,
                    start_lat=origin["start_lat"],
                    start_lon=origin["start_lon"],
                    start_time=origin["start_time"],
                    currents_file=forcing["currents"],
                    winds_file=forcing["wind"],
                    wave_file=forcing.get("wave"),
                    output_name=f"pygnome_{scenario_key}.nc",
                    duration_hours=int(forcing.get("duration_hours") or 72),
                    time_step_minutes=int(forcing.get("time_step_minutes") or 30),
                    output_timestep_minutes=60,
                    random_seed=20260314 + (0 if scenario_key == "light" else 1),
                )
            pygnome_budget_csv = output_dir / f"pygnome_budget_{scenario_key}.csv"
            merged = self._align_budget_frames(openoil_df, pygnome_df)
            case_snapshot_rows = self._snapshot_rows(case_id=case_id, scenario_key=scenario_key, merged=merged)
            case_metric_rows = self._time_series_metric_rows(case_id=case_id, scenario_key=scenario_key, merged=merged)
            snapshot_rows.extend(case_snapshot_rows)
            metric_rows.extend(case_metric_rows)

            scenario_png = output_dir / f"budget_time_series_{scenario_key}.png"
            self._plot_budget_time_series(
                case_id=case_id,
                scenario_key=scenario_key,
                scenario_label=scenario_label,
                openoil_df=openoil_df,
                pygnome_df=pygnome_df,
                metrics_df=pd.DataFrame(case_metric_rows),
                output_path=scenario_png,
            )
            generated_pngs.append(_relative_to_repo(self.repo_root, scenario_png))
            generated_csvs.append(_relative_to_repo(self.repo_root, pygnome_budget_csv))
            layout_audit_rows.append(
                {
                    "graphic_file": _relative_to_repo(self.repo_root, scenario_png),
                    "board_family": "Legacy 2016 Phase 4 budget-comparator graphics",
                    "issue_types_found": "overly dense note block | weak title hierarchy",
                    "layout_fix_applied": "Moved the narrative into dedicated footer cards and kept the plot grid clear.",
                    "requested_font_family": self.font_audit.requested_font_family,
                    "actual_font_resolved": self.font_audit.actual_font_family,
                    "exact_arial_used": bool(self.font_audit.exact_requested_font_used),
                    "fallback_needed": bool(self.font_audit.fallback_used),
                    "text_shortened_or_wrapped": True,
                    "filenames_stayed_same": True,
                }
            )
            scenario_manifest[scenario_key] = self._scenario_manifest_entry(
                output_dir=output_dir,
                budget_csv=pygnome_budget_csv,
                png_path=scenario_png,
                nc_path=nc_path,
                metadata=metadata,
            )

        snapshot_csv = output_dir / "phase4_budget_comparison.csv"
        metrics_csv = output_dir / "phase4_budget_time_series_metrics.csv"
        board_png = output_dir / "budget_comparison_board.png"
        self._plot_budget_board(
            case_id=case_id,
            scenario_labels=scenario_labels,
            snapshot_df=pd.DataFrame(snapshot_rows),
            metrics_df=pd.DataFrame(metric_rows),
            output_path=board_png,
        )
        generated_pngs.append(_relative_to_repo(self.repo_root, board_png))
        layout_audit_rows.append(
            {
                "graphic_file": _relative_to_repo(self.repo_root, board_png),
                "board_family": "Legacy 2016 Phase 4 budget-comparator boards",
                "issue_types_found": "uneven panel spacing | awkward reading-guide placement | overly dense note block",
                "layout_fix_applied": "Used a balanced two-panel top row with separate guide, scope, and metric cards.",
                "requested_font_family": self.font_audit.requested_font_family,
                "actual_font_resolved": self.font_audit.actual_font_family,
                "exact_arial_used": bool(self.font_audit.exact_requested_font_used),
                "fallback_needed": bool(self.font_audit.fallback_used),
                "text_shortened_or_wrapped": True,
                "filenames_stayed_same": True,
            }
        )

        snapshot_columns = [
            "case_id",
            "scenario_key",
            "hours_elapsed",
            "comparison_scope",
            "compartment",
            "opendrift_pct",
            "pygnome_pct",
            "abs_percentage_point_diff",
            "comparable",
            "comparable_reason",
        ]
        metric_columns = [
            "case_id",
            "scenario_key",
            "compartment",
            "comparable",
            "hours_compared",
            "start_hour",
            "end_hour",
            "mae_pct_points",
            "rmse_pct_points",
            "end_horizon_opendrift_pct",
            "end_horizon_pygnome_pct",
            "end_horizon_abs_diff_pct_points",
            "notes",
        ]
        _write_csv(snapshot_csv, snapshot_rows, snapshot_columns)
        _write_csv(metrics_csv, metric_rows, metric_columns)
        generated_csvs.extend(
            [
                _relative_to_repo(self.repo_root, snapshot_csv),
                _relative_to_repo(self.repo_root, metrics_csv),
            ]
        )
        self._write_font_audit(output_dir)
        self._write_board_layout_audit(output_dir, layout_audit_rows)

        manifest = {
            "phase": PHASE,
            "case_id": case_id,
            "workflow_mode": "prototype_2016",
            "generated_at_utc": _utc_now_iso(),
            "support_only": True,
            "comparator_only": True,
            "full_phase4_comparator_feasible": False,
            "budget_only_feasible": True,
            "shoreline_comparison_feasible": False,
            "decision_reason": (
                "Budget-only deterministic PyGNOME comparison is feasible with matched case-specific grid wind/current forcing. "
                "Shoreline comparison remains unavailable because the pilot does not generate matched shoreline-arrival or shoreline-segment products."
            ),
            "selected_recipe": selection.recipe,
            "selection_source": str(selection.source_path),
            "selection_status": selection.status_flag,
            "release_origin": origin,
            "forcing": {
                "recipe": str(forcing["recipe"]),
                "currents": _relative_to_repo(self.repo_root, Path(forcing["currents"])),
                "wind": _relative_to_repo(self.repo_root, Path(forcing["wind"])),
                "wave": _relative_to_repo(self.repo_root, Path(forcing["wave"])) if forcing.get("wave") else "",
            },
            "scenario_keys": list(SUPPORTED_SCENARIOS),
            "shoreline_comparison_available": False,
            "shoreline_comparison_reason": (
                "No matched PyGNOME shoreline-arrival or shoreline-segment outputs are produced for prototype_2016 Phase 4."
            ),
            "budget_metrics_are_observational_skill": False,
            "budget_metrics_description": [
                "absolute percentage-point difference at 24/48/72 h",
                "MAE across normalized budget-fraction time series",
                "RMSE across normalized budget-fraction time series",
            ],
            "generated_pngs": generated_pngs,
            "generated_csvs": generated_csvs,
            "font_audit_csv": _relative_to_repo(self.repo_root, output_dir / "font_audit.csv"),
            "board_layout_audit_csv": _relative_to_repo(self.repo_root, output_dir / "board_layout_audit.csv"),
            "scenarios": scenario_manifest,
            "font_family": self.font_family,
            "font_audit": self.font_audit.as_row(),
            "reuse_existing_outputs_only": bool(reuse_existing_outputs_only),
        }
        manifest_path = output_dir / "pygnome_phase4_run_manifest.json"
        _write_json(manifest_path, manifest)
        return {
            "case_id": case_id,
            "output_dir": _relative_to_repo(self.repo_root, output_dir),
            "run_manifest_json": _relative_to_repo(self.repo_root, manifest_path),
            "phase4_budget_comparison_csv": _relative_to_repo(self.repo_root, snapshot_csv),
            "phase4_budget_time_series_metrics_csv": _relative_to_repo(self.repo_root, metrics_csv),
            "budget_comparison_board_png": _relative_to_repo(self.repo_root, board_png),
            "font_audit_csv": _relative_to_repo(self.repo_root, output_dir / "font_audit.csv"),
            "board_layout_audit_csv": _relative_to_repo(self.repo_root, output_dir / "board_layout_audit.csv"),
            "generated_pngs": generated_pngs,
            "generated_csvs": generated_csvs,
            "budget_only_feasible": True,
            "shoreline_comparison_feasible": False,
        }

    def run(self, *, reuse_existing_outputs_only: bool = False) -> dict[str, Any]:
        if not reuse_existing_outputs_only and not GNOME_AVAILABLE:
            raise RuntimeError("prototype_2016 Phase 4 PyGNOME comparator pilot requires the gnome container.")

        case_results: list[dict[str, Any]] = []
        for case_id in self.case_ids:
            logger.info("Running prototype_2016 Phase 4 PyGNOME comparator pilot for %s", case_id)
            case_results.append(self._run_case(case_id, reuse_existing_outputs_only=reuse_existing_outputs_only))

        return {
            "phase": PHASE,
            "workflow_mode": "prototype_2016",
            "case_ids": list(self.case_ids),
            "case_results": case_results,
            "full_phase4_comparator_feasible": False,
            "budget_only_feasible": True,
            "shoreline_comparison_feasible": False,
            "font_family": self.font_family,
            "font_audit": self.font_audit.as_row(),
            "reuse_existing_outputs_only": bool(reuse_existing_outputs_only),
        }


def run_prototype_legacy_phase4_pygnome_comparator(
    repo_root: str | Path = ".",
    *,
    case_ids: list[str] | tuple[str, ...] | None = None,
    reuse_existing_outputs_only: bool = False,
) -> dict[str, Any]:
    service = PrototypeLegacyPhase4PygnomeComparatorService(repo_root=repo_root, case_ids=case_ids)
    return service.run(reuse_existing_outputs_only=reuse_existing_outputs_only)
