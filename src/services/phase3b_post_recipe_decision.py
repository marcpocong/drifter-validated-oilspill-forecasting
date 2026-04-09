"""
Read-only decision pack builder after Mindoro Phase 3B recipe sensitivities.

This helper does not rerun forecast physics or change the official scoring
semantics. It reads the current local artifacts, compares tested recipes, and
records the single best next action.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.utils.io import (
    get_case_output_dir,
    get_recipe_sensitivity_output_dir,
    resolve_recipe_selection,
)


FSS_COLUMNS = ["fss_1km", "fss_3km", "fss_5km", "fss_10km"]


def _read_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _coerce_float(value) -> float:
    if value is None:
        return float("nan")
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return float("nan")
    return float(text)


def _completed_recipe_rows(summary_df: pd.DataFrame) -> pd.DataFrame:
    completed = summary_df[summary_df["status"].astype(str) == "completed"].copy()
    if completed.empty:
        return completed

    for column in (
        FSS_COLUMNS
        + [
            "mean_fss",
            "centroid_distance_m",
            "area_ratio_forecast_to_obs",
            "iou",
            "dice",
            "nearest_distance_to_obs_m",
            "p50_nonzero_cells",
            "obs_nonzero_cells",
            "max_march6_probability",
            "max_march6_occupancy_members",
        ]
    ):
        if column in completed.columns:
            completed[column] = completed[column].map(_coerce_float)

    if "provisional_transport_model" in completed.columns:
        completed["provisional_transport_model"] = completed["provisional_transport_model"].map(_coerce_bool)
    if "matches_frozen_historical_baseline" in completed.columns:
        completed["matches_frozen_historical_baseline"] = completed["matches_frozen_historical_baseline"].map(_coerce_bool)

    return completed


def rank_completed_recipes(summary_df: pd.DataFrame) -> pd.DataFrame:
    completed = _completed_recipe_rows(summary_df)
    if completed.empty:
        return completed

    ranked = completed.sort_values(
        by=[
            "mean_fss",
            "fss_10km",
            "fss_5km",
            "fss_3km",
            "fss_1km",
            "iou",
            "dice",
            "centroid_distance_m",
            "nearest_distance_to_obs_m",
        ],
        ascending=[False, False, False, False, False, False, False, True, True],
        na_position="last",
    ).reset_index(drop=True)
    ranked["decision_rank"] = np.arange(1, len(ranked) + 1)
    return ranked


def classify_post_recipe_outcome(summary_df: pd.DataFrame, baseline_recipe: str) -> dict:
    ranked = rank_completed_recipes(summary_df)
    if ranked.empty:
        return {
            "class": "R3",
            "label": "no meaningful improvement",
            "next_action": "run a coastal/initialization/domain displacement audit",
            "rationale": [
                "No completed recipe sensitivities are available, so there is no evidence of a recipe-driven improvement.",
            ],
            "best_recipe_id": "",
            "baseline_recipe_id": baseline_recipe,
            "best_differs_from_frozen_baseline": False,
        }

    best = ranked.iloc[0]
    baseline_rows = ranked[ranked["recipe_id"].astype(str) == str(baseline_recipe)]
    baseline = baseline_rows.iloc[0] if not baseline_rows.empty else None
    second = ranked.iloc[1] if len(ranked) > 1 else None

    max_fss = float(ranked[FSS_COLUMNS].max().max())
    max_iou = float(ranked["iou"].max())
    max_dice = float(ranked["dice"].max())
    centroid_gain = float("nan")
    nearest_gain = float("nan")
    if baseline is not None:
        centroid_gain = float(baseline["centroid_distance_m"] - best["centroid_distance_m"])
        nearest_gain = float(baseline["nearest_distance_to_obs_m"] - best["nearest_distance_to_obs_m"])

    best_differs = str(best["recipe_id"]) != str(baseline_recipe)
    rationale = [
        (
            f"Best tested recipe is `{best['recipe_id']}` with FSS(1/3/5/10 km)="
            f"{best['fss_1km']:.4f}/{best['fss_3km']:.4f}/{best['fss_5km']:.4f}/{best['fss_10km']:.4f}."
        ),
        (
            f"Across completed recipes, max IoU={max_iou:.4f}, max Dice={max_dice:.4f}, "
            f"and max FSS={max_fss:.4f}."
        ),
    ]
    if baseline is not None:
        rationale.append(
            (
                f"Relative to the frozen historical baseline `{baseline_recipe}`, the best recipe changes "
                f"centroid distance by {centroid_gain:.1f} m and nearest-distance by {nearest_gain:.1f} m."
            )
        )

    if (
        max_fss >= 0.10
        and max_iou > 0.0
        and (
            baseline is None
            or float(best["mean_fss"]) >= float(baseline["mean_fss"]) + 0.05
            or float(best["iou"]) >= float(baseline["iou"]) + 0.05
        )
    ):
        return {
            "class": "R1",
            "label": "meaningful improvement",
            "next_action": "lock best event-scale recipe and run convergence + shoreline-mask integration",
            "rationale": rationale,
            "best_recipe_id": str(best["recipe_id"]),
            "baseline_recipe_id": baseline_recipe,
            "best_differs_from_frozen_baseline": best_differs,
        }

    if (
        second is not None
        and (
            max_fss > 0.0
            or max_iou > 0.0
            or max_dice > 0.0
            or (np.isfinite(centroid_gain) and centroid_gain >= 25000.0)
            or (np.isfinite(nearest_gain) and nearest_gain >= 25000.0)
        )
    ):
        rationale.append(
            (
                f"The second-ranked recipe `{second['recipe_id']}` remains close enough on the comparison metrics "
                "that the result is still unstable."
            )
        )
        return {
            "class": "R2",
            "label": "mixed but promising",
            "next_action": "run convergence on top 2 recipes and integrate shoreline mask",
            "rationale": rationale,
            "best_recipe_id": str(best["recipe_id"]),
            "baseline_recipe_id": baseline_recipe,
            "best_differs_from_frozen_baseline": best_differs,
        }

    rationale.append(
        "Displacement or zero-overlap behavior persists across the tested recipes, so the next blocker is more likely coastal, initialization, or domain related than recipe choice alone."
    )
    return {
        "class": "R3",
        "label": "no meaningful improvement",
        "next_action": "run a coastal/initialization/domain displacement audit",
        "rationale": rationale,
        "best_recipe_id": str(best["recipe_id"]),
        "baseline_recipe_id": baseline_recipe,
        "best_differs_from_frozen_baseline": best_differs,
    }


def _write_markdown_report(
    path: Path,
    decision: dict,
    best_recipe: pd.Series,
    baseline_recipe: str,
    official_primary: pd.Series,
    ranked_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
) -> None:
    lines = [
        "# Mindoro Phase 3B Post-Recipe Decision",
        "",
        "## Classification",
        "",
        f"- Decision class: `{decision['class']}` = {decision['label']}",
        f"- Best event-scale recipe: `{decision['best_recipe_id']}`",
        f"- Frozen historical baseline recipe: `{baseline_recipe}`",
        f"- Exact next action: `{decision['next_action']}`",
        "",
        "## Official Current Result",
        "",
        (
            f"- Current official primary FSS(1/3/5/10 km) = "
            f"{_coerce_float(official_primary['fss_1km']):.4f}/"
            f"{_coerce_float(official_primary['fss_3km']):.4f}/"
            f"{_coerce_float(official_primary['fss_5km']):.4f}/"
            f"{_coerce_float(official_primary['fss_10km']):.4f}"
        ),
        (
            f"- Current official centroid distance = {_coerce_float(official_primary['centroid_distance_m']):.1f} m; "
            f"IoU = {_coerce_float(official_primary['iou']):.4f}; "
            f"Dice = {_coerce_float(official_primary['dice']):.4f}"
        ),
        "",
        "## Recipe Comparison",
        "",
    ]

    for _, row in ranked_df.iterrows():
        lines.append(
            (
                f"- `{row['recipe_id']}` | FSS={row['fss_1km']:.4f}/{row['fss_3km']:.4f}/{row['fss_5km']:.4f}/{row['fss_10km']:.4f} "
                f"| centroid={row['centroid_distance_m']:.1f} m | area_ratio={row['area_ratio_forecast_to_obs']:.2f} "
                f"| IoU={row['iou']:.4f} | Dice={row['dice']:.4f} | nearest={row['nearest_distance_to_obs_m']:.1f} m "
                f"| p50_cells={int(row['p50_nonzero_cells'])} | obs_cells={int(row['obs_nonzero_cells'])} "
                f"| max_occ={int(row['max_march6_occupancy_members'])} | provisional_transport={bool(row['provisional_transport_model'])}"
            )
        )

    if not skipped_df.empty:
        lines.extend(["", "## Skipped Recipes", ""])
        for _, row in skipped_df.iterrows():
            lines.append(f"- `{row['recipe_id']}` skipped because forcing was unavailable: `{row['missing_inputs']}`")

    lines.extend(["", "## Rationale", ""])
    for item in decision["rationale"]:
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "## Provenance Note",
            "",
            (
                f"- The best event-scale recipe {'differs from' if decision['best_differs_from_frozen_baseline'] else 'matches'} "
                f"the frozen historical baseline `{baseline_recipe}`."
            ),
            "- This is an event-scale Phase 3B sensitivity result only.",
            "- `config/phase1_baseline_selection.yaml` was not edited automatically.",
        ]
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_phase3b_post_recipe_decision(run_name: str | None = None) -> dict:
    case = get_case_context()
    if not case.is_official:
        raise RuntimeError("Phase 3B post-recipe decision pack is only available for official workflow modes.")

    active_run_name = run_name or case.run_name
    base_output_dir = get_case_output_dir(active_run_name)
    recipe_root = get_recipe_sensitivity_output_dir(active_run_name)
    decision_dir = base_output_dir / "decision_after_recipe_sensitivity"
    decision_dir.mkdir(parents=True, exist_ok=True)

    input_paths = {
        "recipe_sensitivity_summary_csv": recipe_root / "recipe_sensitivity_summary.csv",
        "recipe_sensitivity_by_window_csv": recipe_root / "recipe_sensitivity_by_window.csv",
        "recipe_sensitivity_diagnostics_csv": recipe_root / "recipe_sensitivity_diagnostics.csv",
        "recipe_sensitivity_report_md": recipe_root / "recipe_sensitivity_report.md",
        "phase3b_summary_csv": base_output_dir / "phase3b" / "phase3b_summary.csv",
        "phase3b_diagnostics_csv": base_output_dir / "phase3b" / "phase3b_diagnostics.csv",
        "phase3b_run_manifest_json": base_output_dir / "phase3b" / "phase3b_run_manifest.json",
        "forecast_manifest_json": base_output_dir / "forecast" / "forecast_manifest.json",
        "ensemble_manifest_json": base_output_dir / "ensemble" / "ensemble_manifest.json",
    }

    missing_required = [
        name
        for name, path in input_paths.items()
        if name != "recipe_sensitivity_report_md" and not path.exists()
    ]
    if missing_required:
        raise FileNotFoundError(
            "Phase 3B post-recipe decision pack requires existing official outputs. Missing: "
            + ", ".join(f"{name}={input_paths[name]}" for name in missing_required)
        )

    recipe_summary_df = pd.read_csv(input_paths["recipe_sensitivity_summary_csv"])
    recipe_by_window_df = pd.read_csv(input_paths["recipe_sensitivity_by_window_csv"])
    recipe_diagnostics_df = pd.read_csv(input_paths["recipe_sensitivity_diagnostics_csv"])
    recipe_report_text = (
        input_paths["recipe_sensitivity_report_md"].read_text(encoding="utf-8")
        if input_paths["recipe_sensitivity_report_md"].exists()
        else ""
    )
    phase3b_summary_df = pd.read_csv(input_paths["phase3b_summary_csv"])
    phase3b_diagnostics_df = pd.read_csv(input_paths["phase3b_diagnostics_csv"])
    phase3b_run_manifest = _read_json(input_paths["phase3b_run_manifest_json"])
    forecast_manifest = _read_json(input_paths["forecast_manifest_json"])
    ensemble_manifest = _read_json(input_paths["ensemble_manifest_json"])

    primary_official = phase3b_summary_df[phase3b_summary_df["pair_role"].astype(str) == "primary"]
    if primary_official.empty:
        raise RuntimeError("Official Phase 3B summary is missing the primary row.")
    primary_official_row = primary_official.iloc[0]

    frozen_selection = resolve_recipe_selection()
    baseline_recipe = str(frozen_selection.recipe)
    ranked_df = rank_completed_recipes(recipe_summary_df)
    decision = classify_post_recipe_outcome(recipe_summary_df, baseline_recipe)

    best_recipe_row = ranked_df.iloc[0] if not ranked_df.empty else None
    skipped_df = recipe_summary_df[recipe_summary_df["status"].astype(str) != "completed"].copy()

    window_check = {}
    if not recipe_by_window_df.empty:
        for recipe_id, frame in recipe_by_window_df.groupby("recipe_id"):
            primary_frame = frame[frame["pair_role"].astype(str) == "primary"].copy()
            windows = sorted({int(_coerce_float(value)) for value in primary_frame["window_km"].tolist()})
            window_check[str(recipe_id)] = windows

    decision_table_df = recipe_summary_df.copy()
    if not ranked_df.empty:
        rank_map = {str(row["recipe_id"]): int(row["decision_rank"]) for _, row in ranked_df.iterrows()}
        decision_table_df["decision_rank"] = decision_table_df["recipe_id"].astype(str).map(rank_map)
    else:
        decision_table_df["decision_rank"] = np.nan
    decision_table_df["classification"] = decision["class"]
    decision_table_df["recommended_next_action"] = decision["next_action"]
    decision_table_df["best_recipe_matches_row"] = decision_table_df["recipe_id"].astype(str) == str(decision["best_recipe_id"])
    decision_table_df["best_differs_from_frozen_baseline"] = bool(decision["best_differs_from_frozen_baseline"])

    decision_table_path = decision_dir / "phase3b_post_recipe_decision_table.csv"
    decision_json_path = decision_dir / "phase3b_post_recipe_decision.json"
    decision_md_path = decision_dir / "phase3b_post_recipe_decision.md"

    decision_table_df.to_csv(decision_table_path, index=False)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_name": active_run_name,
        "workflow_mode": case.workflow_mode,
        "classification": decision,
        "exact_next_action": decision["next_action"],
        "frozen_historical_baseline": {
            "recipe_id": baseline_recipe,
            "selection_source_kind": frozen_selection.source_kind,
            "selection_source_path": frozen_selection.source_path,
            "status_flag": frozen_selection.status_flag,
            "valid": frozen_selection.valid,
            "provisional": frozen_selection.provisional,
            "rerun_required": frozen_selection.rerun_required,
        },
        "best_recipe": (
            {
                "recipe_id": str(best_recipe_row["recipe_id"]),
                "fss_1km": float(best_recipe_row["fss_1km"]),
                "fss_3km": float(best_recipe_row["fss_3km"]),
                "fss_5km": float(best_recipe_row["fss_5km"]),
                "fss_10km": float(best_recipe_row["fss_10km"]),
                "mean_fss": float(best_recipe_row["mean_fss"]),
                "centroid_distance_m": float(best_recipe_row["centroid_distance_m"]),
                "area_ratio_forecast_to_obs": float(best_recipe_row["area_ratio_forecast_to_obs"]),
                "iou": float(best_recipe_row["iou"]),
                "dice": float(best_recipe_row["dice"]),
                "nearest_distance_to_obs_m": float(best_recipe_row["nearest_distance_to_obs_m"]),
                "p50_nonzero_cells": int(best_recipe_row["p50_nonzero_cells"]),
                "obs_nonzero_cells": int(best_recipe_row["obs_nonzero_cells"]),
                "max_march6_occupancy_members": int(best_recipe_row["max_march6_occupancy_members"]),
                "transport_model": str(best_recipe_row["transport_model"]),
                "provisional_transport_model": bool(best_recipe_row["provisional_transport_model"]),
                "matches_frozen_historical_baseline": bool(best_recipe_row["matches_frozen_historical_baseline"]),
            }
            if best_recipe_row is not None
            else {}
        ),
        "official_current_primary_result": {
            "forecast_product": str(primary_official_row["forecast_product"]),
            "observation_product": str(primary_official_row["observation_product"]),
            "fss_1km": _coerce_float(primary_official_row["fss_1km"]),
            "fss_3km": _coerce_float(primary_official_row["fss_3km"]),
            "fss_5km": _coerce_float(primary_official_row["fss_5km"]),
            "fss_10km": _coerce_float(primary_official_row["fss_10km"]),
            "centroid_distance_m": _coerce_float(primary_official_row["centroid_distance_m"]),
            "area_ratio_forecast_to_obs": _coerce_float(primary_official_row["area_ratio_forecast_to_obs"]),
            "iou": _coerce_float(primary_official_row["iou"]),
            "dice": _coerce_float(primary_official_row["dice"]),
            "nearest_distance_to_obs_m": _coerce_float(primary_official_row["nearest_distance_to_obs_m"]),
        },
        "upstream_status": {
            "transport_model": str((forecast_manifest.get("transport") or {}).get("model") or ""),
            "provisional_transport_model": bool((forecast_manifest.get("transport") or {}).get("provisional_transport_model", False)),
            "forecast_status_flags": forecast_manifest.get("status_flags") or {},
            "ensemble_status_flags": ensemble_manifest.get("status_flags") or {},
            "phase3b_status_flags": phase3b_run_manifest.get("phase3b_status_flags") or {},
            "deterministic_element_count": int((((forecast_manifest.get("deterministic_control") or {}).get("actual_element_count")) or 0)),
            "ensemble_element_count": int((((ensemble_manifest.get("ensemble_configuration") or {}).get("element_count")) or 0)),
        },
        "recipe_window_check": window_check,
        "artifacts_read": {name: {"path": str(path), "exists": path.exists()} for name, path in input_paths.items()},
        "recipe_sensitivity_report_excerpt": recipe_report_text[:1500],
        "recipe_comparison_rows": decision_table_df.to_dict(orient="records"),
        "source_artifact_paths": {
            "decision_table_csv": str(decision_table_path),
            "decision_json": str(decision_json_path),
            "decision_md": str(decision_md_path),
        },
        "provenance_note": (
            "The best event-scale recipe is recorded as a Phase 3B sensitivity result only. "
            "config/phase1_baseline_selection.yaml was not edited automatically."
        ),
    }
    _write_json(decision_json_path, payload)

    _write_markdown_report(
        path=decision_md_path,
        decision=decision,
        best_recipe=best_recipe_row if best_recipe_row is not None else pd.Series(dtype=object),
        baseline_recipe=baseline_recipe,
        official_primary=primary_official_row,
        ranked_df=ranked_df,
        skipped_df=skipped_df,
    )

    return {
        "classification": decision["class"],
        "best_recipe_id": decision["best_recipe_id"],
        "next_action": decision["next_action"],
        "decision_table_csv": decision_table_path,
        "decision_json": decision_json_path,
        "decision_md": decision_md_path,
    }


if __name__ == "__main__":  # pragma: no cover - manual execution entrypoint
    artifacts = run_phase3b_post_recipe_decision()
    print(json.dumps({key: str(value) for key, value in artifacts.items()}, indent=2))
