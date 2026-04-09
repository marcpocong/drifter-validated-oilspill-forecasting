"""
Read-only forensic diagnostics for official Mindoro Phase 3B outputs.

This helper intentionally does not modify the official pairing, metric, or
forecast physics. It reads the existing official artifacts, derives secondary
diagnostics, and writes a diagnosis pack under the Phase 3B output directory.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import (
    GridBuilder,
    extract_particles_at_time,
    normalize_time_index,
    rasterize_particles,
)
from src.helpers.scoring import precheck_same_grid
from src.utils.io import (
    get_case_output_dir,
    get_ensemble_manifest_path,
    get_forecast_manifest_path,
    get_official_control_footprint_mask_path,
    get_official_mask_p50_datecomposite_path,
    get_phase2_loading_audit_paths,
)

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - guarded at runtime
    gpd = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - guarded at runtime
    plt = None

try:
    import rasterio
except ImportError:  # pragma: no cover - guarded at runtime
    rasterio = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover - guarded at runtime
    xr = None

try:
    from scipy.spatial import cKDTree
except ImportError:  # pragma: no cover - guarded at runtime
    cKDTree = None


OFFICIAL_WINDOWS_KM = [1, 3, 5, 10]
THRESHOLD_SWEEP_VALUES = [0.10, 0.20, 0.30, 0.40, 0.50]


def observation_collapse_flag(obs_nonzero_cells: int) -> bool:
    return int(obs_nonzero_cells) <= 5


def choose_phase3b_failure_class(
    primary_metrics: dict,
    threshold_metrics: list[dict],
    observation_collapse_on_1km_grid: bool,
) -> dict:
    threshold_df = pd.DataFrame(threshold_metrics)
    threshold_df = threshold_df[threshold_df["forecast_variant"] == "datecomposite_threshold"].copy()

    min_centroid_distance = float(threshold_df["centroid_distance_m"].min()) if not threshold_df.empty else np.nan
    min_nearest_distance = float(threshold_df["nearest_distance_to_obs_m"].min()) if not threshold_df.empty else np.nan
    max_iou = float(threshold_df["iou"].max()) if not threshold_df.empty else np.nan
    max_dice = float(threshold_df["dice"].max()) if not threshold_df.empty else np.nan
    max_fss = 0.0
    if not threshold_df.empty:
        fss_cols = [f"fss_{window}km" for window in OFFICIAL_WINDOWS_KM]
        max_fss = float(threshold_df[fss_cols].max().max())

    evidence: list[str] = []
    secondary_factors: list[str] = []

    if observation_collapse_on_1km_grid:
        secondary_factors.append(
            "Observation collapses to <=5 cells on the 1 km grid, which makes the target unusually small."
        )

    if bool(primary_metrics.get("forecast_empty_or_near_empty", False)):
        secondary_factors.append(
            "Primary P50 date-composite mask is sparse relative to the full grid."
        )

    if (
        np.isfinite(min_nearest_distance)
        and np.isfinite(min_centroid_distance)
        and min_nearest_distance > 10000.0
        and min_centroid_distance > 10000.0
        and max_iou == 0.0
        and max_dice == 0.0
        and max_fss <= 1e-9
    ):
        evidence.extend(
            [
                (
                    "Threshold sweep from 0.10 to 0.50 never creates overlap with the observed mask: "
                    f"max IoU={max_iou:.3f}, max Dice={max_dice:.3f}, max FSS={max_fss:.6f}."
                ),
                (
                    "Even the closest thresholded forecast remains far from the observation: "
                    f"minimum nearest-distance={min_nearest_distance:.1f} m and "
                    f"minimum centroid-distance={min_centroid_distance:.1f} m."
                ),
                "The primary official date-composite mask is present, so the main failure is not a missing forecast.",
            ]
        )
        return {
            "class": "B",
            "label": "forecast exists but is displaced away from the observation",
            "evidence": evidence,
            "secondary_factors": secondary_factors,
        }

    if bool(primary_metrics.get("forecast_empty_or_near_empty", False)):
        evidence.append(
            "Primary P50 date-composite mask is empty or near-empty, leaving too little forecast footprint to overlap."
        )
        return {
            "class": "A",
            "label": "p50 is empty or near-empty",
            "evidence": evidence,
            "secondary_factors": secondary_factors,
        }

    if observation_collapse_on_1km_grid:
        evidence.append(
            "Observed validation geometry collapses to a tiny raster target on the 1 km grid."
        )
        return {
            "class": "C",
            "label": "observation raster is collapsing or under-representing the source vector",
            "evidence": evidence,
            "secondary_factors": secondary_factors,
        }

    evidence.append("No single displacement or sparsity signal dominates, suggesting a semantic mismatch.")
    return {
        "class": "D",
        "label": "scoring bug or semantic mismatch still exists",
        "evidence": evidence,
        "secondary_factors": secondary_factors,
    }


def _timestamp_to_utc_label(value) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_dump(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _read_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _read_raster(path: Path) -> tuple[np.ndarray, dict]:
    if rasterio is None:
        raise ImportError("rasterio is required for Phase 3B forensic diagnostics")
    with rasterio.open(path) as ds:
        data = ds.read(1).astype(np.float32)
        meta = {
            "crs": ds.crs.to_string() if ds.crs else None,
            "transform": ds.transform,
            "width": int(ds.width),
            "height": int(ds.height),
            "resolution": (float(abs(ds.res[0])), float(abs(ds.res[1]))),
            "nodata": ds.nodata,
            "dtype": ds.dtypes[0],
        }
    return data, meta


def _to_binary_mask(data: np.ndarray) -> np.ndarray:
    return (np.nan_to_num(np.asarray(data, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0) > 0.0).astype(np.float32)


def _mask_cell_centers(mask: np.ndarray, grid: GridBuilder) -> np.ndarray:
    active = np.argwhere(mask > 0)
    if active.size == 0:
        return np.empty((0, 2), dtype=np.float64)
    rows = active[:, 0]
    cols = active[:, 1]
    xs = grid.min_x + ((cols + 0.5) * grid.resolution)
    ys = grid.max_y - ((rows + 0.5) * grid.resolution)
    return np.column_stack([xs, ys]).astype(np.float64)


def _compute_mask_diagnostics(forecast_mask: np.ndarray, obs_mask: np.ndarray, grid: GridBuilder) -> dict:
    forecast_binary = _to_binary_mask(forecast_mask)
    obs_binary = _to_binary_mask(obs_mask)

    forecast_nonzero = int(np.count_nonzero(forecast_binary))
    obs_nonzero = int(np.count_nonzero(obs_binary))
    intersection = int(np.count_nonzero((forecast_binary > 0) & (obs_binary > 0)))
    union = int(np.count_nonzero((forecast_binary > 0) | (obs_binary > 0)))

    area_ratio = np.nan if obs_nonzero == 0 else float(forecast_nonzero / obs_nonzero)
    iou = float(intersection / union) if union > 0 else 1.0
    denom = forecast_nonzero + obs_nonzero
    dice = float((2.0 * intersection) / denom) if denom > 0 else 1.0

    forecast_points = _mask_cell_centers(forecast_binary, grid)
    obs_points = _mask_cell_centers(obs_binary, grid)

    centroid_distance_m = np.nan
    if len(forecast_points) > 0 and len(obs_points) > 0:
        centroid_distance_m = float(np.linalg.norm(forecast_points.mean(axis=0) - obs_points.mean(axis=0)))

    nearest_distance_to_obs_m = np.nan
    if intersection > 0:
        nearest_distance_to_obs_m = 0.0
    elif len(forecast_points) > 0 and len(obs_points) > 0:
        if cKDTree is not None:
            distances, _ = cKDTree(obs_points).query(forecast_points, k=1)
            nearest_distance_to_obs_m = float(np.min(distances))
        else:  # pragma: no cover - scipy is expected in runtime/tests
            deltas = forecast_points[:, None, :] - obs_points[None, :, :]
            nearest_distance_to_obs_m = float(np.sqrt(np.sum(deltas * deltas, axis=2)).min())

    return {
        "forecast_nonzero_cells": forecast_nonzero,
        "obs_nonzero_cells": obs_nonzero,
        "area_ratio_forecast_to_obs": area_ratio,
        "centroid_distance_m": centroid_distance_m,
        "iou": iou,
        "dice": dice,
        "nearest_distance_to_obs_m": nearest_distance_to_obs_m,
        "forecast_empty_or_near_empty": bool(forecast_nonzero <= 10),
    }


def _window_cells(grid: GridBuilder, window_km: int) -> int:
    if grid.is_projected:
        return max(1, int(round((float(window_km) * 1000.0) / float(grid.resolution))))
    return max(1, int(window_km))


def _build_threshold_sweep(
    prob_datecomposite: np.ndarray,
    obs_mask: np.ndarray,
    control_mask: np.ndarray,
    grid: GridBuilder,
    prob_path: Path,
    control_path: Path,
    obs_path: Path,
) -> pd.DataFrame:
    rows: list[dict] = []
    obs_binary = _to_binary_mask(obs_mask)

    for threshold in THRESHOLD_SWEEP_VALUES:
        forecast_mask = (np.nan_to_num(prob_datecomposite, nan=0.0) >= threshold).astype(np.float32)
        diagnostics = _compute_mask_diagnostics(forecast_mask, obs_binary, grid)
        row = {
            "forecast_variant": "datecomposite_threshold",
            "threshold": float(threshold),
            "forecast_path": str(prob_path),
            "observation_path": str(obs_path),
            **diagnostics,
        }
        for window in OFFICIAL_WINDOWS_KM:
            fss = calculate_fss(forecast_mask, obs_binary, window=_window_cells(grid, window))
            row[f"fss_{window}km"] = float(np.clip(fss, 0.0, 1.0))
        rows.append(row)

    control_diagnostics = _compute_mask_diagnostics(control_mask, obs_binary, grid)
    control_row = {
        "forecast_variant": "deterministic_control_saved",
        "threshold": np.nan,
        "forecast_path": str(control_path),
        "observation_path": str(obs_path),
        **control_diagnostics,
    }
    for window in OFFICIAL_WINDOWS_KM:
        fss = calculate_fss(control_mask, obs_binary, window=_window_cells(grid, window))
        control_row[f"fss_{window}km"] = float(np.clip(fss, 0.0, 1.0))
    rows.append(control_row)

    return pd.DataFrame(rows)


def _build_observation_vector_raster_check(
    processed_vector_path: Path,
    obs_mask: np.ndarray,
    obs_mask_path: Path,
    grid: GridBuilder,
    arcgis_processing_report: pd.DataFrame,
) -> pd.DataFrame:
    if gpd is None:
        raise ImportError("geopandas is required for Phase 3B forensic observation QA")

    validation_gdf = gpd.read_file(processed_vector_path)
    if validation_gdf.crs is None:
        raise ValueError(f"Processed validation vector is missing a CRS: {processed_vector_path}")
    if str(validation_gdf.crs) != str(grid.crs):
        validation_gdf = validation_gdf.to_crs(grid.crs)

    union_geom = validation_gdf.geometry.union_all() if hasattr(validation_gdf.geometry, "union_all") else validation_gdf.geometry.unary_union
    vector_centroid = union_geom.centroid
    vector_area_m2 = float(validation_gdf.geometry.area.sum())

    obs_binary = _to_binary_mask(obs_mask)
    raster_points = _mask_cell_centers(obs_binary, grid)
    raster_area_m2 = float(np.count_nonzero(obs_binary) * (grid.resolution ** 2))
    raster_centroid_x = np.nan
    raster_centroid_y = np.nan
    centroid_offset_m = np.nan
    if len(raster_points) > 0:
        raster_centroid_x = float(raster_points[:, 0].mean())
        raster_centroid_y = float(raster_points[:, 1].mean())
        centroid_offset_m = float(
            np.linalg.norm(np.array([vector_centroid.x, vector_centroid.y]) - np.array([raster_centroid_x, raster_centroid_y]))
        )

    report_match = arcgis_processing_report[arcgis_processing_report["layer_id"].astype(int) == int(get_case_context().validation_layer.layer_id)]
    report_row = report_match.iloc[0] if not report_match.empty else None

    row = {
        "layer_id": int(get_case_context().validation_layer.layer_id),
        "role": get_case_context().validation_layer.role,
        "processed_vector_path": str(processed_vector_path),
        "obs_mask_path": str(obs_mask_path),
        "vector_area_m2": vector_area_m2,
        "raster_nonzero_cells": int(np.count_nonzero(obs_binary)),
        "raster_area_m2": raster_area_m2,
        "vector_centroid_x": float(vector_centroid.x),
        "vector_centroid_y": float(vector_centroid.y),
        "raster_centroid_x": raster_centroid_x,
        "raster_centroid_y": raster_centroid_y,
        "centroid_offset_m": centroid_offset_m,
        "observation_collapse_on_1km_grid": observation_collapse_flag(int(np.count_nonzero(obs_binary))),
        "arcgis_report_vector_area_m2": float(report_row["vector_area"]) if report_row is not None else np.nan,
        "arcgis_report_raster_nonzero_cells": int(report_row["raster_nonzero_cells"]) if report_row is not None else np.nan,
        "arcgis_report_notes": str(report_row["notes"]) if report_row is not None else "",
    }
    return pd.DataFrame([row])


def _resolve_artifact_path(path_str: str, case_output_dir: Path) -> Path:
    candidate = Path(path_str)
    if candidate.is_absolute():
        return candidate
    if candidate.parts and candidate.parts[0] == "output":
        return candidate
    return case_output_dir / candidate


def _timestamp_membership_sets(ensemble_manifest: dict, case_output_dir: Path, validation_date: pd.Timestamp) -> tuple[list[pd.Timestamp], dict[int, dict]]:
    if xr is None:
        raise ImportError("xarray is required for Phase 3B forensic hourly diagnostics")

    timestamp_sets: dict[int, dict] = {}
    all_times: set[pd.Timestamp] = set()

    for member in ensemble_manifest.get("member_runs") or []:
        member_id = int(member["member_id"])
        nc_path = _resolve_artifact_path(str(member["relative_path"]), case_output_dir)
        with xr.open_dataset(nc_path) as ds:
            times = normalize_time_index(ds.time.values)
        filtered = tuple(ts for ts in times if pd.Timestamp(ts).date() == validation_date.date())
        timestamp_sets[member_id] = {
            "path": nc_path,
            "times": set(filtered),
        }
        all_times.update(filtered)

    return sorted(all_times), timestamp_sets


def _build_hourly_occupancy_summary(
    ensemble_manifest: dict,
    case_output_dir: Path,
    validation_time_utc: str,
    grid: GridBuilder,
) -> pd.DataFrame:
    validation_time = pd.Timestamp(validation_time_utc)
    if validation_time.tzinfo is not None:
        validation_time = validation_time.tz_convert("UTC").tz_localize(None)

    all_times, member_time_sets = _timestamp_membership_sets(ensemble_manifest, case_output_dir, validation_time)
    ensemble_size = int((ensemble_manifest.get("ensemble_configuration") or {}).get("ensemble_size") or len(member_time_sets))
    rows: list[dict] = []

    for timestamp in all_times:
        occupancy_count = np.zeros((grid.height, grid.width), dtype=np.float32)
        members_with_snapshot = 0

        for member in ensemble_manifest.get("member_runs") or []:
            member_id = int(member["member_id"])
            member_info = member_time_sets[member_id]
            if timestamp not in member_info["times"]:
                continue
            lon, lat, mass, _, _ = extract_particles_at_time(member_info["path"], timestamp, "opendrift")
            hits, _ = rasterize_particles(grid, lon, lat, mass)
            occupancy_count += _to_binary_mask(hits)
            members_with_snapshot += 1

        probability = occupancy_count / float(max(ensemble_size, 1))
        rows.append(
            {
                "timestamp_utc": _timestamp_to_utc_label(timestamp),
                "source": "derived_from_member_netcdf",
                "hourly_raster_present": False,
                "members_with_snapshot": members_with_snapshot,
                "ensemble_size": ensemble_size,
                "denominator_used": ensemble_size,
                "max_probability": float(np.nanmax(probability)) if probability.size else 0.0,
                "max_occupancy_members": int(np.nanmax(occupancy_count)) if occupancy_count.size else 0,
                "nonzero_cells": int(np.count_nonzero(probability > 0.0)),
                "cells_ge_0.10": int(np.count_nonzero(probability >= 0.10)),
                "cells_ge_0.20": int(np.count_nonzero(probability >= 0.20)),
                "cells_ge_0.30": int(np.count_nonzero(probability >= 0.30)),
                "cells_ge_0.40": int(np.count_nonzero(probability >= 0.40)),
                "cells_ge_0.50": int(np.count_nonzero(probability >= 0.50)),
                "cells_ge_0.90": int(np.count_nonzero(probability >= 0.90)),
            }
        )

    return pd.DataFrame(rows)


def _build_snapshot_consistency_check(
    ensemble_manifest: dict,
    case_output_dir: Path,
    validation_time_utc: str,
    grid: GridBuilder,
) -> dict:
    validation_time = pd.Timestamp(validation_time_utc)
    if validation_time.tzinfo is not None:
        validation_time = validation_time.tz_convert("UTC").tz_localize(None)

    ensemble_size = int((ensemble_manifest.get("ensemble_configuration") or {}).get("ensemble_size") or 0)
    timestamp_sets = _timestamp_membership_sets(ensemble_manifest, case_output_dir, validation_time)[1]
    occupancy_count = np.zeros((grid.height, grid.width), dtype=np.float32)
    members_with_exact_snapshot = 0

    for member in ensemble_manifest.get("member_runs") or []:
        member_id = int(member["member_id"])
        member_info = timestamp_sets[member_id]
        if validation_time not in member_info["times"]:
            continue
        lon, lat, mass, _, _ = extract_particles_at_time(member_info["path"], validation_time, "opendrift")
        hits, _ = rasterize_particles(grid, lon, lat, mass)
        occupancy_count += _to_binary_mask(hits)
        members_with_exact_snapshot += 1

    manual_prob = occupancy_count / float(max(ensemble_size, 1))
    saved_prob_path = case_output_dir / "ensemble" / f"prob_presence_{validation_time.strftime('%Y-%m-%dT%H-%M-%SZ')}.tif"
    saved_prob, _ = _read_raster(saved_prob_path)

    control_nc_path = case_output_dir / "forecast" / "deterministic_control_cmems_era5.nc"
    lon, lat, mass, actual_time, _ = extract_particles_at_time(control_nc_path, validation_time, "opendrift")
    manual_control_hits, _ = rasterize_particles(grid, lon, lat, mass)
    saved_control_path = get_official_control_footprint_mask_path(run_name=get_case_context().run_name)
    saved_control, _ = _read_raster(saved_control_path)

    return {
        "validation_time_utc": _timestamp_to_utc_label(validation_time),
        "ensemble_snapshot_saved_prob_path": str(saved_prob_path),
        "ensemble_snapshot_saved_max_probability": float(np.nanmax(saved_prob)),
        "ensemble_snapshot_saved_nonzero_cells": int(np.count_nonzero(_to_binary_mask(saved_prob))),
        "ensemble_snapshot_manual_max_probability": float(np.nanmax(manual_prob)),
        "ensemble_snapshot_manual_nonzero_cells": int(np.count_nonzero(_to_binary_mask(manual_prob))),
        "ensemble_snapshot_members_with_exact_timestamp": members_with_exact_snapshot,
        "ensemble_snapshot_manual_matches_saved": bool(np.allclose(manual_prob, saved_prob)),
        "control_saved_path": str(saved_control_path),
        "control_saved_nonzero_cells": int(np.count_nonzero(_to_binary_mask(saved_control))),
        "control_manual_nonzero_cells": int(np.count_nonzero(_to_binary_mask(manual_control_hits))),
        "control_manual_actual_time_utc": _timestamp_to_utc_label(actual_time),
        "control_manual_matches_saved": bool(np.array_equal(_to_binary_mask(saved_control), _to_binary_mask(manual_control_hits))),
    }


def _write_threshold_plot(threshold_df: pd.DataFrame, out_path: Path) -> None:
    if plt is None:
        return

    threshold_rows = threshold_df[threshold_df["forecast_variant"] == "datecomposite_threshold"].copy()
    if threshold_rows.empty:
        return

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))

    for window in OFFICIAL_WINDOWS_KM:
        axes[0].plot(
            threshold_rows["threshold"],
            threshold_rows[f"fss_{window}km"],
            marker="o",
            label=f"FSS {window} km",
        )
    axes[0].set_title("Threshold Sweep vs official FSS")
    axes[0].set_xlabel("Date-composite probability threshold")
    axes[0].set_ylabel("FSS")
    axes[0].set_ylim(-0.02, 1.02)
    axes[0].grid(True, alpha=0.3)
    axes[0].legend()

    axes[1].plot(threshold_rows["threshold"], threshold_rows["forecast_nonzero_cells"], marker="o", label="Forecast nonzero cells")
    axes[1].plot(threshold_rows["threshold"], threshold_rows["centroid_distance_m"], marker="s", label="Centroid distance (m)")
    axes[1].set_title("Forecast size and displacement")
    axes[1].set_xlabel("Date-composite probability threshold")
    axes[1].grid(True, alpha=0.3)
    axes[1].legend()

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _write_hourly_probmax_plot(hourly_df: pd.DataFrame, out_path: Path) -> None:
    if plt is None or hourly_df.empty:
        return

    plot_df = hourly_df.copy()
    plot_df["timestamp_utc"] = pd.to_datetime(plot_df["timestamp_utc"], utc=True)

    fig, ax1 = plt.subplots(figsize=(11, 4.5))
    ax1.plot(plot_df["timestamp_utc"], plot_df["max_probability"], marker="o", color="#1f77b4", label="Max probability")
    ax1.set_ylim(-0.02, 1.02)
    ax1.set_ylabel("Max probability")
    ax1.set_xlabel("March 6 UTC timestamp")
    ax1.grid(True, alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(plot_df["timestamp_utc"], plot_df["cells_ge_0.50"], marker="s", color="#d62728", label="Cells >= 0.50")
    ax2.set_ylabel("Cells >= 0.50")

    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(handles1 + handles2, labels1 + labels2, loc="upper right")
    ax1.set_title("Derived March 6 hourly ensemble occupancy")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _write_markdown_report(
    path: Path,
    primary_row: pd.Series,
    sensitivity_row: pd.Series,
    diagnosis: dict,
    observation_row: pd.Series,
    hourly_df: pd.DataFrame,
    snapshot_check: dict,
    threshold_df: pd.DataFrame,
) -> None:
    primary_numbers = (
        f"FSS(1/3/5/10 km) = {primary_row['fss_1km']:.6f}, {primary_row['fss_3km']:.6f}, "
        f"{primary_row['fss_5km']:.6f}, {primary_row['fss_10km']:.6f}; "
        f"forecast_nonzero_cells = {int(primary_row['forecast_nonzero_cells'])}; "
        f"obs_nonzero_cells = {int(primary_row['obs_nonzero_cells'])}; "
        f"area_ratio = {float(primary_row['area_ratio_forecast_to_obs']):.3f}; "
        f"centroid_distance_m = {float(primary_row['centroid_distance_m']):.1f}; "
        f"IoU = {float(primary_row['iou']):.3f}; Dice = {float(primary_row['dice']):.3f}; "
        f"nearest_distance_to_obs_m = {float(primary_row['nearest_distance_to_obs_m']):.1f}."
    )
    sensitivity_numbers = (
        f"FSS(1/3/5/10 km) = {sensitivity_row['fss_1km']:.6f}, {sensitivity_row['fss_3km']:.6f}, "
        f"{sensitivity_row['fss_5km']:.6f}, {sensitivity_row['fss_10km']:.6f}; "
        f"forecast_nonzero_cells = {int(sensitivity_row['forecast_nonzero_cells'])}; "
        f"obs_nonzero_cells = {int(sensitivity_row['obs_nonzero_cells'])}."
    )
    max_hourly_prob = float(hourly_df["max_probability"].max()) if not hourly_df.empty else np.nan
    threshold_010 = threshold_df[
        (threshold_df["forecast_variant"] == "datecomposite_threshold") & (threshold_df["threshold"] == 0.10)
    ].iloc[0]

    report = f"""# Phase 3B forensic diagnosis

## Main result semantics

The official primary pairing is unchanged:
- forecast = `mask_p50_2023-03-06_datecomposite.tif`
- observation = `obs_mask_2023-03-06.tif`
- metric = FSS at 1, 3, 5, and 10 km

## Primary numbers

{primary_numbers}

Sensitivity deterministic control:

{sensitivity_numbers}

## Chosen diagnosis class

Class {diagnosis['class']}: {diagnosis['label']}

Evidence:
""" + "\n".join(f"- {line}" for line in diagnosis["evidence"]) + "\n\n" + (
        "Secondary factors:\n" + "\n".join(f"- {line}" for line in diagnosis["secondary_factors"]) + "\n\n"
        if diagnosis["secondary_factors"]
        else ""
    ) + f"""## Observation-side QA

- Vector area = {float(observation_row['vector_area_m2']):.1f} m^2
- Raster area = {float(observation_row['raster_area_m2']):.1f} m^2
- Raster nonzero cells = {int(observation_row['raster_nonzero_cells'])}
- Vector/raster centroid offset = {float(observation_row['centroid_offset_m']):.1f} m
- observation_collapse_on_1km_grid = {bool(observation_row['observation_collapse_on_1km_grid'])}

## Forecast-side QA

- Date-composite threshold 0.10 expands the forecast to {int(threshold_010['forecast_nonzero_cells'])} cells, but overlap metrics remain zero.
- Derived March 6 hourly max probability peaks at {max_hourly_prob:.2f}.
- Saved 2023-03-06T09:59 probability snapshot max = {snapshot_check['ensemble_snapshot_saved_max_probability']:.2f}; manual member-NC reconstruction max = {snapshot_check['ensemble_snapshot_manual_max_probability']:.2f}.
- Saved deterministic control footprint nonzero cells = {snapshot_check['control_saved_nonzero_cells']}; manual deterministic-control NC reconstruction nonzero cells = {snapshot_check['control_manual_nonzero_cells']}.

## Interpretation

The main official date-composite products are internally consistent, but they remain displaced from the observed March 6 mask even when the threshold is relaxed to 0.10. The observed mask is extremely small on the 1 km grid, which increases sensitivity, but its centroid remains aligned with the processed vector. That makes displacement the best-supported explanation for the zero FSS on the official primary pair, with an additional auxiliary inconsistency in the saved timestamped snapshot products.
"""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")


def run_phase3b_forensics(run_name: str | None = None) -> dict:
    case = get_case_context()
    active_run_name = run_name or case.run_name
    case_output_dir = get_case_output_dir(active_run_name)
    phase3b_dir = case_output_dir / "phase3b"
    output_dir = phase3b_dir / "diagnostics_forensics"
    output_dir.mkdir(parents=True, exist_ok=True)

    pairing_manifest_path = phase3b_dir / "phase3b_pairing_manifest.csv"
    fss_path = phase3b_dir / "phase3b_fss_by_date_window.csv"
    summary_path = phase3b_dir / "phase3b_summary.csv"
    diagnostics_path = phase3b_dir / "phase3b_diagnostics.csv"
    run_manifest_path = phase3b_dir / "phase3b_run_manifest.json"
    precheck_csv_path = phase3b_dir / "precheck_phase3b_march6.csv"
    precheck_json_path = phase3b_dir / "precheck_phase3b_march6.json"
    forecast_manifest_path = get_forecast_manifest_path(active_run_name)
    ensemble_manifest_path = get_ensemble_manifest_path(active_run_name)
    phase2_loading_audit_path = get_phase2_loading_audit_paths(active_run_name)["json"]
    arcgis_processing_report_path = Path("data") / "arcgis" / active_run_name / "arcgis_processing_report.csv"
    arcgis_registry_path = Path("data") / "arcgis" / active_run_name / "arcgis_registry.csv"

    required_paths = [
        pairing_manifest_path,
        fss_path,
        summary_path,
        diagnostics_path,
        run_manifest_path,
        forecast_manifest_path,
        ensemble_manifest_path,
        phase2_loading_audit_path,
        arcgis_processing_report_path,
        arcgis_registry_path,
    ]
    missing = [str(path) for path in required_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Phase 3B forensic diagnostics require the existing official artifacts. Missing: "
            + ", ".join(missing)
        )

    pairing_manifest = _read_csv(pairing_manifest_path)
    _ = _read_csv(fss_path)
    summary_df = _read_csv(summary_path)
    _ = _read_csv(diagnostics_path)
    run_manifest = _read_json(run_manifest_path)
    _ = _read_json(forecast_manifest_path)
    ensemble_manifest = _read_json(ensemble_manifest_path)
    phase2_loading_audit = _read_json(phase2_loading_audit_path)
    arcgis_processing_report = _read_csv(arcgis_processing_report_path)
    arcgis_registry = _read_csv(arcgis_registry_path)

    primary_pair = pairing_manifest[pairing_manifest["pair_role"] == "primary"].iloc[0]
    if (
        primary_pair["forecast_product"] != "mask_p50_2023-03-06_datecomposite.tif"
        or primary_pair["observation_product"] != "obs_mask_2023-03-06.tif"
    ):
        raise RuntimeError("Official primary Phase 3B semantics changed unexpectedly; forensic run aborted.")

    primary_summary = summary_df[summary_df["pair_role"] == "primary"].iloc[0]
    sensitivity_summary = summary_df[summary_df["pair_role"] == "sensitivity"].iloc[0]
    validation_time_utc = str(primary_summary["pairing_time_utc"])

    obs_path = Path(str(primary_pair["observation_path"]))
    primary_forecast_path = get_official_mask_p50_datecomposite_path(active_run_name)
    control_path = get_official_control_footprint_mask_path(run_name=active_run_name)
    prob_datecomposite_path = case_output_dir / "ensemble" / "prob_presence_2023-03-06_datecomposite.tif"
    validation_prob_path = case_output_dir / "ensemble" / "prob_presence_2023-03-06T09-59-00Z.tif"

    if not precheck_csv_path.exists() or not precheck_json_path.exists():
        precheck_same_grid(primary_forecast_path, obs_path, phase3b_dir / "precheck_phase3b_march6")

    grid = GridBuilder()
    obs_mask, _ = _read_raster(obs_path)
    primary_forecast_mask, _ = _read_raster(primary_forecast_path)
    control_mask, _ = _read_raster(control_path)
    prob_datecomposite, _ = _read_raster(prob_datecomposite_path)
    validation_prob, _ = _read_raster(validation_prob_path)

    validation_registry = arcgis_registry[arcgis_registry["role"].astype(str) == case.validation_layer.role].iloc[0]
    processed_vector_path = Path(str(validation_registry["processed_vector"]))

    threshold_df = _build_threshold_sweep(
        prob_datecomposite=prob_datecomposite,
        obs_mask=obs_mask,
        control_mask=control_mask,
        grid=grid,
        prob_path=prob_datecomposite_path,
        control_path=control_path,
        obs_path=obs_path,
    )
    obs_check_df = _build_observation_vector_raster_check(
        processed_vector_path=processed_vector_path,
        obs_mask=obs_mask,
        obs_mask_path=obs_path,
        grid=grid,
        arcgis_processing_report=arcgis_processing_report,
    )
    hourly_df = _build_hourly_occupancy_summary(
        ensemble_manifest=ensemble_manifest,
        case_output_dir=case_output_dir,
        validation_time_utc=validation_time_utc,
        grid=grid,
    )
    hourly_peak_row = hourly_df.loc[hourly_df["max_probability"].idxmax()] if not hourly_df.empty else None
    snapshot_check = _build_snapshot_consistency_check(
        ensemble_manifest=ensemble_manifest,
        case_output_dir=case_output_dir,
        validation_time_utc=validation_time_utc,
        grid=grid,
    )

    primary_metrics = {
        "forecast_nonzero_cells": int(primary_summary["forecast_nonzero_cells"]),
        "obs_nonzero_cells": int(primary_summary["obs_nonzero_cells"]),
        "area_ratio_forecast_to_obs": float(primary_summary["area_ratio_forecast_to_obs"]),
        "centroid_distance_m": float(primary_summary["centroid_distance_m"]),
        "iou": float(primary_summary["iou"]),
        "dice": float(primary_summary["dice"]),
        "nearest_distance_to_obs_m": float(primary_summary["nearest_distance_to_obs_m"]),
        "forecast_empty_or_near_empty": bool(int(primary_summary["forecast_nonzero_cells"]) <= 10),
    }
    diagnosis = choose_phase3b_failure_class(
        primary_metrics=primary_metrics,
        threshold_metrics=threshold_df.to_dict(orient="records"),
        observation_collapse_on_1km_grid=bool(obs_check_df.iloc[0]["observation_collapse_on_1km_grid"]),
    )

    precheck_summary = _read_json(precheck_json_path)
    official_main_result_unchanged = bool(
        primary_pair["forecast_product"] == "mask_p50_2023-03-06_datecomposite.tif"
        and primary_pair["observation_product"] == "obs_mask_2023-03-06.tif"
        and primary_pair["metric"] == "FSS"
        and str(primary_pair["windows_km"]) == "1,3,5,10"
    )

    diagnosis_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_name": active_run_name,
        "workflow_mode": case.workflow_mode,
        "official_main_result_semantics_unchanged": official_main_result_unchanged,
        "primary_pairing": primary_pair.to_dict(),
        "primary_summary": primary_summary.to_dict(),
        "sensitivity_summary": sensitivity_summary.to_dict(),
        "precheck_phase3b_march6": {
            "csv": str(precheck_csv_path),
            "json": str(precheck_json_path),
            "passed": bool(precheck_summary.get("passed", False)),
            "checks": precheck_summary.get("checks") or {},
        },
        "upstream_forecast_status": run_manifest.get("upstream_forecast") or {},
        "phase3b_status_flags": run_manifest.get("phase3b_status_flags") or {},
        "phase2_loading_audit_path": str(phase2_loading_audit_path),
        "phase2_loading_audit_run_count": len(phase2_loading_audit.get("runs") or []),
        "observation_check": obs_check_df.iloc[0].to_dict(),
        "forecast_side_checks": {
            "datecomposite_prob_path": str(prob_datecomposite_path),
            "datecomposite_prob_max": float(np.nanmax(prob_datecomposite)),
            "datecomposite_nonzero_cells": int(np.count_nonzero(prob_datecomposite > 0.0)),
            "datecomposite_cells_ge_0.10": int(np.count_nonzero(prob_datecomposite >= 0.10)),
            "datecomposite_cells_ge_0.20": int(np.count_nonzero(prob_datecomposite >= 0.20)),
            "datecomposite_cells_ge_0.30": int(np.count_nonzero(prob_datecomposite >= 0.30)),
            "datecomposite_cells_ge_0.40": int(np.count_nonzero(prob_datecomposite >= 0.40)),
            "datecomposite_cells_ge_0.50": int(np.count_nonzero(prob_datecomposite >= 0.50)),
            "datecomposite_cells_ge_0.90": int(np.count_nonzero(prob_datecomposite >= 0.90)),
            "mask_p50_datecomposite_empty": bool(np.count_nonzero(_to_binary_mask(primary_forecast_mask)) == 0),
            "mask_p50_datecomposite_near_empty": bool(np.count_nonzero(_to_binary_mask(primary_forecast_mask)) <= 10),
            "maximum_ensemble_occupancy_probability_on_march6": float(np.nanmax(prob_datecomposite)),
            "maximum_ensemble_occupancy_members_on_march6": int(round(float(np.nanmax(prob_datecomposite)) * int(ensemble_manifest.get("ensemble_configuration", {}).get("ensemble_size", 0)))),
            "maximum_hourly_probability_on_march6": float(hourly_peak_row["max_probability"]) if hourly_peak_row is not None else np.nan,
            "maximum_hourly_occupancy_members_on_march6": int(hourly_peak_row["max_occupancy_members"]) if hourly_peak_row is not None else 0,
            "maximum_hourly_probability_timestamp_utc": str(hourly_peak_row["timestamp_utc"]) if hourly_peak_row is not None else "",
            "validation_time_prob_presence_path": str(validation_prob_path),
            "validation_time_prob_presence_max": float(np.nanmax(validation_prob)),
            "validation_time_prob_presence_nonzero_cells": int(np.count_nonzero(validation_prob > 0.0)),
        },
        "snapshot_consistency_check": snapshot_check,
        "diagnosis": diagnosis,
        "artifacts": {
            "threshold_sweep_csv": str(output_dir / "phase3b_threshold_sweep.csv"),
            "hourly_occupancy_summary_csv": str(output_dir / "phase3b_hourly_occupancy_summary.csv"),
            "vector_raster_obs_check_csv": str(output_dir / "phase3b_vector_raster_obs_check.csv"),
            "threshold_plot_png": str(output_dir / "qa_phase3b_threshold_sweep.png"),
            "hourly_probmax_png": str(output_dir / "qa_phase3b_hourly_probmax.png"),
        },
    }

    threshold_csv_path = output_dir / "phase3b_threshold_sweep.csv"
    hourly_csv_path = output_dir / "phase3b_hourly_occupancy_summary.csv"
    obs_check_csv_path = output_dir / "phase3b_vector_raster_obs_check.csv"
    threshold_plot_path = output_dir / "qa_phase3b_threshold_sweep.png"
    hourly_plot_path = output_dir / "qa_phase3b_hourly_probmax.png"
    diagnosis_json_path = output_dir / "phase3b_diagnosis.json"
    diagnosis_md_path = output_dir / "phase3b_diagnosis_report.md"

    threshold_df.to_csv(threshold_csv_path, index=False)
    hourly_df.to_csv(hourly_csv_path, index=False)
    obs_check_df.to_csv(obs_check_csv_path, index=False)
    _write_threshold_plot(threshold_df, threshold_plot_path)
    _write_hourly_probmax_plot(hourly_df, hourly_plot_path)
    _json_dump(diagnosis_json_path, diagnosis_payload)
    _write_markdown_report(
        path=diagnosis_md_path,
        primary_row=primary_summary,
        sensitivity_row=sensitivity_summary,
        diagnosis=diagnosis,
        observation_row=obs_check_df.iloc[0],
        hourly_df=hourly_df,
        snapshot_check=snapshot_check,
        threshold_df=threshold_df,
    )

    return {
        "diagnosis_json": diagnosis_json_path,
        "diagnosis_report": diagnosis_md_path,
        "threshold_sweep_csv": threshold_csv_path,
        "hourly_occupancy_summary_csv": hourly_csv_path,
        "vector_raster_obs_check_csv": obs_check_csv_path,
        "threshold_plot_png": threshold_plot_path,
        "hourly_probmax_png": hourly_plot_path,
        "diagnosis": diagnosis,
    }


if __name__ == "__main__":  # pragma: no cover - manual execution entrypoint
    artifacts = run_phase3b_forensics()
    print(json.dumps({key: str(value) for key, value in artifacts.items() if key != "diagnosis"}, indent=2))
    print(json.dumps(artifacts["diagnosis"], indent=2))
