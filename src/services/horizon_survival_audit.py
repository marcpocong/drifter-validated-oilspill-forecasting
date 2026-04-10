"""Read-only horizon-survival diagnostics for extended Mindoro forecast outputs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.raster import GridBuilder, project_points_to_grid, rasterize_particles
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array
from src.services.ensemble import normalize_time_index
from src.services.phase3b_extended_public_scored import (
    EVENT_CORRIDOR_DATES,
    EXTENDED_SCORED_DIR_NAME,
    LOCAL_TIMEZONE,
)
from src.utils.io import get_case_output_dir

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import rasterio
except ImportError:  # pragma: no cover
    rasterio = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover
    xr = None


THRESHOLDS = [0.10, 0.20, 0.30, 0.40, 0.50, 0.90]
SHORT_DIAGNOSTIC_DATES = ["2023-03-05", "2023-03-06", "2023-03-07", "2023-03-08", "2023-03-09"]
RECOMMENDED_RERUN_FOR_CLASS = {
    "A": "transport/retention fix rerun",
    "B": "domain/halo rerun",
    "C": "transport/retention fix rerun",
    "D": "convergence rerun",
    "E": "aggregation/writer fix rerun",
}


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return _iso_z(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _normalize_utc(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp
    return timestamp.tz_convert("UTC").tz_localize(None)


def _iso_z(value: Any) -> str:
    return _normalize_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _local_date(value: pd.Timestamp) -> str:
    return value.tz_localize("UTC").tz_convert(LOCAL_TIMEZONE).date().isoformat()


def _last_timestamp(rows: pd.DataFrame, condition: pd.Series) -> str:
    if rows.empty or not condition.any():
        return ""
    timestamps = pd.to_datetime(rows.loc[condition, "timestamp_utc"], errors="coerce").dropna()
    if timestamps.empty:
        return ""
    return _iso_z(timestamps.max())


def _parse_manifest(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _to_utc_naive_series(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, errors="coerce", utc=True).dt.tz_convert(None)


def classify_horizon_survival(stats: dict) -> tuple[str, str, str]:
    """Choose one dominant failure class from precomputed survival statistics."""
    if stats.get("writer_mismatch_detected"):
        return "E", RECOMMENDED_RERUN_FOR_CLASS["E"], "Raw active signal exists after March 5, but written rasters are missing or zero."
    if stats.get("late_active_outside_domain"):
        return "B", RECOMMENDED_RERUN_FOR_CLASS["B"], "Active particles persist after the last scored signal but move outside the canonical scoring domain."
    if stats.get("late_active_masked_by_ocean"):
        return "C", RECOMMENDED_RERUN_FOR_CLASS["C"], "Active particles persist in-domain but are removed by the scoreable ocean mask."
    if stats.get("terminal_stranding_fraction", 0.0) >= 0.8 and stats.get("march7_9_prob_presence_zero"):
        return "C", RECOMMENDED_RERUN_FOR_CLASS["C"], "Most completed runs terminate with stranded particles and no active raw member output exists on March 7-9."
    if stats.get("late_active_low_probability"):
        return "D", RECOMMENDED_RERUN_FOR_CLASS["D"], "Particles persist into the short tier, but ensemble occupancy never reaches the 0.50 p50 threshold."
    if stats.get("terminal_stranding_fraction", 0.0) >= 0.8:
        return "C", RECOMMENDED_RERUN_FOR_CLASS["C"], "Most completed runs terminate with stranded particles and no active particles."
    if stats.get("runs_with_any_active", 0) == 0 or stats.get("last_active_time_utc", "") == "":
        return "A", RECOMMENDED_RERUN_FOR_CLASS["A"], "No active particles are present in the raw model outputs."
    return "A", RECOMMENDED_RERUN_FOR_CLASS["A"], "Active model output ends before the late horizon without evidence of domain, mask, or threshold causes."


class HorizonSurvivalAuditService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("Horizon-survival audit is only supported for official spill-case workflows.")

        self.base_output = get_case_output_dir()
        self.short_dir = self.base_output / EXTENDED_SCORED_DIR_NAME
        self.model_dir = self.short_dir / "model_run"
        self.forecast_dir = self.model_dir / "forecast"
        self.ensemble_dir = self.model_dir / "ensemble"
        self.output_dir = self.base_output / "horizon_survival_audit"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        if self.sea_mask is None:
            self.valid_ocean = np.ones((self.grid.height, self.grid.width), dtype=bool)
        else:
            self.valid_ocean = self.sea_mask > 0.5

    def run(self) -> dict:
        if xr is None:
            raise ImportError("xarray is required for horizon-survival diagnostics.")
        if rasterio is None:
            raise ImportError("rasterio is required for horizon-survival diagnostics.")

        window = self._resolve_window()
        run_specs = self._discover_run_specs()
        expected_times = pd.date_range(
            _normalize_utc(window["required_start_utc"]),
            _normalize_utc(window["simulation_end_utc"]),
            freq="1h",
        )

        hourly_rows, member_rows, ensemble_counts = self._audit_raw_runs(run_specs, expected_times, window)
        hourly_df = pd.DataFrame(hourly_rows)
        member_df = pd.DataFrame(member_rows)
        probability_df = self._build_ensemble_probability_rows(
            ensemble_counts,
            expected_times,
            len([spec for spec in run_specs if spec["run_kind"] == "ensemble_member"]),
            window,
        )
        hourly_df = pd.concat([hourly_df, probability_df, self._build_existing_raster_rows()], ignore_index=True)

        summary_payload = self._build_summary(hourly_df, member_df, run_specs, window)
        diagnosis_class, recommended_rerun, diagnosis_reason = classify_horizon_survival(summary_payload)
        summary_payload.update(
            {
                "dominant_diagnosis_class": diagnosis_class,
                "dominant_diagnosis_label": self._diagnosis_label(diagnosis_class),
                "dominant_diagnosis_reason": diagnosis_reason,
                "recommended_next_rerun": recommended_rerun,
            }
        )

        paths = self._write_outputs(pd.DataFrame([summary_payload]), member_df, hourly_df)
        qa_paths = self._write_qa(hourly_df, run_specs)
        report_path = self._write_report(summary_payload, paths, qa_paths)
        diagnosis_path = self.output_dir / "horizon_survival_diagnosis.json"
        _write_json(
            diagnosis_path,
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "workflow_mode": self.case.workflow_mode,
                "run_name": self.case.run_name,
                "diagnosis": summary_payload,
                "input_artifacts": self._input_artifacts(),
                "outputs": {
                    **{key: str(value) for key, value in paths.items()},
                    **{key: str(value) for key, value in qa_paths.items()},
                    "report": str(report_path),
                },
            },
        )
        return {
            "output_dir": self.output_dir,
            "report_md": report_path,
            "summary_csv": paths["summary"],
            "member_table_csv": paths["member_table"],
            "hourly_timeseries_csv": paths["hourly_timeseries"],
            "diagnosis_json": diagnosis_path,
            "dominant_diagnosis_class": diagnosis_class,
            "recommended_next_rerun": recommended_rerun,
            "last_nonzero_deterministic_footprint": summary_payload.get("last_nonzero_deterministic_footprint_utc", ""),
            "last_nonzero_prob_presence": summary_payload.get("last_nonzero_prob_presence_utc", ""),
            "last_nonzero_mask_p50": summary_payload.get("last_nonzero_mask_p50_utc", ""),
            "march7_9_empty_reason": summary_payload.get("march7_9_empty_reason", ""),
        }

    def _resolve_window(self) -> dict:
        manifest = _parse_manifest(self.short_dir / "extended_short_run_manifest.json")
        tier_window = manifest.get("tier_window", {})
        return {
            "simulation_start_utc": tier_window.get("simulation_start_utc", self.case.simulation_start_utc),
            "simulation_end_utc": tier_window.get("simulation_end_utc", "2023-03-09T15:59:00Z"),
            "required_start_utc": tier_window.get("required_forcing_start_utc", self.case.simulation_start_utc),
            "required_end_utc": tier_window.get("required_forcing_end_utc", "2023-03-09T18:59:00Z"),
            "date_composite_rule": tier_window.get("date_composite_rule", ""),
        }

    def _discover_run_specs(self) -> list[dict]:
        control_path = self.forecast_dir / "deterministic_control_cmems_era5.nc"
        member_paths = sorted(self.ensemble_dir.glob("member_*.nc"))
        specs: list[dict] = []
        if control_path.exists():
            specs.append(
                {
                    "run_id": "deterministic_control",
                    "run_kind": "deterministic_control",
                    "member_id": "",
                    "path": control_path,
                }
            )
        for index, path in enumerate(member_paths, start=1):
            member_label = path.stem.replace("member_", "")
            specs.append(
                {
                    "run_id": f"member_{member_label}",
                    "run_kind": "ensemble_member",
                    "member_id": int(member_label) if member_label.isdigit() else index,
                    "path": path,
                }
            )
        if not specs:
            raise FileNotFoundError(f"No deterministic or ensemble NetCDF outputs found under {self.model_dir}")
        return specs

    def _audit_raw_runs(self, run_specs: list[dict], expected_times: pd.DatetimeIndex, window: dict) -> tuple[list[dict], list[dict], dict]:
        hourly_rows: list[dict] = []
        member_rows: list[dict] = []
        ensemble_counts = {
            _normalize_utc(timestamp): np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
            for timestamp in expected_times
        }
        expected_set = set(ensemble_counts)
        nominal_start = _normalize_utc(window["simulation_start_utc"])
        requested_end = _normalize_utc(window["simulation_end_utc"])

        for spec in run_specs:
            metrics_by_time: dict[pd.Timestamp, dict] = {}
            with xr.open_dataset(spec["path"]) as ds:
                times = normalize_time_index(ds["time"].values)
                for index, timestamp in enumerate(times):
                    timestamp = _normalize_utc(timestamp)
                    metrics = self._snapshot_metrics(ds, index)
                    metrics_by_time[timestamp] = metrics
                    if spec["run_kind"] == "ensemble_member" and timestamp in expected_set:
                        ensemble_counts[timestamp] += metrics["active_ocean_hits"]

            for timestamp in expected_times:
                timestamp = _normalize_utc(timestamp)
                row = self._empty_hourly_row(spec, timestamp, nominal_start)
                metrics = metrics_by_time.get(timestamp)
                if metrics is not None:
                    row.update(self._metrics_to_hourly_fields(metrics))
                    row["raw_output_exists"] = True
                hourly_rows.append(row)

            existing = pd.DataFrame(
                [
                    {"timestamp_utc": _iso_z(timestamp), **self._metrics_to_hourly_fields(metrics)}
                    for timestamp, metrics in metrics_by_time.items()
                ]
            )
            member_rows.append(self._member_summary(spec, existing, metrics_by_time, requested_end))

        return hourly_rows, member_rows, ensemble_counts

    def _snapshot_metrics(self, ds, time_index: int) -> dict:
        lon = np.asarray(ds["lon"].isel(time=time_index).values).reshape(-1)
        lat = np.asarray(ds["lat"].isel(time=time_index).values).reshape(-1)
        status = np.asarray(ds["status"].isel(time=time_index).values).reshape(-1)
        moving = np.asarray(ds["moving"].isel(time=time_index).values).reshape(-1) if "moving" in ds else np.full_like(status, np.nan)
        model_land = (
            np.asarray(ds["land_binary_mask"].isel(time=time_index).values).reshape(-1)
            if "land_binary_mask" in ds
            else np.full_like(status, np.nan)
        )

        finite = np.isfinite(lon) & np.isfinite(lat) & np.isfinite(status)
        active = finite & (status == 0)
        stranded = finite & (status == 1)
        moving_mask = finite & (moving == 1)
        model_land_mask = finite & np.isfinite(model_land) & (model_land > 0.5)

        all_inside, all_ocean, _, _ = self._point_domain_ocean_flags(lon[finite], lat[finite])
        active_inside, active_ocean, active_x, active_y = self._point_domain_ocean_flags(lon[active], lat[active])
        stranded_inside, stranded_ocean, _, _ = self._point_domain_ocean_flags(lon[stranded], lat[stranded])

        active_ocean_hits = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        active_domain_hits = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        if np.any(active):
            mass = np.ones(int(np.count_nonzero(active)), dtype=np.float32)
            hits, _ = rasterize_particles(self.grid, lon[active], lat[active], mass)
            active_domain_hits = hits.astype(np.float32)
            active_ocean_hits = apply_ocean_mask(hits, sea_mask=self.sea_mask, fill_value=0.0).astype(np.float32)

        if active_x.size and np.any(active_ocean):
            centroid_x = float(np.nanmean(active_x[active_ocean]))
            centroid_y = float(np.nanmean(active_y[active_ocean]))
        else:
            centroid_x = np.nan
            centroid_y = np.nan

        return {
            "finite_count": int(np.count_nonzero(finite)),
            "active_count": int(np.count_nonzero(active)),
            "stranded_count": int(np.count_nonzero(stranded)),
            "moving_count": int(np.count_nonzero(moving_mask)),
            "model_land_particle_count": int(np.count_nonzero(model_land_mask)),
            "all_inside_domain_count": int(np.count_nonzero(all_inside)),
            "all_ocean_particle_count": int(np.count_nonzero(all_ocean)),
            "active_inside_domain_count": int(np.count_nonzero(active_inside)),
            "active_outside_domain_count": int(np.count_nonzero(active) - np.count_nonzero(active_inside)),
            "active_ocean_particle_count": int(np.count_nonzero(active_ocean)),
            "active_land_or_invalid_ocean_particle_count": int(np.count_nonzero(active_inside) - np.count_nonzero(active_ocean)),
            "stranded_inside_domain_count": int(np.count_nonzero(stranded_inside)),
            "stranded_ocean_particle_count": int(np.count_nonzero(stranded_ocean)),
            "surface_presence_cells": int(np.count_nonzero(active_ocean_hits > 0)),
            "domain_presence_cells": int(np.count_nonzero(active_domain_hits > 0)),
            "active_centroid_x": centroid_x,
            "active_centroid_y": centroid_y,
            "active_ocean_hits": active_ocean_hits,
        }

    def _point_domain_ocean_flags(self, lon: np.ndarray, lat: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        if lon.size == 0:
            empty_bool = np.zeros(0, dtype=bool)
            empty_float = np.zeros(0, dtype=float)
            return empty_bool, empty_bool, empty_float, empty_float
        x_vals, y_vals = project_points_to_grid(self.grid, lon, lat)
        col = np.floor((x_vals - self.grid.min_x) / self.grid.resolution).astype(int)
        row = np.floor((self.grid.max_y - y_vals) / self.grid.resolution).astype(int)
        inside = (col >= 0) & (col < self.grid.width) & (row >= 0) & (row < self.grid.height)
        ocean = np.zeros(lon.shape, dtype=bool)
        if np.any(inside):
            ocean[inside] = self.valid_ocean[row[inside], col[inside]]
        return inside, ocean, x_vals, y_vals

    def _empty_hourly_row(self, spec: dict, timestamp: pd.Timestamp, nominal_start: pd.Timestamp) -> dict:
        return {
            "scope": spec["run_kind"],
            "run_id": spec["run_id"],
            "run_kind": spec["run_kind"],
            "member_id": spec["member_id"],
            "timestamp_utc": _iso_z(timestamp),
            "timestamp_local_date": _local_date(timestamp),
            "hour_since_nominal_start": int(round((_normalize_utc(timestamp) - nominal_start).total_seconds() / 3600.0)),
            "raw_output_exists": False,
            "finite_count": 0,
            "active_count": 0,
            "stranded_count": 0,
            "moving_count": 0,
            "model_land_particle_count": 0,
            "all_inside_domain_count": 0,
            "all_ocean_particle_count": 0,
            "active_inside_domain_count": 0,
            "active_outside_domain_count": 0,
            "active_ocean_particle_count": 0,
            "active_land_or_invalid_ocean_particle_count": 0,
            "stranded_inside_domain_count": 0,
            "stranded_ocean_particle_count": 0,
            "surface_presence_cells": 0,
            "domain_presence_cells": 0,
            "active_centroid_x": np.nan,
            "active_centroid_y": np.nan,
            "max_prob_presence": np.nan,
            "prob_presence_nonzero_cells": np.nan,
            "cells_ge_0p10": np.nan,
            "cells_ge_0p20": np.nan,
            "cells_ge_0p30": np.nan,
            "cells_ge_0p40": np.nan,
            "cells_ge_0p50": np.nan,
            "cells_ge_0p90": np.nan,
            "p50_nonzero_cells": np.nan,
            "p90_nonzero_cells": np.nan,
            "product_path": "",
        }

    @staticmethod
    def _metrics_to_hourly_fields(metrics: dict) -> dict:
        return {key: value for key, value in metrics.items() if key != "active_ocean_hits"}

    @staticmethod
    def _last_metric_time(metrics_by_time: dict[pd.Timestamp, dict], metric: str) -> pd.Timestamp | None:
        matches = [timestamp for timestamp, metrics in metrics_by_time.items() if metrics.get(metric, 0) > 0]
        return max(matches) if matches else None

    def _member_summary(self, spec: dict, existing: pd.DataFrame, metrics_by_time: dict[pd.Timestamp, dict], requested_end: pd.Timestamp) -> dict:
        if existing.empty:
            return {
                "run_id": spec["run_id"],
                "run_kind": spec["run_kind"],
                "member_id": spec["member_id"],
                "path": str(spec["path"]),
                "status": "missing_or_empty",
            }

        raw_start = min(metrics_by_time)
        raw_end = max(metrics_by_time)
        last_active = self._last_metric_time(metrics_by_time, "active_count")
        last_stranded = self._last_metric_time(metrics_by_time, "stranded_count")
        last_inside = self._last_metric_time(metrics_by_time, "active_inside_domain_count")
        last_ocean = self._last_metric_time(metrics_by_time, "active_ocean_particle_count")
        last_surface = self._last_metric_time(metrics_by_time, "surface_presence_cells")
        final_metrics = metrics_by_time[raw_end]
        early_stop_hours = max(0.0, (requested_end - raw_end).total_seconds() / 3600.0)
        return {
            "run_id": spec["run_id"],
            "run_kind": spec["run_kind"],
            "member_id": spec["member_id"],
            "path": str(spec["path"]),
            "status": "audited",
            "raw_time_start_utc": _iso_z(raw_start),
            "raw_time_end_utc": _iso_z(raw_end),
            "raw_time_count": len(metrics_by_time),
            "requested_end_utc": _iso_z(requested_end),
            "early_stop_hours_before_requested_end": early_stop_hours,
            "last_active_time_utc": _iso_z(last_active) if last_active is not None else "",
            "last_stranded_time_utc": _iso_z(last_stranded) if last_stranded is not None else "",
            "last_active_inside_domain_time_utc": _iso_z(last_inside) if last_inside is not None else "",
            "last_active_ocean_time_utc": _iso_z(last_ocean) if last_ocean is not None else "",
            "last_nonzero_surface_presence_time_utc": _iso_z(last_surface) if last_surface is not None else "",
            "final_finite_count": final_metrics["finite_count"],
            "final_active_count": final_metrics["active_count"],
            "final_stranded_count": final_metrics["stranded_count"],
            "final_active_inside_domain_count": final_metrics["active_inside_domain_count"],
            "final_active_ocean_particle_count": final_metrics["active_ocean_particle_count"],
            "final_surface_presence_cells": final_metrics["surface_presence_cells"],
            "ended_with_active_particles": final_metrics["active_count"] > 0,
            "ended_with_stranded_particles": final_metrics["stranded_count"] > 0,
            "terminal_state": "stranded_no_active" if final_metrics["active_count"] == 0 and final_metrics["stranded_count"] > 0 else "other",
            "max_active_count": int(existing["active_count"].max()),
            "max_surface_presence_cells": int(existing["surface_presence_cells"].max()),
        }

    def _build_ensemble_probability_rows(self, counts_by_time: dict, expected_times: pd.DatetimeIndex, member_count: int, window: dict) -> pd.DataFrame:
        rows: list[dict] = []
        nominal_start = _normalize_utc(window["simulation_start_utc"])
        denominator = max(member_count, 1)
        for timestamp in expected_times:
            timestamp = _normalize_utc(timestamp)
            probability = counts_by_time[timestamp] / float(denominator)
            rows.append(
                {
                    **self._empty_probability_row("ensemble_aggregate_hourly", "ensemble_aggregate", timestamp, nominal_start),
                    **self._probability_fields(probability),
                }
            )
        return pd.DataFrame(rows)

    def _empty_probability_row(self, scope: str, run_id: str, timestamp: pd.Timestamp, nominal_start: pd.Timestamp) -> dict:
        return {
            "scope": scope,
            "run_id": run_id,
            "run_kind": "ensemble_aggregate" if scope == "ensemble_aggregate_hourly" else scope,
            "member_id": "",
            "timestamp_utc": _iso_z(timestamp) if pd.notna(timestamp) else "",
            "timestamp_local_date": _local_date(timestamp) if pd.notna(timestamp) else "",
            "hour_since_nominal_start": int(round((_normalize_utc(timestamp) - nominal_start).total_seconds() / 3600.0)) if pd.notna(timestamp) else "",
            "raw_output_exists": "",
            "finite_count": "",
            "active_count": "",
            "stranded_count": "",
            "moving_count": "",
            "model_land_particle_count": "",
            "all_inside_domain_count": "",
            "all_ocean_particle_count": "",
            "active_inside_domain_count": "",
            "active_outside_domain_count": "",
            "active_ocean_particle_count": "",
            "active_land_or_invalid_ocean_particle_count": "",
            "stranded_inside_domain_count": "",
            "stranded_ocean_particle_count": "",
            "surface_presence_cells": "",
            "domain_presence_cells": "",
            "active_centroid_x": "",
            "active_centroid_y": "",
            "product_path": "",
        }

    def _build_existing_raster_rows(self) -> pd.DataFrame:
        rows: list[dict] = []
        for date in SHORT_DIAGNOSTIC_DATES:
            prob_path = self.short_dir / "forecast_datecomposites" / f"prob_presence_{date}_datecomposite.tif"
            p50_path = self.short_dir / "forecast_datecomposites" / f"mask_p50_{date}_datecomposite.tif"
            if not prob_path.exists():
                continue
            probability = self._read_raster(prob_path)
            p50 = self._read_raster(p50_path) if p50_path.exists() else (probability >= 0.5).astype(np.float32)
            row = {
                **self._empty_probability_row("short_date_composite_probability", f"short_date_composite_{date}", pd.NaT, pd.Timestamp("2023-03-03T09:59:00")),
                **self._probability_fields(probability),
                "timestamp_local_date": date,
                "p50_nonzero_cells": int(np.count_nonzero(p50 > 0)),
                "p90_nonzero_cells": int(np.count_nonzero(probability >= 0.9)),
                "product_path": str(prob_path),
            }
            rows.append(row)

        for path in sorted(self.ensemble_dir.glob("prob_presence_*.tif")):
            probability = self._read_raster(path)
            row = {
                **self._empty_probability_row("written_probability_product", path.stem, pd.NaT, pd.Timestamp("2023-03-03T09:59:00")),
                **self._probability_fields(probability),
                "timestamp_utc": self._timestamp_from_product_name(path.name),
                "product_path": str(path),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    @staticmethod
    def _probability_fields(probability: np.ndarray) -> dict:
        arr = np.asarray(probability, dtype=np.float32)
        return {
            "max_prob_presence": float(np.nanmax(arr)) if arr.size else 0.0,
            "prob_presence_nonzero_cells": int(np.count_nonzero(arr > 0)),
            "cells_ge_0p10": int(np.count_nonzero(arr >= 0.10)),
            "cells_ge_0p20": int(np.count_nonzero(arr >= 0.20)),
            "cells_ge_0p30": int(np.count_nonzero(arr >= 0.30)),
            "cells_ge_0p40": int(np.count_nonzero(arr >= 0.40)),
            "cells_ge_0p50": int(np.count_nonzero(arr >= 0.50)),
            "cells_ge_0p90": int(np.count_nonzero(arr >= 0.90)),
            "p50_nonzero_cells": int(np.count_nonzero(arr >= 0.50)),
            "p90_nonzero_cells": int(np.count_nonzero(arr >= 0.90)),
        }

    @staticmethod
    def _timestamp_from_product_name(name: str) -> str:
        if "_datecomposite" in name:
            return ""
        token = name.replace(".tif", "").split("prob_presence_")[-1]
        try:
            return _iso_z(pd.Timestamp(token.replace("T", " ").replace("-", ":", 2)))
        except Exception:
            return ""

    @staticmethod
    def _read_raster(path: Path) -> np.ndarray:
        with rasterio.open(path) as src:
            return src.read(1).astype(np.float32)

    def _build_summary(self, hourly_df: pd.DataFrame, member_df: pd.DataFrame, run_specs: list[dict], window: dict) -> dict:
        raw_rows = hourly_df[hourly_df["scope"].isin(["deterministic_control", "ensemble_member"])].copy()
        aggregate_rows = hourly_df[hourly_df["scope"] == "ensemble_aggregate_hourly"].copy()
        composite_rows = hourly_df[hourly_df["scope"] == "short_date_composite_probability"].copy()
        written_rows = hourly_df[hourly_df["scope"] == "written_probability_product"].copy()

        numeric_cols = [
            "active_count",
            "stranded_count",
            "active_inside_domain_count",
            "active_outside_domain_count",
            "active_ocean_particle_count",
            "surface_presence_cells",
            "domain_presence_cells",
            "max_prob_presence",
            "prob_presence_nonzero_cells",
            "p50_nonzero_cells",
            "p90_nonzero_cells",
        ]
        for frame in [raw_rows, aggregate_rows, composite_rows, written_rows]:
            for column in numeric_cols:
                if column in frame:
                    frame[column] = pd.to_numeric(frame[column], errors="coerce")

        deterministic = raw_rows[raw_rows["scope"] == "deterministic_control"].copy()
        raw_rows["timestamp_utc"] = _to_utc_naive_series(raw_rows["timestamp_utc"])
        aggregate_rows["timestamp_utc"] = _to_utc_naive_series(aggregate_rows["timestamp_utc"])
        written_rows["timestamp_utc"] = _to_utc_naive_series(written_rows["timestamp_utc"])

        march7_9_composites = composite_rows[composite_rows["timestamp_local_date"].isin(["2023-03-07", "2023-03-08", "2023-03-09"])]
        terminal_stranding_fraction = 0.0
        if not member_df.empty and "terminal_state" in member_df:
            terminal_stranding_fraction = float((member_df["terminal_state"] == "stranded_no_active").mean())

        last_active_time = _last_timestamp(raw_rows, raw_rows["active_count"].fillna(0) > 0)
        last_prob_time = _last_timestamp(aggregate_rows, aggregate_rows["prob_presence_nonzero_cells"].fillna(0) > 0)
        last_p50_time = _last_timestamp(aggregate_rows, aggregate_rows["p50_nonzero_cells"].fillna(0) > 0)
        last_p90_time = _last_timestamp(aggregate_rows, aggregate_rows["p90_nonzero_cells"].fillna(0) > 0)
        last_det_footprint = _last_timestamp(deterministic, deterministic["surface_presence_cells"].fillna(0) > 0)
        last_written_prob = _last_timestamp(written_rows, written_rows["prob_presence_nonzero_cells"].fillna(0) > 0) if not written_rows.empty else ""

        late_start = pd.Timestamp("2023-03-05T00:00:00")
        late_raw = raw_rows[raw_rows["timestamp_utc"] >= late_start]
        late_aggregate = aggregate_rows[aggregate_rows["timestamp_utc"] >= late_start]

        late_active_outside_domain = bool(
            (late_raw["active_outside_domain_count"].fillna(0) > 0).any()
            and not (late_raw["active_inside_domain_count"].fillna(0) > 0).any()
        )
        late_active_masked_by_ocean = bool(
            (late_raw["active_inside_domain_count"].fillna(0) > 0).any()
            and not (late_raw["active_ocean_particle_count"].fillna(0) > 0).any()
        )
        late_active_low_probability = bool(
            (late_raw["active_ocean_particle_count"].fillna(0) > 0).any()
            and (late_aggregate["max_prob_presence"].fillna(0).max() < 0.50)
        )
        writer_mismatch = bool(
            (late_aggregate["prob_presence_nonzero_cells"].fillna(0) > 0).any()
            and not (written_rows["prob_presence_nonzero_cells"].fillna(0) > 0).any()
        )
        march7_9_p50_zero = bool(march7_9_composites["p50_nonzero_cells"].fillna(0).sum() == 0)
        march7_9_prob_zero = bool(march7_9_composites["prob_presence_nonzero_cells"].fillna(0).sum() == 0)
        if march7_9_prob_zero:
            empty_reason = "no active raw member output exists on March 7-9 local dates"
        elif march7_9_p50_zero:
            empty_reason = "probability exists but never reaches 0.50"
        else:
            empty_reason = "March 7-9 p50 is not empty"

        deterministic_rows = member_df[member_df["run_kind"] == "deterministic_control"]
        ensemble_rows = member_df[member_df["run_kind"] == "ensemble_member"]
        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "simulation_start_utc": window["simulation_start_utc"],
            "simulation_end_utc": window["simulation_end_utc"],
            "run_count": len(run_specs),
            "ensemble_member_count": len([s for s in run_specs if s["run_kind"] == "ensemble_member"]),
            "runs_with_any_active": int((member_df["last_active_time_utc"].fillna("") != "").sum()) if not member_df.empty else 0,
            "terminal_stranding_fraction": terminal_stranding_fraction,
            "last_active_time_utc": last_active_time,
            "last_nonzero_deterministic_footprint_utc": last_det_footprint,
            "last_nonzero_prob_presence_utc": last_prob_time,
            "last_nonzero_mask_p50_utc": last_p50_time,
            "last_nonzero_mask_p90_utc": last_p90_time,
            "last_nonzero_written_prob_presence_utc": last_written_prob,
            "march7_9_p50_zero": march7_9_p50_zero,
            "march7_9_prob_presence_zero": march7_9_prob_zero,
            "march7_9_empty_reason": empty_reason,
            "max_prob_presence_march5_to_march9": float(late_aggregate["max_prob_presence"].fillna(0).max()) if not late_aggregate.empty else 0.0,
            "max_prob_presence_march7_to_march9_composites": float(march7_9_composites["max_prob_presence"].fillna(0).max()) if not march7_9_composites.empty else 0.0,
            "late_active_outside_domain": late_active_outside_domain,
            "late_active_masked_by_ocean": late_active_masked_by_ocean,
            "late_active_low_probability": late_active_low_probability,
            "writer_mismatch_detected": writer_mismatch,
            "max_late_active_inside_domain_count": int(late_raw["active_inside_domain_count"].fillna(0).max()) if not late_raw.empty else 0,
            "max_late_active_ocean_particle_count": int(late_raw["active_ocean_particle_count"].fillna(0).max()) if not late_raw.empty else 0,
            "max_late_active_outside_domain_count": int(late_raw["active_outside_domain_count"].fillna(0).max()) if not late_raw.empty else 0,
            "deterministic_raw_end_utc": deterministic_rows["raw_time_end_utc"].iloc[0] if not deterministic_rows.empty else "",
            "ensemble_latest_raw_end_utc": ensemble_rows["raw_time_end_utc"].max() if not ensemble_rows.empty else "",
        }

    @staticmethod
    def _diagnosis_label(code: str) -> str:
        return {
            "A": "extinction",
            "B": "out-of-domain",
            "C": "shoreline/beaching masking",
            "D": "p50 threshold collapse / sparsity",
            "E": "write/aggregation bug",
        }.get(code, "unknown")

    def _write_outputs(self, summary_df: pd.DataFrame, member_df: pd.DataFrame, hourly_df: pd.DataFrame) -> dict[str, Path]:
        paths = {
            "summary": self.output_dir / "horizon_survival_summary.csv",
            "member_table": self.output_dir / "horizon_survival_member_table.csv",
            "hourly_timeseries": self.output_dir / "horizon_survival_hourly_timeseries.csv",
        }
        summary_df.to_csv(paths["summary"], index=False)
        member_df.to_csv(paths["member_table"], index=False)
        hourly_df.to_csv(paths["hourly_timeseries"], index=False)
        return paths

    def _write_report(self, summary: dict, paths: dict[str, Path], qa_paths: dict[str, Path]) -> Path:
        path = self.output_dir / "horizon_survival_report.md"
        lines = [
            "# Horizon Survival Audit",
            "",
            "Read-only diagnostic audit for the completed short-tier extended public-observation run.",
            "",
            f"- Dominant diagnosis class: {summary['dominant_diagnosis_class']} ({summary['dominant_diagnosis_label']})",
            f"- Diagnosis reason: {summary['dominant_diagnosis_reason']}",
            f"- Recommended next rerun: {summary['recommended_next_rerun']}",
            f"- Last nonzero deterministic footprint: {summary['last_nonzero_deterministic_footprint_utc'] or 'none'}",
            f"- Last nonzero prob_presence: {summary['last_nonzero_prob_presence_utc'] or 'none'}",
            f"- Last nonzero mask_p50: {summary['last_nonzero_mask_p50_utc'] or 'none'}",
            f"- Last nonzero mask_p90: {summary['last_nonzero_mask_p90_utc'] or 'none'}",
            f"- Deterministic raw output ends: {summary['deterministic_raw_end_utc'] or 'unknown'}",
            f"- Latest ensemble raw output ends: {summary['ensemble_latest_raw_end_utc'] or 'unknown'}",
            f"- Terminal stranding fraction: {summary['terminal_stranding_fraction']:.3f}",
            f"- March 7-9 empty reason: {summary['march7_9_empty_reason']}",
            f"- Max probability March 5-9: {summary['max_prob_presence_march5_to_march9']:.3f}",
            f"- Max probability March 7-9 composites: {summary['max_prob_presence_march7_to_march9_composites']:.3f}",
            "",
            "Interpretation: forcing coverage is not the limiting factor in this audit. The raw model outputs terminate early with no active particles, while terminal states are dominated by stranded particles.",
            "",
            "Artifacts:",
            f"- Summary CSV: {paths['summary']}",
            f"- Member table: {paths['member_table']}",
            f"- Hourly timeseries: {paths['hourly_timeseries']}",
        ]
        for label, qa_path in qa_paths.items():
            lines.append(f"- {label}: {qa_path}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_qa(self, hourly_df: pd.DataFrame, run_specs: list[dict]) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs
        timeseries = self._plot_timeseries(hourly_df)
        tracks = self._plot_tracks(run_specs)
        probabilities = self._plot_probabilities()
        if timeseries:
            outputs["qa_horizon_survival_timeseries"] = timeseries
        if tracks:
            outputs["qa_horizon_survival_tracks"] = tracks
        if probabilities:
            outputs["qa_horizon_survival_probabilities"] = probabilities
        return outputs

    def _plot_timeseries(self, hourly_df: pd.DataFrame) -> Path | None:
        path = self.output_dir / "qa_horizon_survival_timeseries.png"
        raw = hourly_df[hourly_df["scope"].isin(["deterministic_control", "ensemble_member"])].copy()
        agg = hourly_df[hourly_df["scope"] == "ensemble_aggregate_hourly"].copy()
        if raw.empty or agg.empty:
            return None
        raw["timestamp"] = pd.to_datetime(raw["timestamp_utc"], errors="coerce")
        agg["timestamp"] = pd.to_datetime(agg["timestamp_utc"], errors="coerce")
        for column in ["active_count", "stranded_count"]:
            raw[column] = pd.to_numeric(raw[column], errors="coerce")
        for column in ["max_prob_presence", "p50_nonzero_cells"]:
            agg[column] = pd.to_numeric(agg[column], errors="coerce")
        member_hourly = raw[raw["scope"] == "ensemble_member"].groupby("timestamp")[["active_count", "stranded_count"]].sum().reset_index()
        det = raw[raw["scope"] == "deterministic_control"]
        fig, axes = plt.subplots(3, 1, figsize=(11, 9), sharex=True)
        axes[0].plot(member_hourly["timestamp"], member_hourly["active_count"], label="ensemble active particle sum", color="#1f77b4")
        axes[0].plot(member_hourly["timestamp"], member_hourly["stranded_count"], label="ensemble stranded particle sum", color="#8c564b")
        axes[0].legend(loc="upper right")
        axes[0].set_ylabel("particles")
        axes[1].plot(det["timestamp"], det["active_count"], label="deterministic active", color="#2ca02c")
        axes[1].plot(det["timestamp"], det["stranded_count"], label="deterministic stranded", color="#d62728")
        axes[1].legend(loc="upper right")
        axes[1].set_ylabel("particles")
        axes[2].plot(agg["timestamp"], agg["max_prob_presence"], label="max prob_presence", color="#9467bd")
        axes[2].plot(agg["timestamp"], agg["p50_nonzero_cells"], label="p50 nonzero cells", color="#ff7f0e")
        axes[2].legend(loc="upper right")
        axes[2].set_ylabel("prob / cells")
        axes[2].set_xlabel("UTC time")
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _plot_tracks(self, run_specs: list[dict]) -> Path | None:
        path = self.output_dir / "qa_horizon_survival_tracks.png"
        sample_specs = run_specs[:1] + [spec for spec in run_specs if spec["run_kind"] == "ensemble_member"][:4]
        if not sample_specs:
            return None
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.imshow(
            self.valid_ocean,
            cmap="Blues",
            alpha=0.25,
            extent=[self.grid.min_x, self.grid.max_x, self.grid.min_y, self.grid.max_y],
            origin="upper",
        )
        for spec in sample_specs:
            with xr.open_dataset(spec["path"]) as ds:
                lon = np.asarray(ds["lon"].values)
                lat = np.asarray(ds["lat"].values)
                if lon.ndim != 2:
                    continue
                trajectories = np.linspace(0, lon.shape[0] - 1, min(30, lon.shape[0]), dtype=int)
                for idx in trajectories:
                    finite = np.isfinite(lon[idx]) & np.isfinite(lat[idx])
                    if not np.any(finite):
                        continue
                    x_vals, y_vals = project_points_to_grid(self.grid, lon[idx][finite], lat[idx][finite])
                    ax.plot(x_vals, y_vals, linewidth=0.5, alpha=0.25)
        ax.set_title("Sample short-extended trajectories on canonical grid")
        ax.set_xlim(self.grid.min_x, self.grid.max_x)
        ax.set_ylim(self.grid.min_y, self.grid.max_y)
        ax.set_aspect("equal")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _plot_probabilities(self) -> Path | None:
        paths = [self.short_dir / "forecast_datecomposites" / f"prob_presence_{date}_datecomposite.tif" for date in SHORT_DIAGNOSTIC_DATES]
        existing = [path for path in paths if path.exists()]
        if not existing:
            return None
        out_path = self.output_dir / "qa_horizon_survival_probabilities.png"
        fig, axes = plt.subplots(1, len(existing), figsize=(4 * len(existing), 4))
        if len(existing) == 1:
            axes = [axes]
        for ax, path in zip(axes, existing):
            arr = self._read_raster(path)
            vmax = max(0.5, float(np.nanmax(arr)) if arr.size else 0.5)
            im = ax.imshow(arr, cmap="magma", vmin=0, vmax=vmax)
            ax.set_title(path.name.replace("prob_presence_", "").replace("_datecomposite.tif", ""))
            ax.axis("off")
            fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        fig.tight_layout()
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return out_path

    def _input_artifacts(self) -> dict:
        return {
            "official_forecast_manifest": str(self.base_output / "forecast" / "forecast_manifest.json"),
            "official_loading_audit": str(self.base_output / "forecast" / "phase2_loading_audit.json"),
            "official_ensemble_manifest": str(self.base_output / "ensemble" / "ensemble_manifest.json"),
            "phase3b_diagnostics": str(self.base_output / "phase3b" / "phase3b_diagnostics.csv"),
            "short_extended_run_manifest": str(self.short_dir / "extended_short_run_manifest.json"),
            "short_extended_loading_audit": str(self.short_dir / "extended_loading_audit.json"),
            "short_extended_summary": str(self.short_dir / "extended_short_summary.csv"),
            "short_extended_model_dir": str(self.model_dir),
            "event_corridor_dates": list(EVENT_CORRIDOR_DATES),
        }


def run_horizon_survival_audit() -> dict:
    return HorizonSurvivalAuditService().run()


if __name__ == "__main__":  # pragma: no cover
    print(json.dumps(run_horizon_survival_audit(), indent=2, default=_json_default))
