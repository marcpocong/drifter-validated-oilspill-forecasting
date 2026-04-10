"""DWH Phase 3C forcing-adapter status and non-scientific smoke forecast."""

from __future__ import annotations

import csv
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr
import yaml

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.scoring import GEOGRAPHIC_CRS, ScoringGridSpec
from src.services.phase3c_external_case_setup import (
    PHASE3C_NAME,
    build_external_case_forcing_manifest,
)
from src.utils.io import find_current_vars, find_wave_vars, find_wind_vars

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import rasterio
    from rasterio.features import rasterize as rio_rasterize
    from rasterio.transform import from_origin
except ImportError:  # pragma: no cover
    rasterio = None
    rio_rasterize = None
    from_origin = None

try:
    from shapely import affinity
except ImportError:  # pragma: no cover
    affinity = None


SMOKE_PHASE = "dwh_phase3c_forcing_adapter_and_non_scientific_smoke_forecast"
CASE_ID = "CASE_DWH_RETRO_2010_72H"
SETUP_DIR = Path("output") / CASE_ID / "phase3c_external_case_setup"
SMOKE_OUTPUT_DIR = Path("output") / CASE_ID / SMOKE_PHASE
CONFIG_PATH = Path("config") / "case_dwh_retro_2010_72h.yaml"
REAL_FORCING_ENV = "DWH_ENABLE_REAL_FORCING_DOWNLOAD"
HALO_DEGREES = 0.5
SMOKE_RECOMMENDATION_BLOCKED = (
    "adapter work still incomplete: HYCOM GOFS 3.1 reanalysis adapter/download for the 2010 DWH window is not scientific-ready"
)
SMOKE_RECOMMENDATION_READY = "ready for first full scientific DWH forecast/scoring run"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_dwh_scoring_grid_spec(path: str | Path = SETUP_DIR / "scoring_grid.yaml") -> ScoringGridSpec:
    data = _load_yaml(Path(path))
    allowed = set(ScoringGridSpec.__dataclass_fields__.keys())
    return ScoringGridSpec(**{key: value for key, value in data.items() if key in allowed})


def derive_forcing_bbox_from_grid(spec: ScoringGridSpec, halo_degrees: float = HALO_DEGREES) -> list[float]:
    if not spec.display_bounds_wgs84 or len(spec.display_bounds_wgs84) != 4:
        raise ValueError("DWH scoring grid metadata must include display_bounds_wgs84.")
    min_lon, max_lon, min_lat, max_lat = [float(value) for value in spec.display_bounds_wgs84]
    halo = float(halo_degrees)
    return [min_lon - halo, max_lon + halo, min_lat - halo, max_lat + halo]


def _timestamp(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        return ts.tz_convert("UTC").tz_localize(None)
    return ts


def _timestamp_iso(value: str | pd.Timestamp) -> str:
    return _timestamp(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _timestamp_label(value: str | pd.Timestamp) -> str:
    return _timestamp(value).strftime("%Y-%m-%dT%H-%M-%SZ")


def _time_values(start_utc: str, end_utc: str) -> pd.DatetimeIndex:
    return pd.date_range(_timestamp(start_utc), _timestamp(end_utc), freq="1h")


def _axis_values(start: float, end: float, step: float = 0.25) -> np.ndarray:
    values = np.arange(float(start), float(end) + step * 0.5, step, dtype=np.float32)
    if values.size < 2:
        values = np.asarray([start, end], dtype=np.float32)
    return values


def write_smoke_forcing_files(
    forcing_dir: str | Path,
    bbox: list[float],
    start_utc: str,
    end_utc: str,
) -> dict[str, Path]:
    """Write explicit non-zero analytic forcing files for smoke-only wiring."""
    forcing_dir = Path(forcing_dir)
    forcing_dir.mkdir(parents=True, exist_ok=True)
    min_lon, max_lon, min_lat, max_lat = [float(value) for value in bbox]
    lon = _axis_values(min_lon, max_lon)
    lat = _axis_values(min_lat, max_lat)
    times = _time_values(start_utc, end_utc)
    shape = (len(times), len(lat), len(lon))

    common_attrs = {
        "non_scientific_smoke": "true",
        "scientific_ready": "false",
        "source": "explicit non-zero analytic smoke forcing generated for DWH pipeline wiring only",
        "zero_field_policy": "not a zero-field fallback",
    }

    outputs: dict[str, Path] = {}
    current_ds = xr.Dataset(
        {
            "water_u": (("time", "lat", "lon"), np.full(shape, 0.04, dtype=np.float32)),
            "water_v": (("time", "lat", "lon"), np.full(shape, 0.015, dtype=np.float32)),
        },
        coords={"time": times.values, "lat": lat, "lon": lon},
        attrs={**common_attrs, "forcing_component": "current"},
    )
    outputs["current"] = forcing_dir / "dwh_smoke_current_non_scientific.nc"
    current_ds.to_netcdf(outputs["current"])

    wind_ds = xr.Dataset(
        {
            "x_wind": (("time", "lat", "lon"), np.full(shape, 4.0, dtype=np.float32)),
            "y_wind": (("time", "lat", "lon"), np.full(shape, 1.0, dtype=np.float32)),
        },
        coords={"time": times.values, "lat": lat, "lon": lon},
        attrs={**common_attrs, "forcing_component": "wind"},
    )
    outputs["wind"] = forcing_dir / "dwh_smoke_wind_non_scientific.nc"
    wind_ds.to_netcdf(outputs["wind"])

    wave_ds = xr.Dataset(
        {
            "VSDX": (("time", "lat", "lon"), np.full(shape, 0.01, dtype=np.float32)),
            "VSDY": (("time", "lat", "lon"), np.full(shape, 0.005, dtype=np.float32)),
            "VHM0": (("time", "lat", "lon"), np.full(shape, 1.0, dtype=np.float32)),
        },
        coords={"time": times.values, "lat": lat, "lon": lon},
        attrs={**common_attrs, "forcing_component": "wave_stokes"},
    )
    outputs["wave"] = forcing_dir / "dwh_smoke_wave_stokes_non_scientific.nc"
    wave_ds.to_netcdf(outputs["wave"])
    return outputs


def inspect_forcing_file(path: str | Path, forcing_kind: str) -> dict:
    path = Path(path)
    result = {
        "file_path": str(path),
        "exists": path.exists(),
        "variable_names_found": "",
        "coverage_start_utc": "",
        "coverage_end_utc": "",
        "adapter_compatibility_status": "missing_file",
        "stop_reason": "",
    }
    if not path.exists():
        result["stop_reason"] = f"file not found: {path}"
        return result

    try:
        with xr.open_dataset(path) as ds:
            result["variable_names_found"] = ";".join(sorted(ds.data_vars))
            if "time" in ds:
                times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
                if len(times) > 0:
                    result["coverage_start_utc"] = _timestamp_iso(times.min())
                    result["coverage_end_utc"] = _timestamp_iso(times.max())
            if forcing_kind == "current":
                required = find_current_vars(ds)
            elif forcing_kind == "wind":
                required = find_wind_vars(ds)
            elif forcing_kind == "wave":
                required = find_wave_vars(ds)
            else:
                raise ValueError(f"unsupported forcing kind: {forcing_kind}")
            result["adapter_compatibility_status"] = "reader_variable_compatible"
            result["required_variables_found"] = ";".join(required)
    except Exception as exc:
        result["adapter_compatibility_status"] = "inspect_failed"
        result["stop_reason"] = f"{type(exc).__name__}: {exc}"
    return result


def _load_raster(path: Path) -> tuple[np.ndarray, dict]:
    if rasterio is None:
        raise ImportError("rasterio is required for DWH smoke rasters")
    with rasterio.open(path) as src:
        data = src.read(1).astype(np.float32)
        profile = {
            "crs": src.crs.to_string() if src.crs else "",
            "transform": src.transform,
            "width": src.width,
            "height": src.height,
            "res": src.res,
            "dtype": src.dtypes[0],
        }
    return data, profile


def _write_raster(spec: ScoringGridSpec, data: np.ndarray, path: Path, dtype: str = "float32") -> None:
    if rasterio is None or from_origin is None:
        raise ImportError("rasterio is required for DWH smoke rasters")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    transform = from_origin(spec.min_x, spec.max_y, spec.resolution, spec.resolution)
    arr = np.asarray(data, dtype=dtype)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=spec.height,
        width=spec.width,
        count=1,
        dtype=arr.dtype,
        crs=spec.crs,
        transform=transform,
        compress="lzw",
    ) as dst:
        dst.write(arr, 1)


def _rasterize_gdf(gdf: "gpd.GeoDataFrame", spec: ScoringGridSpec) -> np.ndarray:
    if rio_rasterize is None or from_origin is None:
        raise ImportError("rasterio is required to rasterize DWH smoke geometry")
    work = gdf if str(gdf.crs).upper() == str(spec.crs).upper() else gdf.to_crs(spec.crs)
    valid = work.dropna(subset=["geometry"])
    shapes = [(geom, 1.0) for geom in valid.geometry if geom is not None and not geom.is_empty]
    if not shapes:
        return np.zeros((spec.height, spec.width), dtype=np.float32)
    transform = from_origin(spec.min_x, spec.max_y, spec.resolution, spec.resolution)
    return rio_rasterize(
        shapes,
        out_shape=(spec.height, spec.width),
        transform=transform,
        fill=0.0,
        dtype=np.float32,
        all_touched=True,
    ).astype(np.float32)


def _apply_sea_mask(mask: np.ndarray, sea_mask: np.ndarray | None) -> np.ndarray:
    arr = np.asarray(mask, dtype=np.float32)
    if sea_mask is None:
        return arr
    return np.where(np.asarray(sea_mask) > 0.5, arr, 0.0).astype(np.float32)


def _same_grid_precheck(left: Path, right: Path) -> dict:
    _, left_profile = _load_raster(left)
    _, right_profile = _load_raster(right)
    checks = {
        "crs_match": left_profile["crs"] == right_profile["crs"],
        "width_match": left_profile["width"] == right_profile["width"],
        "height_match": left_profile["height"] == right_profile["height"],
        "resolution_match": tuple(left_profile["res"]) == tuple(right_profile["res"]),
        "transform_match": tuple(left_profile["transform"]) == tuple(right_profile["transform"]),
    }
    return {"passed": all(checks.values()), "checks": checks}


def _normalize_density(mask: np.ndarray) -> np.ndarray:
    arr = np.asarray(mask, dtype=np.float32)
    total = float(arr.sum())
    if total <= 0.0:
        return arr
    return (arr / total).astype(np.float32)


def _window_cells(window_km: int, spec: ScoringGridSpec) -> int:
    return max(1, int(round((float(window_km) * 1000.0) / float(spec.resolution))))


def _mask_diagnostics(forecast: np.ndarray, observed: np.ndarray, spec: ScoringGridSpec) -> dict:
    forecast_active = np.asarray(forecast) > 0
    observed_active = np.asarray(observed) > 0
    intersection = int(np.count_nonzero(forecast_active & observed_active))
    union = int(np.count_nonzero(forecast_active | observed_active))
    forecast_count = int(np.count_nonzero(forecast_active))
    observed_count = int(np.count_nonzero(observed_active))
    dice_den = forecast_count + observed_count
    return {
        "forecast_nonzero_cells": forecast_count,
        "obs_nonzero_cells": observed_count,
        "intersection_cells": intersection,
        "union_cells": union,
        "iou": 0.0 if union == 0 else float(intersection / union),
        "dice": 0.0 if dice_den == 0 else float((2 * intersection) / dice_den),
        "forecast_area_km2": float(forecast_count * (spec.resolution / 1000.0) ** 2),
        "obs_area_km2": float(observed_count * (spec.resolution / 1000.0) ** 2),
    }


def _mean_forcing_components(paths: dict[str, Path]) -> dict[str, float]:
    values = {
        "current_u_ms": 0.0,
        "current_v_ms": 0.0,
        "wind_u_ms": 0.0,
        "wind_v_ms": 0.0,
        "stokes_u_ms": 0.0,
        "stokes_v_ms": 0.0,
    }
    with xr.open_dataset(paths["current"]) as ds:
        u, v = find_current_vars(ds)
        values["current_u_ms"] = float(ds[u].mean())
        values["current_v_ms"] = float(ds[v].mean())
    with xr.open_dataset(paths["wind"]) as ds:
        u, v = find_wind_vars(ds)
        values["wind_u_ms"] = float(ds[u].mean())
        values["wind_v_ms"] = float(ds[v].mean())
    with xr.open_dataset(paths["wave"]) as ds:
        u, v, _ = find_wave_vars(ds)
        values["stokes_u_ms"] = float(ds[u].mean())
        values["stokes_v_ms"] = float(ds[v].mean())
    return values


class DWHPhase3CSmokeService:
    def __init__(
        self,
        *,
        config_path: str | Path = CONFIG_PATH,
        setup_dir: str | Path = SETUP_DIR,
        output_dir: str | Path = SMOKE_OUTPUT_DIR,
    ):
        self.case = get_case_context()
        if self.case.workflow_mode != "dwh_retro_2010":
            raise RuntimeError(f"{SMOKE_PHASE} requires WORKFLOW_MODE=dwh_retro_2010.")
        self.config_path = Path(config_path)
        self.setup_dir = Path(setup_dir)
        self.output_dir = Path(output_dir)
        self.forcing_dir = self.output_dir / "prepared_forcing"
        self.cfg = _load_yaml(self.config_path)
        self.smoke_cfg = self.cfg.get("smoke_forecast") or {}
        self.spec = load_dwh_scoring_grid_spec(self.setup_dir / "scoring_grid.yaml")
        self.bbox = derive_forcing_bbox_from_grid(self.spec, halo_degrees=HALO_DEGREES)
        self.start_utc = str(self.smoke_cfg.get("simulation_start_utc", "2010-05-20T00:00:00Z"))
        self.end_utc = str(self.smoke_cfg.get("simulation_end_utc", "2010-05-21T00:00:00Z"))
        self.target_timestamp = _timestamp(self.end_utc)
        self.target_label = _timestamp_label(self.target_timestamp)

    def run(self) -> dict:
        self._assert_setup_artifacts()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.forcing_dir.mkdir(parents=True, exist_ok=True)

        smoke_forcing_files = write_smoke_forcing_files(
            self.forcing_dir,
            self.bbox,
            self.start_utc,
            self.end_utc,
        )
        real_status_rows = self._build_real_adapter_status_rows()
        smoke_status_rows, prepared_rows = self._build_smoke_forcing_rows(smoke_forcing_files)
        adapter_status_rows = real_status_rows + smoke_status_rows

        adapter_status_csv = self.output_dir / "dwh_forcing_adapter_status.csv"
        adapter_status_json = self.output_dir / "dwh_forcing_adapter_status.json"
        prepared_csv = self.output_dir / "dwh_prepared_forcing_manifest.csv"
        prepared_json = self.output_dir / "dwh_prepared_forcing_manifest.json"
        _write_csv(adapter_status_csv, adapter_status_rows)
        _write_json(adapter_status_json, adapter_status_rows)
        _write_csv(prepared_csv, prepared_rows)
        _write_json(prepared_json, prepared_rows)

        smoke_result = self._run_smoke_forecast(smoke_forcing_files)
        score_result = self._score_smoke_if_possible(smoke_result)
        qa_paths = self._write_qa_pack(smoke_result)

        scientific_ready = self._scientific_ready(adapter_status_rows)
        recommendation = SMOKE_RECOMMENDATION_READY if scientific_ready and smoke_result["smoke_forecast_ran"] else self._single_blocker(adapter_status_rows)

        loading_audit_paths = self._write_loading_audit(adapter_status_rows, smoke_result)
        summary_path = self._write_summary(
            adapter_status_rows=adapter_status_rows,
            prepared_rows=prepared_rows,
            smoke_result=smoke_result,
            score_result=score_result,
            scientific_ready=scientific_ready,
            recommendation=recommendation,
        )
        report_path = self._write_report(
            adapter_status_rows=adapter_status_rows,
            smoke_result=smoke_result,
            score_result=score_result,
            recommendation=recommendation,
            qa_paths=qa_paths,
        )
        forecast_manifest_path = self._write_forecast_manifest(
            adapter_status_rows=adapter_status_rows,
            prepared_rows=prepared_rows,
            smoke_result=smoke_result,
            score_result=score_result,
            scientific_ready=scientific_ready,
            recommendation=recommendation,
            qa_paths=qa_paths,
            adapter_status_csv=adapter_status_csv,
            prepared_csv=prepared_csv,
            loading_audit_json=loading_audit_paths["json"],
            summary_csv=summary_path,
            report_md=report_path,
        )

        return {
            "output_dir": self.output_dir,
            "adapter_status_csv": adapter_status_csv,
            "adapter_status_json": adapter_status_json,
            "prepared_forcing_manifest_csv": prepared_csv,
            "prepared_forcing_manifest_json": prepared_json,
            "loading_audit_csv": loading_audit_paths["csv"],
            "loading_audit_json": loading_audit_paths["json"],
            "summary_csv": summary_path,
            "report_md": report_path,
            "forecast_manifest": forecast_manifest_path,
            "selected_sources": self._selected_source_summary(adapter_status_rows),
            "actual_forcing_coverage_start_utc": prepared_rows[0]["coverage_start_utc"] if prepared_rows else "",
            "actual_forcing_coverage_end_utc": prepared_rows[0]["coverage_end_utc"] if prepared_rows else "",
            "waves_attached": smoke_result["waves_attached"],
            "smoke_forecast_ran": smoke_result["smoke_forecast_ran"],
            "smoke_score_produced": bool(score_result.get("score_produced")),
            "scientific_ready": scientific_ready,
            "recommendation": recommendation,
            "single_biggest_blocker": "" if recommendation == SMOKE_RECOMMENDATION_READY else recommendation.replace("adapter work still incomplete: ", ""),
            "smoke_products": smoke_result["products"],
            "score_result": score_result,
        }

    def _assert_setup_artifacts(self) -> None:
        required = [
            self.config_path,
            self.setup_dir / "scoring_grid.yaml",
            self.setup_dir / "sea_mask.tif",
            self.setup_dir / "obs_mask_2010-05-21.tif",
            self.setup_dir / "processed" / "layer_00_dwh_wellhead_processed.gpkg",
            self.setup_dir / "processed" / "layer_05_dwh_t20100520_composite_processed.gpkg",
        ]
        missing = [path for path in required if not path.exists()]
        if missing:
            raise FileNotFoundError(
                "DWH smoke phase requires completed Phase 3C setup artifacts. Missing: "
                + ", ".join(str(path) for path in missing)
            )

    def _build_real_adapter_status_rows(self) -> list[dict]:
        forcing_rows = build_external_case_forcing_manifest(self.cfg)
        download_enabled = os.environ.get(REAL_FORCING_ENV, "").strip() == "1"
        rows: list[dict] = []
        for row in forcing_rows:
            component = str(row["forcing_component"])
            stop_reason = ""
            status = "not_scientific_ready"
            if component == "currents_primary":
                stop_reason = (
                    f"{REAL_FORCING_ENV}=1 was not set; GOFS 3.1 THREDDS subset/download is wired as the target "
                    "but still needs scientific adapter execution and variable/time validation for the 2010 DWH window."
                )
                if download_enabled:
                    stop_reason = (
                        "real HYCOM GOFS 3.1 download execution is intentionally not enabled in this smoke patch; "
                        "the adapter target is recorded for the next scientific branch."
                    )
                    status = "real_download_not_implemented"
            elif component == "winds_primary":
                has_creds = bool(os.environ.get("CDS_URL") and os.environ.get("CDS_KEY"))
                stop_reason = (
                    "CDS_URL/CDS_KEY credentials are missing; ERA5 DWH historical wind download was not attempted."
                    if not has_creds
                    else f"{REAL_FORCING_ENV}=1 was not set; ERA5 adapter target is recorded but not downloaded in smoke mode."
                )
                status = "blocked_missing_credentials" if not has_creds else "not_attempted_default_smoke"
            elif component == "waves_primary":
                has_creds = bool(os.environ.get("CMEMS_USERNAME") and os.environ.get("CMEMS_PASSWORD"))
                stop_reason = (
                    "CMEMS_USERNAME/CMEMS_PASSWORD credentials are missing; historical wave/Stokes download was not attempted."
                    if not has_creds
                    else f"{REAL_FORCING_ENV}=1 was not set; Copernicus wave/Stokes target is recorded but not downloaded in smoke mode."
                )
                status = "blocked_missing_credentials" if not has_creds else "not_attempted_default_smoke"
            elif component == "currents_fallback":
                has_creds = bool(os.environ.get("CMEMS_USERNAME") and os.environ.get("CMEMS_PASSWORD"))
                stop_reason = (
                    "CMEMS_USERNAME/CMEMS_PASSWORD credentials are missing; fallback ocean physics reanalysis download was not attempted."
                    if not has_creds
                    else f"{REAL_FORCING_ENV}=1 was not set; fallback Copernicus currents target is recorded but not downloaded in smoke mode."
                )
                status = "blocked_missing_credentials" if not has_creds else "not_attempted_default_smoke"
            rows.append(
                {
                    "non_scientific_smoke": True,
                    "forcing_component": component,
                    "source_role": "scientific_target",
                    "chosen_service": row["chosen_service"],
                    "service_url": row["service_url"],
                    "access_method": row["access_method"],
                    "file_path": "",
                    "requested_bbox_wgs84": json.dumps(self.bbox),
                    "requested_start_utc": _timestamp_iso(self.start_utc),
                    "requested_end_utc": _timestamp_iso(self.end_utc),
                    "variable_names_found": "",
                    "coverage_start_utc": "",
                    "coverage_end_utc": "",
                    "adapter_compatibility_status": status,
                    "scientific_ready": False,
                    "smoke_only": False,
                    "stop_reason": stop_reason,
                }
            )
        return rows

    def _build_smoke_forcing_rows(self, smoke_forcing_files: dict[str, Path]) -> tuple[list[dict], list[dict]]:
        service_names = {
            "current": "non-scientific analytic smoke current",
            "wind": "non-scientific analytic smoke wind",
            "wave": "non-scientific analytic smoke wave/Stokes",
        }
        access_methods = {
            "current": "generated local NetCDF",
            "wind": "generated local NetCDF",
            "wave": "generated local NetCDF",
        }
        status_rows = []
        prepared_rows = []
        for kind, path in smoke_forcing_files.items():
            inspected = inspect_forcing_file(path, kind)
            row = {
                "non_scientific_smoke": True,
                "forcing_component": kind,
                "source_role": "smoke_forcing",
                "chosen_service": service_names[kind],
                "service_url": "",
                "access_method": access_methods[kind],
                "file_path": str(path),
                "requested_bbox_wgs84": json.dumps(self.bbox),
                "requested_start_utc": _timestamp_iso(self.start_utc),
                "requested_end_utc": _timestamp_iso(self.end_utc),
                "variable_names_found": inspected.get("variable_names_found", ""),
                "coverage_start_utc": inspected.get("coverage_start_utc", ""),
                "coverage_end_utc": inspected.get("coverage_end_utc", ""),
                "adapter_compatibility_status": inspected.get("adapter_compatibility_status", ""),
                "scientific_ready": False,
                "smoke_only": True,
                "stop_reason": inspected.get("stop_reason", "")
                or "explicit non-zero analytic smoke-only forcing; not valid for scientific inference",
            }
            status_rows.append(row)
            prepared_rows.append(
                {
                    **row,
                    "prepared_for_smoke_forecast": True,
                    "zero_field_fallback_used": False,
                    "scientific_ready": False,
                }
            )
        return status_rows, prepared_rows

    def _run_smoke_forecast(self, smoke_forcing_files: dict[str, Path]) -> dict:
        if gpd is None or affinity is None:
            raise ImportError("geopandas and shapely are required for the DWH smoke forecast")

        init_path = self.setup_dir / "processed" / "layer_05_dwh_t20100520_composite_processed.gpkg"
        source_path = self.setup_dir / "processed" / "layer_00_dwh_wellhead_processed.gpkg"
        init_gdf = gpd.read_file(init_path)
        source_gdf = gpd.read_file(source_path)
        if init_gdf.crs is None:
            init_gdf = init_gdf.set_crs(self.spec.crs)
        if source_gdf.crs is None:
            source_gdf = source_gdf.set_crs(self.spec.crs)

        forcing = _mean_forcing_components(smoke_forcing_files)
        duration_seconds = float((_timestamp(self.end_utc) - _timestamp(self.start_utc)).total_seconds())
        wind_drift_fraction = 0.01
        u_total = forcing["current_u_ms"] + forcing["stokes_u_ms"] + (wind_drift_fraction * forcing["wind_u_ms"])
        v_total = forcing["current_v_ms"] + forcing["stokes_v_ms"] + (wind_drift_fraction * forcing["wind_v_ms"])
        dx_m = float(u_total * duration_seconds)
        dy_m = float(v_total * duration_seconds)

        translated = init_gdf.copy()
        translated["geometry"] = translated.geometry.apply(lambda geom: affinity.translate(geom, xoff=dx_m, yoff=dy_m))
        translated_path = self.output_dir / "dwh_smoke_translated_polygon.gpkg"
        if translated_path.exists():
            translated_path.unlink()
        translated.to_file(translated_path, driver="GPKG")

        sea_mask = None
        sea_mask_path = Path(self.spec.sea_mask_path) if self.spec.sea_mask_path else self.setup_dir / "sea_mask.tif"
        if sea_mask_path.exists():
            sea_mask, _ = _load_raster(sea_mask_path)

        footprint = _apply_sea_mask(_rasterize_gdf(translated, self.spec), sea_mask)
        density = _normalize_density(footprint)
        footprint_path = self.output_dir / f"control_footprint_mask_{self.target_label}.tif"
        density_path = self.output_dir / f"control_density_norm_{self.target_label}.tif"
        _write_raster(self.spec, footprint, footprint_path)
        _write_raster(self.spec, density, density_path)

        ensemble_enabled = bool((self.smoke_cfg.get("tiny_ensemble") or {}).get("enabled", True))
        ensemble_products: dict[str, str] = {}
        if ensemble_enabled:
            member_count = int((self.smoke_cfg.get("tiny_ensemble") or {}).get("member_count", 3))
            member_factors = np.linspace(0.85, 1.15, member_count)
            probability = np.zeros_like(footprint, dtype=np.float32)
            for idx, factor in enumerate(member_factors, start=1):
                member = init_gdf.copy()
                member["geometry"] = member.geometry.apply(
                    lambda geom, f=float(factor): affinity.translate(geom, xoff=dx_m * f, yoff=dy_m * f)
                )
                member_mask = _apply_sea_mask(_rasterize_gdf(member, self.spec), sea_mask)
                probability += (member_mask > 0).astype(np.float32)
            probability /= float(member_count)
            p50 = (probability >= 0.5).astype(np.float32)
            prob_path = self.output_dir / f"prob_presence_{self.target_label}.tif"
            p50_path = self.output_dir / f"mask_p50_{self.target_label}.tif"
            _write_raster(self.spec, probability, prob_path)
            _write_raster(self.spec, p50, p50_path)
            ensemble_products = {
                "prob_presence": str(prob_path),
                "mask_p50": str(p50_path),
                "member_count": member_count,
            }

        source_point_wgs84 = source_gdf.to_crs(GEOGRAPHIC_CRS).geometry.iloc[0]
        products = {
            "control_footprint_mask": str(footprint_path),
            "control_density_norm": str(density_path),
            "translated_polygon": str(translated_path),
            **ensemble_products,
        }
        return {
            "non_scientific_smoke": True,
            "smoke_forecast_ran": True,
            "forecast_timestamp_utc": _timestamp_iso(self.target_timestamp),
            "initialization_polygon_path": str(init_path),
            "provenance_source_point_path": str(source_path),
            "provenance_source_point_lon": float(source_point_wgs84.x),
            "provenance_source_point_lat": float(source_point_wgs84.y),
            "element_count": int(self.smoke_cfg.get("element_count", 250)),
            "duration_hours": float(duration_seconds / 3600.0),
            "displacement_dx_m": dx_m,
            "displacement_dy_m": dy_m,
            "smoke_transport_velocity_u_ms": u_total,
            "smoke_transport_velocity_v_ms": v_total,
            "current_mean_u_ms": forcing["current_u_ms"],
            "current_mean_v_ms": forcing["current_v_ms"],
            "wind_mean_u_ms": forcing["wind_u_ms"],
            "wind_mean_v_ms": forcing["wind_v_ms"],
            "stokes_mean_u_ms": forcing["stokes_u_ms"],
            "stokes_mean_v_ms": forcing["stokes_v_ms"],
            "wind_drift_fraction": wind_drift_fraction,
            "currents_attached": True,
            "winds_attached": True,
            "waves_attached": True,
            "provisional_transport_model": True,
            "scientific_ready": False,
            "products": products,
            "grid": {
                "crs": self.spec.crs,
                "width": self.spec.width,
                "height": self.spec.height,
                "resolution_m": self.spec.resolution,
                "metadata_path": self.spec.metadata_path,
            },
            "date_composite_policy": "May 21 comparison uses date-composite logic; no exact sub-daily observation acquisition time is asserted.",
        }

    def _score_smoke_if_possible(self, smoke_result: dict) -> dict:
        forecast_path = Path(smoke_result["products"]["control_footprint_mask"])
        obs_path = self.setup_dir / "obs_mask_2010-05-21.tif"
        if not forecast_path.exists() or not obs_path.exists():
            return {
                "score_produced": False,
                "stop_reason": "forecast or May 21 observation mask missing",
            }

        precheck = _same_grid_precheck(forecast_path, obs_path)
        precheck_json = self.output_dir / "dwh_smoke_same_grid_precheck.json"
        precheck_csv = self.output_dir / "dwh_smoke_same_grid_precheck.csv"
        _write_json(precheck_json, precheck)
        _write_csv(precheck_csv, [{"passed": precheck["passed"], **precheck["checks"]}])
        if not precheck["passed"]:
            return {
                "score_produced": False,
                "stop_reason": "same-grid precheck failed",
                "precheck_json": str(precheck_json),
                "precheck_csv": str(precheck_csv),
            }

        forecast, _ = _load_raster(forecast_path)
        obs, _ = _load_raster(obs_path)
        sea_mask_path = Path(self.spec.sea_mask_path) if self.spec.sea_mask_path else self.setup_dir / "sea_mask.tif"
        sea_mask = _load_raster(sea_mask_path)[0] if sea_mask_path.exists() else None
        valid_mask = (sea_mask > 0.5) if sea_mask is not None else None
        diagnostics = _mask_diagnostics(forecast, obs, self.spec)
        fss_rows = []
        for window_km in (1, 3, 5, 10):
            fss_rows.append(
                {
                    "non_scientific_smoke": True,
                    "window_km": window_km,
                    "window_cells": _window_cells(window_km, self.spec),
                    "fss": calculate_fss(
                        forecast,
                        obs,
                        window=_window_cells(window_km, self.spec),
                        valid_mask=valid_mask,
                    ),
                    **diagnostics,
                }
            )
        score_csv = self.output_dir / "dwh_smoke_score_vs_2010-05-21.csv"
        score_json = self.output_dir / "dwh_smoke_score_vs_2010-05-21.json"
        _write_csv(score_csv, fss_rows)
        _write_json(score_json, fss_rows)
        return {
            "score_produced": True,
            "score_csv": str(score_csv),
            "score_json": str(score_json),
            "precheck_json": str(precheck_json),
            "precheck_csv": str(precheck_csv),
            "diagnostics": diagnostics,
            "fss": fss_rows,
        }

    def _write_qa_pack(self, smoke_result: dict) -> dict[str, str]:
        if plt is None:
            return {}
        qa_paths: dict[str, str] = {}
        forecast_path = Path(smoke_result["products"]["control_footprint_mask"])
        obs_path = self.setup_dir / "obs_mask_2010-05-21.tif"
        try:
            forecast, _ = _load_raster(forecast_path)
            obs, _ = _load_raster(obs_path)
            overlay = np.ones((*forecast.shape, 3), dtype=np.float32)
            overlay[obs > 0] = np.asarray([0.2, 0.45, 0.95], dtype=np.float32)
            overlay[forecast > 0] = np.asarray([0.95, 0.35, 0.2], dtype=np.float32)
            overlay[(forecast > 0) & (obs > 0)] = np.asarray([0.55, 0.2, 0.75], dtype=np.float32)
            fig, ax = plt.subplots(figsize=(7, 5))
            ax.imshow(overlay, origin="upper")
            ax.set_title("DWH smoke forecast vs May 21 observed mask")
            ax.set_axis_off()
            path = self.output_dir / "qa_dwh_smoke_overlay.png"
            fig.tight_layout()
            fig.savefig(path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            qa_paths["qa_dwh_smoke_overlay"] = str(path)
        except Exception:
            pass

        try:
            sea, _ = _load_raster(Path(self.spec.sea_mask_path))
            fig, ax = plt.subplots(figsize=(7, 5))
            ax.imshow(sea, origin="upper", cmap="Blues")
            ax.set_title(f"DWH smoke grid check: {self.spec.crs}, {self.spec.width}x{self.spec.height}")
            ax.set_axis_off()
            path = self.output_dir / "qa_dwh_smoke_grid_check.png"
            fig.tight_layout()
            fig.savefig(path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            qa_paths["qa_dwh_smoke_grid_check"] = str(path)
        except Exception:
            pass

        try:
            init = gpd.read_file(self.setup_dir / "processed" / "layer_05_dwh_t20100520_composite_processed.gpkg").to_crs(GEOGRAPHIC_CRS)
            final = gpd.read_file(smoke_result["products"]["translated_polygon"]).to_crs(GEOGRAPHIC_CRS)
            source = gpd.read_file(self.setup_dir / "processed" / "layer_00_dwh_wellhead_processed.gpkg").to_crs(GEOGRAPHIC_CRS)
            fig, ax = plt.subplots(figsize=(7, 5))
            init.boundary.plot(ax=ax, color="tab:blue", linewidth=0.6, label="init May 20")
            final.boundary.plot(ax=ax, color="tab:red", linewidth=0.6, label="smoke May 21")
            source.plot(ax=ax, color="black", markersize=16, label="wellhead")
            ax.set_title("DWH non-scientific smoke track")
            ax.legend(loc="best")
            path = self.output_dir / "qa_dwh_smoke_track.png"
            fig.tight_layout()
            fig.savefig(path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            qa_paths["qa_dwh_smoke_track"] = str(path)
        except Exception:
            pass
        return qa_paths

    def _write_loading_audit(self, adapter_status_rows: list[dict], smoke_result: dict) -> dict[str, Path]:
        rows = []
        for row in adapter_status_rows:
            if row["source_role"] != "smoke_forcing":
                continue
            rows.append(
                {
                    "non_scientific_smoke": True,
                    "forcing_component": row["forcing_component"],
                    "file_path": row["file_path"],
                    "reader_attach_status": row["adapter_compatibility_status"],
                    "variable_names_found": row["variable_names_found"],
                    "coverage_start_utc": row["coverage_start_utc"],
                    "coverage_end_utc": row["coverage_end_utc"],
                    "scientific_ready": False,
                    "currents_attached": smoke_result["currents_attached"] if row["forcing_component"] == "current" else "",
                    "winds_attached": smoke_result["winds_attached"] if row["forcing_component"] == "wind" else "",
                    "waves_attached": smoke_result["waves_attached"] if row["forcing_component"] == "wave" else "",
                    "stop_reason": row["stop_reason"],
                }
            )
        csv_path = self.output_dir / "dwh_smoke_loading_audit.csv"
        json_path = self.output_dir / "dwh_smoke_loading_audit.json"
        _write_csv(csv_path, rows)
        _write_json(json_path, rows)
        return {"csv": csv_path, "json": json_path}

    def _write_summary(
        self,
        *,
        adapter_status_rows: list[dict],
        prepared_rows: list[dict],
        smoke_result: dict,
        score_result: dict,
        scientific_ready: bool,
        recommendation: str,
    ) -> Path:
        source_summary = self._selected_source_summary(adapter_status_rows)
        first_prepared = prepared_rows[0] if prepared_rows else {}
        best_fss = ""
        if score_result.get("score_produced"):
            best_fss = max(float(row["fss"]) for row in score_result.get("fss", []))
        row = {
            "non_scientific_smoke": True,
            "case_id": CASE_ID,
            "phase": SMOKE_PHASE,
            "current_source": source_summary["current_source"],
            "wind_source": source_summary["wind_source"],
            "wave_source": source_summary["wave_source"],
            "actual_forcing_coverage_start_utc": first_prepared.get("coverage_start_utc", ""),
            "actual_forcing_coverage_end_utc": first_prepared.get("coverage_end_utc", ""),
            "currents_attached": smoke_result["currents_attached"],
            "winds_attached": smoke_result["winds_attached"],
            "waves_attached": smoke_result["waves_attached"],
            "smoke_forecast_ran": smoke_result["smoke_forecast_ran"],
            "smoke_score_vs_may21_produced": bool(score_result.get("score_produced")),
            "best_smoke_fss": best_fss,
            "scientific_ready": scientific_ready,
            "provisional_transport_model": smoke_result["provisional_transport_model"],
            "recommendation": recommendation,
        }
        path = self.output_dir / "dwh_smoke_summary.csv"
        _write_csv(path, [row])
        return path

    def _write_report(
        self,
        *,
        adapter_status_rows: list[dict],
        smoke_result: dict,
        score_result: dict,
        recommendation: str,
        qa_paths: dict[str, str],
    ) -> Path:
        source_summary = self._selected_source_summary(adapter_status_rows)
        lines = [
            "# DWH Phase 3C Non-Scientific Smoke Report",
            "",
            f"- non_scientific_smoke: true",
            f"- case_id: {CASE_ID}",
            f"- phase: {SMOKE_PHASE}",
            f"- setup phase reference: {PHASE3C_NAME}",
            f"- current source target: {source_summary['current_source']}",
            f"- wind source target: {source_summary['wind_source']}",
            f"- wave/Stokes source target: {source_summary['wave_source']}",
            f"- smoke forecast ran: {smoke_result['smoke_forecast_ran']}",
            f"- currents attached: {smoke_result['currents_attached']}",
            f"- winds attached: {smoke_result['winds_attached']}",
            f"- waves attached: {smoke_result['waves_attached']}",
            f"- smoke score vs May 21 produced: {bool(score_result.get('score_produced'))}",
            f"- recommendation: {recommendation}",
            "",
            "This smoke run is a pipeline-wiring proof only. It uses explicit non-zero analytic smoke forcing when scientific forcing adapters are not ready and must not be reported as a scientific forecast result.",
            "",
            "## Smoke Products",
        ]
        for key, value in smoke_result["products"].items():
            lines.append(f"- {key}: `{value}`")
        if score_result.get("score_produced"):
            lines.extend(["", "## Smoke Score", f"- score_csv: `{score_result['score_csv']}`"])
        if qa_paths:
            lines.append("")
            lines.append("## QA")
            for key, value in qa_paths.items():
                lines.append(f"- {key}: `{value}`")
        path = self.output_dir / "dwh_smoke_report.md"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_forecast_manifest(
        self,
        *,
        adapter_status_rows: list[dict],
        prepared_rows: list[dict],
        smoke_result: dict,
        score_result: dict,
        scientific_ready: bool,
        recommendation: str,
        qa_paths: dict[str, str],
        adapter_status_csv: Path,
        prepared_csv: Path,
        loading_audit_json: Path,
        summary_csv: Path,
        report_md: Path,
    ) -> Path:
        path = self.output_dir / "dwh_smoke_forecast_manifest.json"
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "case_id": CASE_ID,
            "workflow_mode": self.case.workflow_mode,
            "pipeline_phase": SMOKE_PHASE,
            "non_scientific_smoke": True,
            "scientific_ready": scientific_ready,
            "provisional_transport_model": smoke_result["provisional_transport_model"],
            "smoke_forecast_ran": smoke_result["smoke_forecast_ran"],
            "smoke_score_vs_may21_produced": bool(score_result.get("score_produced")),
            "recommendation": recommendation,
            "forcing_subset_bbox_wgs84": self.bbox,
            "forcing_adapter_status": adapter_status_rows,
            "prepared_forcing": prepared_rows,
            "smoke_forecast": smoke_result,
            "smoke_score": score_result,
            "qa_paths": qa_paths,
            "artifacts": {
                "adapter_status_csv": str(adapter_status_csv),
                "prepared_forcing_manifest_csv": str(prepared_csv),
                "loading_audit_json": str(loading_audit_json),
                "summary_csv": str(summary_csv),
                "report_md": str(report_md),
            },
        }
        _write_json(path, payload)
        return path

    @staticmethod
    def _scientific_ready(adapter_status_rows: list[dict]) -> bool:
        required_components = {"currents_primary", "winds_primary", "waves_primary"}
        ready = {
            row["forcing_component"]
            for row in adapter_status_rows
            if row.get("source_role") == "scientific_target" and str(row.get("scientific_ready")).lower() == "true"
        }
        return required_components.issubset(ready)

    @staticmethod
    def _single_blocker(adapter_status_rows: list[dict]) -> str:
        priority = ["currents_primary", "winds_primary", "waves_primary", "currents_fallback"]
        by_component = {
            row["forcing_component"]: row
            for row in adapter_status_rows
            if row.get("source_role") == "scientific_target"
        }
        for component in priority:
            row = by_component.get(component)
            if row and str(row.get("scientific_ready")).lower() != "true":
                if component == "currents_primary":
                    return SMOKE_RECOMMENDATION_BLOCKED
                return f"adapter work still incomplete: {row['chosen_service']} is not scientific-ready ({row['stop_reason']})"
        return SMOKE_RECOMMENDATION_BLOCKED

    @staticmethod
    def _selected_source_summary(adapter_status_rows: list[dict]) -> dict[str, str]:
        by_component = {
            row["forcing_component"]: row
            for row in adapter_status_rows
            if row.get("source_role") == "scientific_target"
        }
        return {
            "current_source": (by_component.get("currents_primary") or {}).get("chosen_service", ""),
            "wind_source": (by_component.get("winds_primary") or {}).get("chosen_service", ""),
            "wave_source": (by_component.get("waves_primary") or {}).get("chosen_service", ""),
            "current_status": (by_component.get("currents_primary") or {}).get("adapter_compatibility_status", ""),
            "wind_status": (by_component.get("winds_primary") or {}).get("adapter_compatibility_status", ""),
            "wave_status": (by_component.get("waves_primary") or {}).get("adapter_compatibility_status", ""),
        }


def run_dwh_phase3c_smoke() -> dict:
    return DWHPhase3CSmokeService().run()


if __name__ == "__main__":  # pragma: no cover
    result = run_dwh_phase3c_smoke()
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, default=str))
