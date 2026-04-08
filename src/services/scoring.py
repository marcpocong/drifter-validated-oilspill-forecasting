"""
Generic Phase 3B observation-based validation.

This single service owns the full Phase 3B flow:
ArcGIS layer loading → observation rasterization → forecast pairing → FSS scoring.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.scoring import precheck_same_grid
from src.helpers.raster import GridBuilder
from src.utils.io import (
    get_forecast_manifest_path,
    get_phase3b_forecast_candidates,
    resolve_recipe_selection,
)

try:
    import geopandas as gpd
except ImportError:
    gpd = None


@dataclass(frozen=True)
class Phase3BArtifacts:
    obs_registry: Path
    pairing_manifest: Path
    fss_by_date_window: Path
    summary: Path
    run_manifest: Path


class Phase3BScoringService:
    def __init__(self, output_dir: str | Path | None = None):
        self.output_dir = Path(output_dir) if output_dir else Path("output") / "phase3b"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.grid = GridBuilder()

    def run(self) -> Phase3BArtifacts:
        from src.core.constants import RUN_NAME
        obs_registry = self._build_obs_registry(RUN_NAME)
        pairing_manifest = self._build_pairing_manifest(obs_registry, RUN_NAME)
        fss_df = self._score_pairings(pairing_manifest)
        summary_df = self._summarize(fss_df)

        obs_registry_path = self.output_dir / "phase3b_obs_registry.csv"
        pairing_manifest_path = self.output_dir / "phase3b_pairing_manifest.csv"
        fss_path = self.output_dir / "phase3b_fss_by_date_window.csv"
        summary_path = self.output_dir / "phase3b_summary.csv"
        run_manifest_path = self.output_dir / "phase3b_run_manifest.json"

        obs_registry.to_csv(obs_registry_path, index=False)
        pairing_manifest.to_csv(pairing_manifest_path, index=False)
        fss_df.to_csv(fss_path, index=False)
        summary_df.to_csv(summary_path, index=False)
        self._write_run_manifest(run_manifest_path, RUN_NAME, pairing_manifest)

        return Phase3BArtifacts(obs_registry_path, pairing_manifest_path, fss_path, summary_path, run_manifest_path)

    def _build_obs_registry(self, run_name: str) -> pd.DataFrame:
        case = get_case_context()
        registry_path = Path(f"data/arcgis/{run_name}/arcgis_registry.csv")
        if not registry_path.exists():
            raise FileNotFoundError(f"ArcGIS registry not found: {registry_path}")
        df = pd.read_csv(registry_path)
        records = []
        for _, row in df.iterrows():
            mask_path = Path(row["mask"])
            if not mask_path.exists():
                raise FileNotFoundError(f"ArcGIS mask not found: {mask_path}")
            
            row_role = row.get("role")
            row_layer_id = int(row["layer_id"])
            if row_role:
                is_validation_layer = str(row_role) == case.validation_layer.role
            else:
                is_validation_layer = row_layer_id == case.validation_layer.layer_id
            if not is_validation_layer:
                continue

            records.append({
                "layer_id": row_layer_id,
                "name": row["name"],
                "feature_count": int(row["feature_count"]),
                "mask_path": str(mask_path),
                "mask_sum": float(self._read_mask(mask_path).sum()),
                "validation_time_utc": row.get("event_time_utc", case.validation_layer.event_time_utc or ""),
            })
        return pd.DataFrame(records)

    def _build_pairing_manifest(self, obs_registry: pd.DataFrame, run_name: str) -> pd.DataFrame:
        # FSS pairings are only executed on the current RUN_NAME case.
        case = get_case_context()
        case_dir = Path("output") / run_name
        if not case_dir.exists():
            raise FileNotFoundError(f"Expected case directory not found: {case_dir}")

        candidate_outputs = []
        selection = resolve_recipe_selection(case_dir / "validation" / "validation_ranking.csv")
        forecast_manifest_path = get_forecast_manifest_path(run_name)
        forecast_manifest = {}
        if case.is_official and forecast_manifest_path.exists():
            with open(forecast_manifest_path, "r") as f:
                forecast_manifest = json.load(f) or {}

        for candidate in get_phase3b_forecast_candidates(selection.recipe, run_name):
            candidate_path = Path(candidate["path"])
            if candidate_path.exists():
                candidate_outputs.append(
                    {
                        "type": candidate["type"],
                        "path": str(candidate_path),
                    }
                )

        if not candidate_outputs:
            raise FileNotFoundError(
                f"No Phase 3B forecast outputs are available under {case_dir}. "
                f"Expected ensemble probability or deterministic spill forecast artifacts."
            )
        
        records = []
        for _, row in obs_registry.iterrows():
            for output in candidate_outputs:
                records.append({
                    "layer_id": row["layer_id"],
                    "name": row["name"],
                    "mask_path": row["mask_path"],
                    "output_case": run_name,
                    "output_type": output["type"],
                    "output_path": output["path"],
                    "pairing_time_utc": row.get("validation_time_utc", ""),
                    "recipe": selection.recipe,
                    "selection_source_kind": selection.source_kind,
                    "selection_source_path": selection.source_path or "",
                    "selection_status_flag": selection.status_flag,
                    "valid": selection.valid,
                    "provisional": selection.provisional,
                    "rerun_required": selection.rerun_required,
                    "selection_note": selection.note,
                    "forecast_manifest_path": str(forecast_manifest_path) if case.is_official else "",
                    "forecast_manifest_status_flag": str(
                        ((forecast_manifest.get("recipe_selection") or {}).get("status_flag")) if forecast_manifest else ""
                    ),
                })
        return pd.DataFrame(records)

    def _score_pairings(self, pairing_manifest: pd.DataFrame) -> pd.DataFrame:
        case = get_case_context()
        windows_km = [1, 3, 5, 10]
        rows = []
        precheck_dir = self.output_dir / "same_grid_precheck"
        for _, row in pairing_manifest.iterrows():
            obs_mask_path = Path(row["mask_path"])
            forecast_path = Path(row["output_path"])

            precheck_csv = ""
            precheck_json = ""
            if case.is_official:
                if forecast_path.suffix.lower() != ".tif":
                    raise RuntimeError(
                        "Official Phase 3B scoring requires canonical raster forecasts on the scoring grid. "
                        f"Unsupported official forecast artifact: {forecast_path}"
                    )
                report_base = precheck_dir / (
                    f"layer_{int(row['layer_id'])}__{forecast_path.stem}"
                )
                precheck = precheck_same_grid(
                    forecast=forecast_path,
                    target=obs_mask_path,
                    report_base_path=report_base,
                )
                precheck_csv = str(precheck.csv_report_path)
                precheck_json = str(precheck.json_report_path)
                if not precheck.passed:
                    raise RuntimeError(
                        "Phase 3B same-grid precheck failed. "
                        f"Forecast: {forecast_path} | Target: {obs_mask_path} | "
                        f"CSV: {precheck.csv_report_path} | JSON: {precheck.json_report_path}"
                    )

            obs_mask = self._read_mask(obs_mask_path)
            fc_mask = self._load_forecast_mask(forecast_path)
            for window_km in windows_km:
                fss = calculate_fss(fc_mask, obs_mask, window=max(1, int(window_km)))
                rows.append({
                    "layer_id": row["layer_id"],
                    "name": row["name"],
                    "output_case": row["output_case"],
                    "output_type": row["output_type"],
                    "pairing_time_utc": row["pairing_time_utc"],
                    "recipe": row["recipe"],
                    "selection_source_kind": row["selection_source_kind"],
                    "selection_status_flag": row["selection_status_flag"],
                    "valid": row["valid"],
                    "provisional": row["provisional"],
                    "rerun_required": row["rerun_required"],
                    "window_km": window_km,
                    "fss": fss,
                    "precheck_csv": precheck_csv,
                    "precheck_json": precheck_json,
                })
        return pd.DataFrame(rows)

    def _summarize(self, fss_df: pd.DataFrame) -> pd.DataFrame:
        if fss_df.empty:
            return pd.DataFrame()
        summary = (
            fss_df.groupby(["name", "output_type", "window_km"], as_index=False)
            .agg(mean_fss=("fss", "mean"), min_fss=("fss", "min"), max_fss=("fss", "max"))
        )
        meta_cols = [
            "name",
            "output_type",
            "recipe",
            "selection_source_kind",
            "selection_status_flag",
            "valid",
            "provisional",
            "rerun_required",
        ]
        meta = fss_df[meta_cols].drop_duplicates(subset=["name", "output_type"])
        return summary.merge(meta, on=["name", "output_type"], how="left")

    def _write_run_manifest(self, path: Path, run_name: str, pairing_manifest: pd.DataFrame):
        """Write a compact Phase 3B run manifest including selection status."""
        records = []
        for _, row in pairing_manifest.drop_duplicates(subset=["output_type", "output_path"]).iterrows():
            records.append(
                {
                    "output_type": row["output_type"],
                    "output_path": row["output_path"],
                }
            )

        selection = {}
        if not pairing_manifest.empty:
            first = pairing_manifest.iloc[0]
            selection = {
                "recipe": first["recipe"],
                "source_kind": first["selection_source_kind"],
                "source_path": first.get("selection_source_path", ""),
                "status_flag": first["selection_status_flag"],
                "valid": bool(first["valid"]),
                "provisional": bool(first["provisional"]),
                "rerun_required": bool(first["rerun_required"]),
                "note": first.get("selection_note", ""),
            }

        payload = {
            "run_name": run_name,
            "workflow_mode": get_case_context().workflow_mode,
            "recipe_selection": selection,
            "candidate_outputs": records,
            "status_flags": {
                "valid": bool(selection.get("valid", False)),
                "provisional": bool(selection.get("provisional", False)),
                "rerun_required": bool(selection.get("rerun_required", False)),
            },
        }
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)

    def _read_mask(self, path: Path) -> np.ndarray:
        import rasterio
        with rasterio.open(path) as src:
            return src.read(1).astype(np.float32)

    def _load_forecast_mask(self, path: Path) -> np.ndarray:
        import rasterio
        import xarray as xr
        if path.suffix.lower() == ".tif":
            with rasterio.open(path) as src:
                return src.read(1).astype(np.float32)
        if path.suffix.lower() == ".nc":
            with xr.open_dataset(path) as ds:
                if "probability" in ds.data_vars or "probability_density" in ds.data_vars:
                    # It's an ensemble probability grid
                    var_name = "probability" if "probability" in ds.data_vars else "probability_density"
                    prob_data = ds[var_name].values
                    if prob_data.ndim == 3: # (time, lat, lon) or similar
                         prob_data = prob_data[0] # Take first snapshot usually 72h is a single snapshot
                    # Return normalized mask (1 where prob > 0, 0 otherwise)
                    return (prob_data > 0).astype(np.float32)
                
            # It's a deterministic particle track (fallback)
            from src.helpers.raster import extract_particles_at_hour, rasterize_particles
            lon, lat, mass = extract_particles_at_hour(path, 72, "opendrift")
            hits, _ = rasterize_particles(self.grid, lon, lat, mass)
            return hits.astype(np.float32)
        raise ValueError(f"Unsupported forecast output: {path}")


def run_phase3b_scoring(output_dir: str | Path | None = None):
    service = Phase3BScoringService(output_dir=output_dir)
    return service.run()
