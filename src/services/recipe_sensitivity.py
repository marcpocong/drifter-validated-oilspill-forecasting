"""
Official Mindoro event-scale recipe sensitivities for Phase 3B.

These runs keep the official primary scoring semantics unchanged while testing
whether the March 6 displacement responds to forcing choice. Results are
written under output/CASE_MINDORO_RETRO_2023/recipe_sensitivity/.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from src.core.case_context import get_case_context
from src.services.ensemble import run_official_spill_forecast
from src.services.scoring import run_phase3b_scoring
from src.utils.io import (
    RecipeSelection,
    get_case_output_dir,
    get_ensemble_manifest_path,
    get_forecast_manifest_path,
    get_recipe_sensitivity_output_dir,
    get_recipe_sensitivity_run_name,
    resolve_recipe_selection,
    resolve_spill_origin,
    resolve_validation_mask_path,
)

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - optional at runtime
    plt = None

try:
    import rasterio
except ImportError:  # pragma: no cover - optional at runtime
    rasterio = None


@dataclass(frozen=True)
class EventSensitivitySpec:
    recipe_id: str
    currents_file: str
    wind_file: str
    wave_file: str
    current_source: str
    wind_source: str
    wave_source: str
    cheap_and_clean_only: bool = False


@dataclass(frozen=True)
class EvaluatedSensitivitySpec:
    recipe_id: str
    currents_path: str
    winds_path: str
    wave_path: str
    current_source: str
    wind_source: str
    wave_source: str
    available: bool
    missing_inputs: list[str]
    cheap_and_clean_only: bool = False


EVENT_SENSITIVITY_SPECS = [
    EventSensitivitySpec(
        recipe_id="cmems_era5",
        currents_file="cmems_curr.nc",
        wind_file="era5_wind.nc",
        wave_file="cmems_wave.nc",
        current_source="CMEMS",
        wind_source="ERA5",
        wave_source="CMEMS wave/Stokes",
    ),
    EventSensitivitySpec(
        recipe_id="cmems_gfs",
        currents_file="cmems_curr.nc",
        wind_file="gfs_wind.nc",
        wave_file="cmems_wave.nc",
        current_source="CMEMS",
        wind_source="GFS",
        wave_source="CMEMS wave/Stokes",
    ),
    EventSensitivitySpec(
        recipe_id="hycom_era5",
        currents_file="hycom_curr.nc",
        wind_file="era5_wind.nc",
        wave_file="cmems_wave.nc",
        current_source="HYCOM",
        wind_source="ERA5",
        wave_source="CMEMS wave/Stokes",
    ),
    EventSensitivitySpec(
        recipe_id="hycom_gfs",
        currents_file="hycom_curr.nc",
        wind_file="gfs_wind.nc",
        wave_file="cmems_wave.nc",
        current_source="HYCOM",
        wind_source="GFS",
        wave_source="CMEMS wave/Stokes",
        cheap_and_clean_only=True,
    ),
]


def _load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
        f.write("\n")


def _read_mask(path: Path) -> np.ndarray:
    if rasterio is None:  # pragma: no cover - runtime dependency
        raise RuntimeError("rasterio is required for recipe-sensitivity diagnostics.")
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _binary_mask(data: np.ndarray) -> np.ndarray:
    return (np.nan_to_num(np.asarray(data, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0) > 0.0).astype(np.float32)


def _find_manifest_product(products: list[dict], product_type: str, date_utc: str) -> dict:
    for product in products:
        if str(product.get("product_type")) != product_type:
            continue
        if str(product.get("date_utc")) != date_utc:
            continue
        return product
    raise RuntimeError(f"Manifest is missing product_type='{product_type}' for date_utc={date_utc}.")


class RecipeSensitivityService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("Official recipe sensitivities are only available for official workflow modes.")

        self.base_run_name = self.case.run_name
        self.output_root = get_recipe_sensitivity_output_dir(self.base_run_name)
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.validation_time = pd.Timestamp(self.case.validation_layer.event_time_utc or self.case.simulation_end_utc)
        self.validation_date = str(self.validation_time.date())
        self.frozen_selection = resolve_recipe_selection()
        self.historical_baseline_provenance = {
            "recipe": self.frozen_selection.recipe,
            "source_kind": self.frozen_selection.source_kind,
            "source_path": self.frozen_selection.source_path,
            "status_flag": self.frozen_selection.status_flag,
            "valid": self.frozen_selection.valid,
            "provisional": self.frozen_selection.provisional,
            "rerun_required": self.frozen_selection.rerun_required,
            "note": self.frozen_selection.note,
        }
        self.obs_mask_path = resolve_validation_mask_path(self.base_run_name)
        if not self.obs_mask_path.exists():
            raise FileNotFoundError(f"Official observation mask is missing: {self.obs_mask_path}")
        self.element_count, self.element_count_source = self._resolve_element_count()
        self.current_forcing_dir = Path("data") / "forcing" / self.base_run_name

    def _resolve_element_count(self) -> tuple[int, str]:
        case_path = Path(str(self.case.case_definition_path or ""))
        if not case_path.exists():
            raise FileNotFoundError("Official case definition is required to resolve the event-sensitivity element count.")
        with open(case_path, "r", encoding="utf-8") as f:
            case_cfg = yaml.safe_load(f) or {}
        value = int((case_cfg.get("forecast") or {}).get("element_count", 0))
        if value <= 0:
            raise RuntimeError("Official case config must define forecast.element_count > 0 for recipe sensitivities.")
        return value, "case_config.forecast.element_count (no convergence artifact found locally)"

    def get_candidate_specs(self) -> list[EvaluatedSensitivitySpec]:
        specs: list[EvaluatedSensitivitySpec] = []
        for spec in EVENT_SENSITIVITY_SPECS:
            currents_path = self.current_forcing_dir / spec.currents_file
            winds_path = self.current_forcing_dir / spec.wind_file
            wave_path = self.current_forcing_dir / spec.wave_file
            missing_inputs = [
                str(path)
                for path in (currents_path, winds_path, wave_path)
                if not path.exists()
            ]
            specs.append(
                EvaluatedSensitivitySpec(
                    recipe_id=spec.recipe_id,
                    currents_path=str(currents_path),
                    winds_path=str(winds_path),
                    wave_path=str(wave_path),
                    current_source=spec.current_source,
                    wind_source=spec.wind_source,
                    wave_source=spec.wave_source,
                    available=not missing_inputs,
                    missing_inputs=missing_inputs,
                    cheap_and_clean_only=spec.cheap_and_clean_only,
                )
            )
        return specs

    def _build_selection(self, spec: EvaluatedSensitivitySpec) -> RecipeSelection:
        note = (
            "Event-scale Phase 3B sensitivity. "
            f"Recipe {spec.recipe_id} uses {spec.current_source} currents + {spec.wind_source} winds + "
            f"{spec.wave_source}. Frozen historical baseline remains {self.frozen_selection.recipe} "
            "and is not rewritten by this run."
        )
        return RecipeSelection(
            recipe=spec.recipe_id,
            source_kind="event_phase3b_sensitivity",
            source_path=None,
            status_flag="provisional",
            valid=False,
            provisional=True,
            rerun_required=False,
            note=note,
        )

    def _build_sensitivity_context(self, spec: EvaluatedSensitivitySpec) -> dict:
        return {
            "event_scale_phase3b_sensitivity": True,
            "sensitivity_recipe_id": spec.recipe_id,
            "recipe_label": f"{spec.current_source} + {spec.wind_source} + {spec.wave_source}",
            "current_source": spec.current_source,
            "wind_source": spec.wind_source,
            "wave_source": spec.wave_source,
            "currents_path": spec.currents_path,
            "winds_path": spec.winds_path,
            "wave_path": spec.wave_path,
            "frozen_historical_baseline_recipe": self.frozen_selection.recipe,
            "matches_frozen_historical_baseline": spec.recipe_id == self.frozen_selection.recipe,
            "element_count": self.element_count,
            "element_count_source": self.element_count_source,
        }

    def _load_primary_summary_row(self, recipe_run_name: str) -> tuple[pd.Series, pd.DataFrame, pd.DataFrame]:
        phase3b_dir = get_case_output_dir(recipe_run_name) / "phase3b"
        summary_df = pd.read_csv(phase3b_dir / "phase3b_summary.csv")
        fss_df = pd.read_csv(phase3b_dir / "phase3b_fss_by_date_window.csv")
        diagnostics_df = pd.read_csv(phase3b_dir / "phase3b_diagnostics.csv")
        primary = summary_df[summary_df["pair_role"] == "primary"]
        if primary.empty:
            raise RuntimeError(f"Recipe sensitivity summary is missing the primary Phase 3B row for {recipe_run_name}.")
        return primary.iloc[0], fss_df, diagnostics_df

    def _resolve_datecomposite_probability_path(self, recipe_run_name: str) -> Path:
        manifest_path = get_ensemble_manifest_path(recipe_run_name)
        manifest = _load_json(manifest_path)
        product = _find_manifest_product(
            manifest.get("products") or [],
            product_type="prob_presence_datecomposite",
            date_utc=self.validation_date,
        )
        relative_path = str(product.get("relative_path") or "").strip()
        if not relative_path:
            raise RuntimeError(f"Ensemble manifest is missing a relative path for prob_presence_datecomposite: {manifest_path}")
        return get_case_output_dir(recipe_run_name) / relative_path

    def _load_occupancy_metrics(self, recipe_run_name: str, ensemble_size: int) -> dict:
        probability_path = self._resolve_datecomposite_probability_path(recipe_run_name)
        probability = _read_mask(probability_path)
        max_probability = float(np.nanmax(probability)) if probability.size else 0.0
        return {
            "prob_presence_datecomposite_path": str(probability_path),
            "max_march6_probability": max_probability,
            "max_march6_occupancy_members": int(round(max_probability * float(ensemble_size))),
        }

    def _build_overlay_canvas(self, forecast_mask: np.ndarray, obs_mask: np.ndarray) -> np.ndarray:
        overlap = np.logical_and(forecast_mask > 0, obs_mask > 0)
        canvas = np.ones((forecast_mask.shape[0], forecast_mask.shape[1], 3), dtype=np.float32)
        canvas[obs_mask > 0] = np.array([0.2, 0.45, 0.95], dtype=np.float32)
        canvas[forecast_mask > 0] = np.array([0.95, 0.35, 0.2], dtype=np.float32)
        canvas[overlap] = np.array([0.55, 0.2, 0.75], dtype=np.float32)
        return canvas

    def _write_overlay_figure(self, summary_df: pd.DataFrame) -> Path | None:
        if plt is None or rasterio is None:
            return None

        available = summary_df[summary_df["status"] == "completed"].copy()
        if available.empty:
            return None

        obs_mask = _binary_mask(_read_mask(self.obs_mask_path))
        ncols = min(2, len(available))
        nrows = int(np.ceil(len(available) / ncols))
        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6 * ncols, 6 * nrows))
        axes_array = np.atleast_1d(axes).reshape(-1)

        for ax, (_, row) in zip(axes_array, available.iterrows()):
            forecast_mask = _binary_mask(_read_mask(Path(str(row["forecast_path"]))))
            ax.imshow(self._build_overlay_canvas(forecast_mask, obs_mask), origin="upper")
            ax.set_title(
                f"{row['recipe_id']}\n"
                f"mean FSS={row['mean_fss']:.4f} | centroid={row['centroid_distance_m']:.0f} m"
            )
            ax.set_axis_off()

        for ax in axes_array[len(available):]:
            ax.set_axis_off()

        out_path = self.output_root / "qa_recipe_sensitivity_overlays.png"
        fig.suptitle("Mindoro event-scale recipe sensitivities: March 6 obs vs date-composite P50", fontsize=14)
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return out_path

    def _build_summary_row(
        self,
        spec: EvaluatedSensitivitySpec,
        recipe_run_name: str | None,
        forecast_manifest_path: Path | None,
        ensemble_manifest_path: Path | None,
        phase3b_run_manifest_path: Path | None,
        primary_row: pd.Series | None,
        occupancy: dict | None,
        status: str,
        error_text: str = "",
    ) -> dict:
        base = {
            "run_category": "event_scale_phase3b_sensitivity",
            "recipe_id": spec.recipe_id,
            "recipe_label": f"{spec.current_source} + {spec.wind_source} + {spec.wave_source}",
            "current_source": spec.current_source,
            "wind_source": spec.wind_source,
            "wave_source": spec.wave_source,
            "available_forcing": bool(spec.available),
            "missing_inputs": ";".join(spec.missing_inputs),
            "cheap_and_clean_only": bool(spec.cheap_and_clean_only),
            "status": status,
            "error_text": error_text,
            "recipe_run_name": recipe_run_name or "",
            "forecast_manifest_path": str(forecast_manifest_path) if forecast_manifest_path else "",
            "ensemble_manifest_path": str(ensemble_manifest_path) if ensemble_manifest_path else "",
            "phase3b_run_manifest_path": str(phase3b_run_manifest_path) if phase3b_run_manifest_path else "",
            "frozen_historical_baseline_recipe": self.frozen_selection.recipe,
            "matches_frozen_historical_baseline": spec.recipe_id == self.frozen_selection.recipe,
            "element_count": self.element_count,
            "element_count_source": self.element_count_source,
        }
        if primary_row is None:
            return {
                **base,
                "forecast_path": "",
                "observation_path": str(self.obs_mask_path),
                "transport_model": "",
                "provisional_transport_model": "",
                "phase3b_status_flag": "",
                "fss_1km": np.nan,
                "fss_3km": np.nan,
                "fss_5km": np.nan,
                "fss_10km": np.nan,
                "mean_fss": np.nan,
                "centroid_distance_m": np.nan,
                "area_ratio_forecast_to_obs": np.nan,
                "iou": np.nan,
                "dice": np.nan,
                "nearest_distance_to_obs_m": np.nan,
                "p50_nonzero_cells": np.nan,
                "obs_nonzero_cells": np.nan,
                "max_march6_probability": np.nan,
                "max_march6_occupancy_members": np.nan,
            }

        return {
            **base,
            "forecast_path": str(primary_row["forecast_path"]),
            "observation_path": str(primary_row["observation_path"]),
            "transport_model": str(primary_row["transport_model"]),
            "provisional_transport_model": bool(primary_row["provisional_transport_model"]),
            "phase3b_status_flag": str(primary_row["phase3b_status_flag"]),
            "fss_1km": float(primary_row["fss_1km"]),
            "fss_3km": float(primary_row["fss_3km"]),
            "fss_5km": float(primary_row["fss_5km"]),
            "fss_10km": float(primary_row["fss_10km"]),
            "mean_fss": float(np.nanmean([primary_row["fss_1km"], primary_row["fss_3km"], primary_row["fss_5km"], primary_row["fss_10km"]])),
            "centroid_distance_m": float(primary_row["centroid_distance_m"]),
            "area_ratio_forecast_to_obs": float(primary_row["area_ratio_forecast_to_obs"]),
            "iou": float(primary_row["iou"]),
            "dice": float(primary_row["dice"]),
            "nearest_distance_to_obs_m": float(primary_row["nearest_distance_to_obs_m"]),
            "p50_nonzero_cells": int(primary_row["forecast_nonzero_cells"]),
            "obs_nonzero_cells": int(primary_row["obs_nonzero_cells"]),
            "max_march6_probability": float((occupancy or {}).get("max_march6_probability", np.nan)),
            "max_march6_occupancy_members": int((occupancy or {}).get("max_march6_occupancy_members", 0)),
        }

    def _write_report(
        self,
        summary_df: pd.DataFrame,
        by_window_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        overlay_path: Path | None,
    ) -> Path:
        report_path = self.output_root / "recipe_sensitivity_report.md"
        completed = summary_df[summary_df["status"] == "completed"].copy()
        skipped = summary_df[summary_df["status"] == "skipped_missing_forcing"].copy()
        failed = summary_df[summary_df["status"] == "failed"].copy()

        lines = [
            "# Mindoro Event-Scale Phase 3B Recipe Sensitivities",
            "",
            f"- Frozen historical baseline recipe: `{self.frozen_selection.recipe}`",
            f"- Element count used: `{self.element_count}`",
            f"- Element-count provenance: `{self.element_count_source}`",
            f"- Official observation mask: `{self.obs_mask_path}`",
            "",
            "## Completed Recipes",
            "",
        ]

        if completed.empty:
            lines.append("No recipe sensitivities completed successfully.")
        else:
            ranked = completed.sort_values(
                by=["mean_fss", "fss_10km", "fss_5km", "fss_3km", "fss_1km", "centroid_distance_m"],
                ascending=[False, False, False, False, False, True],
            )
            for rank, (_, row) in enumerate(ranked.iterrows(), start=1):
                lines.extend(
                    [
                        (
                            f"{rank}. `{row['recipe_id']}`"
                            f" | mean FSS={row['mean_fss']:.4f}"
                            f" | FSS(1/3/5/10)={row['fss_1km']:.4f}/{row['fss_3km']:.4f}/{row['fss_5km']:.4f}/{row['fss_10km']:.4f}"
                            f" | centroid={row['centroid_distance_m']:.1f} m"
                            f" | IoU={row['iou']:.4f}"
                            f" | Dice={row['dice']:.4f}"
                        )
                    ]
                )

            best = ranked.iloc[0]
            baseline_rows = ranked[ranked["recipe_id"] == self.frozen_selection.recipe]
            baseline = baseline_rows.iloc[0] if not baseline_rows.empty else None
            if baseline is None:
                conclusion = "The frozen historical baseline was not among the completed sensitivity runs."
            elif best["recipe_id"] == self.frozen_selection.recipe:
                conclusion = "The best event-scale Phase 3B sensitivity matched the frozen historical baseline."
            elif float(best["mean_fss"]) > float(baseline["mean_fss"]) + 0.01:
                conclusion = (
                    f"The best event-scale recipe `{best['recipe_id']}` outperformed the frozen historical baseline "
                    f"`{self.frozen_selection.recipe}` on mean Phase 3B FSS."
                )
            else:
                conclusion = (
                    f"The best event-scale recipe `{best['recipe_id']}` differed from the frozen historical baseline "
                    f"`{self.frozen_selection.recipe}`, but the improvement was not meaningful on the official Phase 3B score."
                )

            lines.extend(["", "## Conclusion", "", conclusion])

        if not skipped.empty:
            lines.extend(["", "## Skipped Recipes", ""])
            for _, row in skipped.iterrows():
                lines.append(f"- `{row['recipe_id']}` skipped because required forcing files were missing: `{row['missing_inputs']}`")

        if not failed.empty:
            lines.extend(["", "## Failed Recipes", ""])
            for _, row in failed.iterrows():
                lines.append(f"- `{row['recipe_id']}` failed: `{row['error_text']}`")

        if overlay_path is not None and overlay_path.exists():
            lines.extend(["", "## QA Overlay", "", f"- `{overlay_path}`"])

        report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return report_path

    def run(self) -> dict:
        summary_rows: list[dict] = []
        by_window_frames: list[pd.DataFrame] = []
        diagnostics_frames: list[pd.DataFrame] = []
        candidate_specs = self.get_candidate_specs()
        start_lat, start_lon, start_time = resolve_spill_origin()

        for spec in candidate_specs:
            if not spec.available:
                summary_rows.append(
                    self._build_summary_row(
                        spec=spec,
                        recipe_run_name=None,
                        forecast_manifest_path=None,
                        ensemble_manifest_path=None,
                        phase3b_run_manifest_path=None,
                        primary_row=None,
                        occupancy=None,
                        status="skipped_missing_forcing",
                    )
                )
                continue

            recipe_run_name = get_recipe_sensitivity_run_name(spec.recipe_id, run_name=self.base_run_name)
            recipe_output_dir = get_case_output_dir(recipe_run_name)
            selection = self._build_selection(spec)
            sensitivity_context = self._build_sensitivity_context(spec)

            try:
                forecast_result = run_official_spill_forecast(
                    selection=selection,
                    start_time=start_time,
                    start_lat=start_lat,
                    start_lon=start_lon,
                    output_run_name=recipe_run_name,
                    forcing_override={
                        "currents": Path(spec.currents_path),
                        "wind": Path(spec.winds_path),
                        "wave": Path(spec.wave_path),
                    },
                    sensitivity_context=sensitivity_context,
                    historical_baseline_provenance=self.historical_baseline_provenance,
                )
                if forecast_result.get("status") != "success":
                    raise RuntimeError(str(forecast_result))

                phase3b_output_dir = recipe_output_dir / "phase3b"
                phase3b_results = run_phase3b_scoring(
                    output_dir=phase3b_output_dir,
                    forecast_run_name=recipe_run_name,
                    observation_run_name=self.base_run_name,
                    run_context=sensitivity_context,
                )

                primary_row, fss_df, diagnostics_df = self._load_primary_summary_row(recipe_run_name)
                ensemble_manifest = _load_json(get_ensemble_manifest_path(recipe_run_name))
                occupancy = self._load_occupancy_metrics(
                    recipe_run_name=recipe_run_name,
                    ensemble_size=int((ensemble_manifest.get("ensemble_configuration") or {}).get("ensemble_size", 0)),
                )

                by_window = fss_df.copy()
                by_window.insert(0, "recipe_id", spec.recipe_id)
                by_window.insert(1, "run_category", "event_scale_phase3b_sensitivity")
                by_window.insert(2, "frozen_historical_baseline_recipe", self.frozen_selection.recipe)
                by_window.insert(3, "matches_frozen_historical_baseline", spec.recipe_id == self.frozen_selection.recipe)
                by_window_frames.append(by_window)

                diagnostics = diagnostics_df.copy()
                diagnostics.insert(0, "recipe_id", spec.recipe_id)
                diagnostics.insert(1, "run_category", "event_scale_phase3b_sensitivity")
                diagnostics.insert(2, "frozen_historical_baseline_recipe", self.frozen_selection.recipe)
                diagnostics.insert(3, "matches_frozen_historical_baseline", spec.recipe_id == self.frozen_selection.recipe)
                diagnostics["max_march6_probability"] = occupancy["max_march6_probability"]
                diagnostics["max_march6_occupancy_members"] = occupancy["max_march6_occupancy_members"]
                diagnostics_frames.append(diagnostics)

                summary_rows.append(
                    self._build_summary_row(
                        spec=spec,
                        recipe_run_name=recipe_run_name,
                        forecast_manifest_path=get_forecast_manifest_path(recipe_run_name),
                        ensemble_manifest_path=get_ensemble_manifest_path(recipe_run_name),
                        phase3b_run_manifest_path=phase3b_results.run_manifest,
                        primary_row=primary_row,
                        occupancy=occupancy,
                        status="completed",
                    )
                )
            except Exception as exc:
                summary_rows.append(
                    self._build_summary_row(
                        spec=spec,
                        recipe_run_name=recipe_run_name,
                        forecast_manifest_path=get_forecast_manifest_path(recipe_run_name),
                        ensemble_manifest_path=get_ensemble_manifest_path(recipe_run_name),
                        phase3b_run_manifest_path=recipe_output_dir / "phase3b" / "phase3b_run_manifest.json",
                        primary_row=None,
                        occupancy=None,
                        status="failed",
                        error_text=f"{type(exc).__name__}: {exc}",
                    )
                )

        summary_df = pd.DataFrame(summary_rows)
        completed_mask = summary_df["status"] == "completed"
        if completed_mask.any():
            ranked = summary_df.loc[completed_mask].sort_values(
                by=["mean_fss", "fss_10km", "fss_5km", "fss_3km", "fss_1km", "centroid_distance_m"],
                ascending=[False, False, False, False, False, True],
            )
            rank_map = {recipe_id: rank for rank, recipe_id in enumerate(ranked["recipe_id"].tolist(), start=1)}
            summary_df["rank_by_mean_fss"] = summary_df["recipe_id"].map(rank_map)
        else:
            summary_df["rank_by_mean_fss"] = np.nan

        by_window_df = pd.concat(by_window_frames, ignore_index=True) if by_window_frames else pd.DataFrame()
        diagnostics_df = pd.concat(diagnostics_frames, ignore_index=True) if diagnostics_frames else pd.DataFrame()

        overlay_path = self._write_overlay_figure(summary_df)
        report_path = self._write_report(summary_df, by_window_df, diagnostics_df, overlay_path)

        summary_path = self.output_root / "recipe_sensitivity_summary.csv"
        by_window_path = self.output_root / "recipe_sensitivity_by_window.csv"
        diagnostics_path = self.output_root / "recipe_sensitivity_diagnostics.csv"
        summary_df.to_csv(summary_path, index=False)
        by_window_df.to_csv(by_window_path, index=False)
        diagnostics_df.to_csv(diagnostics_path, index=False)

        result_payload = {
            "run_category": "event_scale_phase3b_sensitivity",
            "frozen_historical_baseline_recipe": self.frozen_selection.recipe,
            "element_count": self.element_count,
            "element_count_source": self.element_count_source,
            "candidate_specs": [asdict(spec) for spec in candidate_specs],
            "artifacts": {
                "summary_csv": str(summary_path),
                "by_window_csv": str(by_window_path),
                "diagnostics_csv": str(diagnostics_path),
                "report_md": str(report_path),
                "overlay_png": str(overlay_path) if overlay_path else "",
            },
            "completed_recipes": summary_df[summary_df["status"] == "completed"].to_dict(orient="records"),
            "skipped_recipes": summary_df[summary_df["status"] == "skipped_missing_forcing"].to_dict(orient="records"),
            "failed_recipes": summary_df[summary_df["status"] == "failed"].to_dict(orient="records"),
        }
        _write_json(self.output_root / "recipe_sensitivity_manifest.json", result_payload)

        if not completed_mask.any():
            raise RuntimeError(
                "No event-scale recipe sensitivities completed successfully. "
                f"See {report_path} for skipped/failed recipe details."
            )

        return result_payload


def run_recipe_sensitivity() -> dict:
    service = RecipeSensitivityService()
    return service.run()
