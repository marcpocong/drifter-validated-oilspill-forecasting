"""
Shoreline-aware particle-count convergence study for the official Mindoro case.

This workflow keeps the official Phase 3B main pairing unchanged:
  March 6 date-composite mask_p50 vs obs_mask_2023-03-06.tif
"""

from __future__ import annotations

import json
import math
import os
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, precheck_same_grid
from src.services.ensemble import (
    OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV,
    run_official_spill_forecast,
)
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService, run_phase3b_scoring
from src.utils.io import (
    RecipeSelection,
    get_case_output_dir,
    get_convergence_after_shoreline_output_dir,
    get_convergence_after_shoreline_run_name,
    get_ensemble_manifest_path,
    get_forecast_manifest_path,
    get_official_mask_p50_datecomposite_path,
    get_phase2_loading_audit_paths,
    get_recipe_sensitivity_output_dir,
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

try:
    import xarray as xr
except ImportError:  # pragma: no cover - optional at runtime
    xr = None


CONVERGENCE_ELEMENT_COUNTS = [5000, 20000, 50000, 100000]
APPENDIX_EVENTCORRIDOR_OBS_NAME = "appendix_eventcorridor_obs_union_2023-03-03_to_2023-03-06.tif"
APPENDIX_EVENTCORRIDOR_MODEL_NAME = "appendix_eventcorridor_model_union_2023-03-03_to_2023-03-06.tif"


@dataclass(frozen=True)
class ConvergenceRecipeChoice:
    recipe: str
    source_kind: str
    source_path: str
    note: str
    uses_event_scale_recipe_result: bool
    clearly_best_recipe_identified: bool
    frozen_baseline_recipe: str


def _load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")
    os.replace(temp_path, path)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    df.to_csv(temp_path, index=False)
    os.replace(temp_path, path)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(temp_path, "w", encoding="utf-8") as handle:
        handle.write(text)
    os.replace(temp_path, path)


def _coerce_float(value) -> float:
    if value is None:
        return float("nan")
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return float("nan")
    return float(text)


def _read_mask(path: Path) -> np.ndarray:
    if rasterio is None:  # pragma: no cover - runtime dependency
        raise RuntimeError("rasterio is required for shoreline-aware convergence diagnostics.")
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _binary_mask(data: np.ndarray) -> np.ndarray:
    return (np.nan_to_num(np.asarray(data, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0) > 0.0).astype(np.float32)


def _find_manifest_product(
    products: list[dict],
    product_type: str,
    *,
    date_utc: str | None = None,
    timestamp_utc: str | None = None,
) -> dict:
    for product in products:
        if str(product.get("product_type")) != product_type:
            continue
        if date_utc is not None and str(product.get("date_utc")) != date_utc:
            continue
        if timestamp_utc is not None and str(product.get("timestamp_utc")) != timestamp_utc:
            continue
        return product
    raise RuntimeError(
        f"Manifest is missing product_type='{product_type}'"
        f"{'' if date_utc is None else f' date_utc={date_utc}'}"
        f"{'' if timestamp_utc is None else f' timestamp_utc={timestamp_utc}'}."
    )


def _rank_recipe_sensitivity_rows(summary_df: pd.DataFrame) -> pd.DataFrame:
    completed = summary_df[summary_df["status"].astype(str) == "completed"].copy()
    if completed.empty:
        return completed

    for column in (
        "mean_fss",
        "fss_1km",
        "fss_3km",
        "fss_5km",
        "fss_10km",
        "iou",
        "dice",
        "centroid_distance_m",
    ):
        if column in completed.columns:
            completed[column] = completed[column].map(_coerce_float)

    return completed.sort_values(
        by=["mean_fss", "fss_10km", "fss_5km", "fss_3km", "fss_1km", "iou", "dice", "centroid_distance_m"],
        ascending=[False, False, False, False, False, False, False, True],
        na_position="last",
    ).reset_index(drop=True)


def _recipe_rows_are_tied(first_row: pd.Series, second_row: pd.Series) -> bool:
    equal_high = all(
        np.isclose(_coerce_float(first_row[column]), _coerce_float(second_row[column]), equal_nan=True)
        for column in ("mean_fss", "fss_10km", "fss_5km", "fss_3km", "fss_1km", "iou", "dice")
    )
    equal_low = np.isclose(
        _coerce_float(first_row["centroid_distance_m"]),
        _coerce_float(second_row["centroid_distance_m"]),
        equal_nan=True,
    )
    return bool(equal_high and equal_low)


@contextmanager
def _temporary_element_count_override(element_count: int):
    previous = os.environ.get(OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV)
    os.environ[OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV] = str(int(element_count))
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV, None)
        else:
            os.environ[OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV] = previous


class ConvergenceAfterShorelineService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("convergence_after_shoreline is only supported for official spill-case workflows.")
        if xr is None:
            raise RuntimeError("xarray is required for shoreline-aware convergence runs.")

        self.base_run_name = self.case.run_name
        self.base_output_dir = get_case_output_dir(self.base_run_name)
        self.output_root = get_convergence_after_shoreline_output_dir(self.base_run_name)
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.validation_time = pd.Timestamp(self.case.validation_layer.event_time_utc or self.case.simulation_end_utc)
        self.validation_date = str(self.validation_time.date())
        self.official_obs_mask_path = resolve_validation_mask_path(self.base_run_name)
        if not self.official_obs_mask_path.exists():
            raise FileNotFoundError(f"Official March 6 observation mask is missing: {self.official_obs_mask_path}")

        self.frozen_selection = resolve_recipe_selection()
        self.recipe_choice = self._resolve_recipe_choice()
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
        self.grid = GridBuilder()
        self.phase3b_helper = Phase3BScoringService(output_dir=self.output_root / "_scratch_phase3b_helper")
        self.accepted_appendix_dates, self.appendix_obs_union_path = self._resolve_appendix_eventcorridor_context()
        self.compute_appendix_eventcorridor = bool(self.accepted_appendix_dates and self.appendix_obs_union_path and self.appendix_obs_union_path.exists())

    def _resolve_recipe_choice(self) -> ConvergenceRecipeChoice:
        summary_path = get_recipe_sensitivity_output_dir(self.base_run_name) / "recipe_sensitivity_summary.csv"
        if summary_path.exists():
            ranked = _rank_recipe_sensitivity_rows(pd.read_csv(summary_path))
            if not ranked.empty:
                top_row = ranked.iloc[0]
                clearly_best = len(ranked) == 1 or not _recipe_rows_are_tied(top_row, ranked.iloc[1])
                if clearly_best:
                    return ConvergenceRecipeChoice(
                        recipe=str(top_row["recipe_id"]),
                        source_kind="event_scale_recipe_sensitivity_best",
                        source_path=str(summary_path),
                        note=(
                            "Using the clearly best completed event-scale recipe sensitivity result for shoreline-aware convergence. "
                            f"The frozen historical baseline remains {self.frozen_selection.recipe} and is not overwritten."
                        ),
                        uses_event_scale_recipe_result=True,
                        clearly_best_recipe_identified=True,
                        frozen_baseline_recipe=self.frozen_selection.recipe,
                    )

        return ConvergenceRecipeChoice(
            recipe=self.frozen_selection.recipe,
            source_kind="frozen_historical_baseline_fallback",
            source_path=str(self.frozen_selection.source_path or ""),
            note=(
                "No completed local event-scale recipe sensitivity result clearly identified a single best within-horizon recipe. "
                "Falling back to the frozen historical baseline for shoreline-aware convergence."
            ),
            uses_event_scale_recipe_result=False,
            clearly_best_recipe_identified=False,
            frozen_baseline_recipe=self.frozen_selection.recipe,
        )

    def _build_recipe_selection(self) -> RecipeSelection:
        if not self.recipe_choice.uses_event_scale_recipe_result:
            return self.frozen_selection
        return RecipeSelection(
            recipe=self.recipe_choice.recipe,
            source_kind=self.recipe_choice.source_kind,
            source_path=self.recipe_choice.source_path,
            status_flag="provisional",
            valid=False,
            provisional=True,
            rerun_required=False,
            note=self.recipe_choice.note,
        )

    def _resolve_appendix_eventcorridor_context(self) -> tuple[list[str], Path | None]:
        appendix_manifest_path = self.base_output_dir / "public_obs_appendix" / "public_obs_appendix_manifest.json"
        obs_union_path = self.base_output_dir / "public_obs_appendix" / APPENDIX_EVENTCORRIDOR_OBS_NAME
        if not appendix_manifest_path.exists() or not obs_union_path.exists():
            return [], None

        manifest = _load_json(appendix_manifest_path)
        accepted_dates = sorted({str(value) for value in manifest.get("accepted_quantitative_dates") or [] if str(value).strip()})
        return accepted_dates, obs_union_path

    def _load_primary_row(self, run_name: str) -> tuple[pd.Series, pd.DataFrame]:
        phase3b_dir = get_case_output_dir(run_name) / "phase3b"
        summary_df = pd.read_csv(phase3b_dir / "phase3b_summary.csv")
        primary = summary_df[summary_df["pair_role"].astype(str) == "primary"]
        if primary.empty:
            raise RuntimeError(f"Phase 3B summary for {run_name} is missing the primary March 6 row.")
        return primary.iloc[0], summary_df

    def _resolve_probability_datecomposite_path(self, run_name: str) -> Path:
        manifest = _load_json(get_ensemble_manifest_path(run_name))
        product = _find_manifest_product(
            manifest.get("products") or [],
            "prob_presence_datecomposite",
            date_utc=self.validation_date,
        )
        return get_case_output_dir(run_name) / str(product.get("relative_path") or "")

    def _load_march6_occupancy_metrics(self, run_name: str) -> dict:
        ensemble_manifest = _load_json(get_ensemble_manifest_path(run_name))
        ensemble_size = int((ensemble_manifest.get("ensemble_configuration") or {}).get("ensemble_size", 0))
        probability_path = self._resolve_probability_datecomposite_path(run_name)
        probability = _read_mask(probability_path)
        max_probability = float(np.nanmax(probability)) if probability.size else 0.0
        return {
            "prob_presence_datecomposite_path": str(probability_path),
            "max_march6_probability": max_probability,
            "max_march6_occupancy_members": int(round(max_probability * max(ensemble_size, 0))),
        }

    def _appendix_eventcorridor_dir(self, run_name: str) -> Path:
        return get_case_output_dir(run_name) / "appendix_eventcorridor"

    def _member_paths(self, run_name: str) -> list[Path]:
        members = sorted((get_case_output_dir(run_name) / "ensemble").glob("member_*.nc"))
        if not members:
            raise FileNotFoundError(f"Appendix event-corridor scoring requires ensemble member outputs under {get_case_output_dir(run_name) / 'ensemble'}")
        return members

    def _build_appendix_date_composites(self, run_name: str) -> dict[str, Path]:
        date_dir = self._appendix_eventcorridor_dir(run_name) / "forecast_datecomposites"
        date_dir.mkdir(parents=True, exist_ok=True)
        target_dates = sorted(self.accepted_appendix_dates)
        member_paths = self._member_paths(run_name)
        per_date_member_masks: dict[str, list[np.ndarray]] = {date: [] for date in target_dates}

        for member_path in member_paths:
            member_daily = {
                date: np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
                for date in target_dates
            }
            with xr.open_dataset(member_path) as ds:
                times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
                if times.tz is not None:
                    times = times.tz_convert("UTC").tz_localize(None)
                for index, timestamp in enumerate(times):
                    date_key = str(timestamp.date())
                    if date_key not in member_daily:
                        continue
                    lon = np.asarray(ds["lon"].isel(time=index).values).reshape(-1)
                    lat = np.asarray(ds["lat"].isel(time=index).values).reshape(-1)
                    status = np.asarray(ds["status"].isel(time=index).values).reshape(-1)
                    valid = ~np.isnan(lon) & ~np.isnan(lat) & (status == 0)
                    if not np.any(valid):
                        continue
                    hits, _ = rasterize_particles(
                        self.grid,
                        lon[valid],
                        lat[valid],
                        np.ones(np.count_nonzero(valid), dtype=np.float32),
                    )
                    member_daily[date_key] = np.maximum(member_daily[date_key], hits.astype(np.float32))
            for date_key, mask in member_daily.items():
                per_date_member_masks[date_key].append(mask.astype(np.float32))

        output_paths: dict[str, Path] = {}
        for date_key, member_masks in per_date_member_masks.items():
            if not member_masks:
                raise RuntimeError(f"No ensemble member occupancy was found for accepted appendix date {date_key} in run {run_name}.")
            probability = np.mean(np.stack(member_masks, axis=0), axis=0).astype(np.float32)
            threshold_mask = apply_ocean_mask((probability >= 0.5).astype(np.float32), sea_mask=self.phase3b_helper.sea_mask, fill_value=0.0)
            out_path = date_dir / f"mask_p50_{date_key}_datecomposite.tif"
            save_raster(self.grid, threshold_mask.astype(np.float32), out_path)
            output_paths[date_key] = out_path
        return output_paths

    def _score_appendix_eventcorridor(self, run_name: str) -> dict:
        if not self.compute_appendix_eventcorridor:
            return {
                "status": "not_computed",
                "reason": "Accepted appendix within-horizon observation union is not available locally.",
            }

        date_composites = self._build_appendix_date_composites(run_name)
        model_union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for date_key in self.accepted_appendix_dates:
            model_union = np.maximum(
                model_union,
                self.phase3b_helper._load_binary_score_mask(date_composites[date_key]),
            )

        run_dir = self._appendix_eventcorridor_dir(run_name)
        run_dir.mkdir(parents=True, exist_ok=True)
        model_union_path = run_dir / APPENDIX_EVENTCORRIDOR_MODEL_NAME
        save_raster(self.grid, model_union.astype(np.float32), model_union_path)

        precheck = precheck_same_grid(
            model_union_path,
            self.appendix_obs_union_path,
            report_base_path=run_dir / "appendix_eventcorridor_precheck",
        )
        if not precheck.passed:
            raise RuntimeError(
                "Appendix event-corridor pair failed same-grid precheck: "
                f"{precheck.json_report_path}"
            )

        forecast_mask = self.phase3b_helper._load_binary_score_mask(model_union_path)
        observation_mask = self.phase3b_helper._load_binary_score_mask(self.appendix_obs_union_path)
        diagnostics = self.phase3b_helper._compute_mask_diagnostics(forecast_mask, observation_mask)
        fss_rows = []
        summary = {
            "score_group": "appendix_eventcorridor",
            "forecast_path": str(model_union_path),
            "observation_path": str(self.appendix_obs_union_path),
            "precheck_csv": str(precheck.csv_report_path),
            "precheck_json": str(precheck.json_report_path),
            **diagnostics,
        }
        for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
            fss = float(
                np.clip(
                    calculate_fss(
                        forecast_mask,
                        observation_mask,
                        window=self.phase3b_helper._window_km_to_cells(window_km),
                        valid_mask=(self.phase3b_helper.sea_mask > 0.5) if self.phase3b_helper.sea_mask is not None else None,
                    ),
                    0.0,
                    1.0,
                )
            )
            summary[f"fss_{window_km}km"] = fss
            fss_rows.append(
                {
                    "score_group": "appendix_eventcorridor",
                    "window_km": int(window_km),
                    "fss": fss,
                    "forecast_path": str(model_union_path),
                    "observation_path": str(self.appendix_obs_union_path),
                }
            )

        _write_csv(run_dir / "appendix_eventcorridor_fss_by_window.csv", pd.DataFrame(fss_rows))
        _write_csv(run_dir / "appendix_eventcorridor_diagnostics.csv", pd.DataFrame([summary]))
        return {
            "status": "completed",
            "summary": summary,
            "fss_rows": fss_rows,
            "model_union_path": str(model_union_path),
            "observation_union_path": str(self.appendix_obs_union_path),
            "date_composites": {date_key: str(path) for date_key, path in date_composites.items()},
        }

    def _build_overlay_canvas(self, forecast_mask: np.ndarray, obs_mask: np.ndarray) -> np.ndarray:
        overlap = np.logical_and(forecast_mask > 0, obs_mask > 0)
        canvas = np.ones((forecast_mask.shape[0], forecast_mask.shape[1], 3), dtype=np.float32)
        canvas[obs_mask > 0] = np.array([0.2, 0.45, 0.95], dtype=np.float32)
        canvas[forecast_mask > 0] = np.array([0.95, 0.35, 0.2], dtype=np.float32)
        canvas[overlap] = np.array([0.55, 0.2, 0.75], dtype=np.float32)
        return canvas

    def _write_fss_plot(self, summary_df: pd.DataFrame) -> Path | None:
        if plt is None:
            return None
        completed = summary_df[summary_df["status"] == "completed"].sort_values("element_count_actual").copy()
        if completed.empty:
            return None

        fig, axes = plt.subplots(1, 2 if completed["appendix_eventcorridor_computed"].any() else 1, figsize=(12, 5))
        axes_array = np.atleast_1d(axes).reshape(-1)
        x = completed["element_count_actual"].astype(int).tolist()

        for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
            axes_array[0].plot(x, completed[f"official_main_fss_{window_km}km"], marker="o", label=f"{window_km} km")
        axes_array[0].set_title("Official main Phase 3B FSS")
        axes_array[0].set_xlabel("Element count")
        axes_array[0].set_ylabel("FSS")
        axes_array[0].set_ylim(0.0, 1.0)
        axes_array[0].grid(True, alpha=0.3)
        axes_array[0].legend()

        if len(axes_array) > 1:
            appendix = completed[completed["appendix_eventcorridor_computed"]].copy()
            if not appendix.empty:
                x_app = appendix["element_count_actual"].astype(int).tolist()
                for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
                    axes_array[1].plot(x_app, appendix[f"appendix_eventcorridor_fss_{window_km}km"], marker="o", label=f"{window_km} km")
                axes_array[1].set_title("Appendix event-corridor FSS")
                axes_array[1].set_xlabel("Element count")
                axes_array[1].set_ylabel("FSS")
                axes_array[1].set_ylim(0.0, 1.0)
                axes_array[1].grid(True, alpha=0.3)
                axes_array[1].legend()
            else:
                axes_array[1].set_axis_off()

        out_path = self.output_root / "qa_convergence_after_shoreline_fss.png"
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return out_path

    def _write_nonzero_plot(self, summary_df: pd.DataFrame) -> Path | None:
        if plt is None:
            return None
        completed = summary_df[summary_df["status"] == "completed"].sort_values("element_count_actual").copy()
        if completed.empty:
            return None

        fig, ax1 = plt.subplots(figsize=(8, 5))
        x = completed["element_count_actual"].astype(int).tolist()
        ax1.plot(x, completed["official_main_forecast_nonzero_cells"], marker="o", label="Official main forecast nonzero")
        ax1.plot(x, completed["official_main_obs_nonzero_cells"], marker="o", label="Official main obs nonzero")
        if completed["appendix_eventcorridor_computed"].any():
            appendix = completed[completed["appendix_eventcorridor_computed"]]
            ax1.plot(
                appendix["element_count_actual"].astype(int).tolist(),
                appendix["appendix_eventcorridor_forecast_nonzero_cells"],
                marker="o",
                linestyle="--",
                label="Appendix event-corridor forecast nonzero",
            )
        ax1.set_xlabel("Element count")
        ax1.set_ylabel("Nonzero cells")
        ax1.grid(True, alpha=0.3)
        ax1.legend(loc="upper left")

        ax2 = ax1.twinx()
        ax2.plot(x, completed["max_march6_occupancy_members"], color="black", marker="s", label="Max March 6 occupancy")
        ax2.set_ylabel("Occupancy members")
        ax2.legend(loc="upper right")

        out_path = self.output_root / "qa_convergence_after_shoreline_nonzero.png"
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return out_path

    def _write_overlay_plot(self, summary_df: pd.DataFrame) -> Path | None:
        if plt is None or rasterio is None:
            return None
        completed = summary_df[summary_df["status"] == "completed"].sort_values("element_count_actual").copy()
        if completed.empty:
            return None

        obs_mask = _binary_mask(_read_mask(self.official_obs_mask_path))
        ncols = min(2, len(completed))
        nrows = int(math.ceil(len(completed) / max(ncols, 1)))
        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6 * ncols, 6 * nrows))
        axes_array = np.atleast_1d(axes).reshape(-1)

        for ax, (_, row) in zip(axes_array, completed.iterrows()):
            forecast_mask = _binary_mask(_read_mask(Path(str(row["official_main_forecast_path"]))))
            ax.imshow(self._build_overlay_canvas(forecast_mask, obs_mask), origin="upper")
            ax.set_title(
                f"{int(row['element_count_actual'])} elements\n"
                f"FSS10={_coerce_float(row['official_main_fss_10km']):.4f} | "
                f"nonzero={int(row['official_main_forecast_nonzero_cells'])}"
            )
            ax.set_axis_off()

        for ax in axes_array[len(completed):]:
            ax.set_axis_off()

        out_path = self.output_root / "qa_convergence_after_shoreline_overlays.png"
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return out_path

    def _build_recommendation(self, summary_df: pd.DataFrame) -> dict:
        completed = summary_df[summary_df["status"] == "completed"].sort_values("element_count_actual").copy()
        if completed.empty:
            return {
                "recommended_final_official_element_count": None,
                "five_thousand_too_sparse": None,
                "one_hundred_thousand_feasible": False,
                "one_hundred_thousand_materially_stabilizes_march6_p50": False,
                "strong_enough_for_final_official_phase3b_rerun": False,
                "reason": "No convergence runs completed successfully.",
            }

        highest = completed.iloc[-1]
        row_5000 = completed[completed["element_count_actual"] == 5000]
        row_50000 = completed[completed["element_count_actual"] == 50000]
        row_100000 = completed[completed["element_count_actual"] == 100000]
        five_thousand_too_sparse = False
        if not row_5000.empty and len(completed) > 1:
            base = row_5000.iloc[0]
            higher = completed[completed["element_count_actual"] > 5000]
            five_thousand_too_sparse = bool(
                int(base["official_main_forecast_nonzero_cells"]) == 0
                and int(higher["official_main_forecast_nonzero_cells"].max()) > 0
            ) or bool(
                int(base["max_march6_occupancy_members"]) < int(higher["max_march6_occupancy_members"].max())
            ) or bool(
                int(base["official_main_forecast_nonzero_cells"]) < int(higher["official_main_forecast_nonzero_cells"].max())
            )

        one_hundred_thousand_feasible = not row_100000.empty
        one_hundred_thousand_materially_stabilizes = False
        if one_hundred_thousand_feasible and not row_50000.empty:
            row_hi = row_100000.iloc[0]
            row_mid = row_50000.iloc[0]
            fss_stable = all(
                abs(_coerce_float(row_hi[f"official_main_fss_{window}km"]) - _coerce_float(row_mid[f"official_main_fss_{window}km"])) <= 0.01
                for window in OFFICIAL_PHASE3B_WINDOWS_KM
            )
            nonzero_stable = abs(int(row_hi["official_main_forecast_nonzero_cells"]) - int(row_mid["official_main_forecast_nonzero_cells"])) <= max(
                5,
                int(round(0.1 * max(int(row_hi["official_main_forecast_nonzero_cells"]), 1))),
            )
            occupancy_stable = abs(int(row_hi["max_march6_occupancy_members"]) - int(row_mid["max_march6_occupancy_members"])) <= 1
            one_hundred_thousand_materially_stabilizes = bool(fss_stable and nonzero_stable and occupancy_stable)

        recommended_count = 100000 if one_hundred_thousand_feasible else int(highest["element_count_actual"])
        strong_enough = bool(
            int(highest["official_main_forecast_nonzero_cells"]) > 0
            and max(_coerce_float(highest[f"official_main_fss_{window}km"]) for window in OFFICIAL_PHASE3B_WINDOWS_KM) > 0.0
        )
        reason = (
            "100000 completed successfully and matches the frozen Chapter 3 target element count."
            if one_hundred_thousand_feasible
            else f"100000 did not complete; recommending the highest successful count {recommended_count}."
        )
        return {
            "recommended_final_official_element_count": int(recommended_count),
            "five_thousand_too_sparse": bool(five_thousand_too_sparse),
            "one_hundred_thousand_feasible": bool(one_hundred_thousand_feasible),
            "one_hundred_thousand_materially_stabilizes_march6_p50": bool(one_hundred_thousand_materially_stabilizes),
            "strong_enough_for_final_official_phase3b_rerun": bool(strong_enough),
            "reason": reason,
        }

    def _write_report(self, summary_df: pd.DataFrame, recommendation: dict, recipe_selection: RecipeSelection) -> Path:
        report_path = self.output_root / "convergence_after_shoreline_report.md"
        completed = summary_df[summary_df["status"] == "completed"].sort_values("element_count_actual").copy()
        failed = summary_df[summary_df["status"] != "completed"].copy()

        lines = [
            "# Mindoro Shoreline-Aware Particle-Count Convergence",
            "",
            f"- Selected recipe for convergence: `{recipe_selection.recipe}`",
            f"- Recipe choice source: `{self.recipe_choice.source_kind}`",
            f"- Frozen historical baseline recipe: `{self.frozen_selection.recipe}`",
            f"- Recipe choice note: {self.recipe_choice.note}",
            f"- Official main pair remains `{Path(get_official_mask_p50_datecomposite_path(self.base_run_name)).name}` vs `{self.official_obs_mask_path.name}`.",
            f"- Appendix event-corridor computed: `{'yes' if self.compute_appendix_eventcorridor else 'no'}`",
            "",
            "## Completed Runs",
            "",
        ]
        if completed.empty:
            lines.append("No convergence runs completed successfully.")
        else:
            for _, row in completed.iterrows():
                lines.append(
                    (
                        f"- `{int(row['element_count_actual'])}` elements"
                        f" | official FSS(1/3/5/10)={_coerce_float(row['official_main_fss_1km']):.4f}/"
                        f"{_coerce_float(row['official_main_fss_3km']):.4f}/"
                        f"{_coerce_float(row['official_main_fss_5km']):.4f}/"
                        f"{_coerce_float(row['official_main_fss_10km']):.4f}"
                        f" | forecast_nonzero={int(row['official_main_forecast_nonzero_cells'])}"
                        f" | max_occ={int(row['max_march6_occupancy_members'])}"
                        f" | runtime={_coerce_float(row['runtime_minutes']):.2f} min"
                    )
                )

        if not failed.empty:
            lines.extend(["", "## Failed / Incomplete Runs", ""])
            for _, row in failed.iterrows():
                lines.append(
                    f"- `{int(row['element_count_requested'])}` elements: `{row['status']}`"
                    f" | stop reason: `{row['stop_reason']}`"
                )

        lines.extend(
            [
                "",
                "## Recommendation",
                "",
                f"- Recommended final official element_count: `{recommendation['recommended_final_official_element_count']}`",
                f"- Was 5000 too sparse? `{recommendation['five_thousand_too_sparse']}`",
                f"- Is 100000 feasible? `{recommendation['one_hundred_thousand_feasible']}`",
                (
                    f"- Does 100000 materially stabilize the official March 6 p50 footprint? "
                    f"`{recommendation['one_hundred_thousand_materially_stabilizes_march6_p50']}`"
                ),
                (
                    f"- Is the stabilized result strong enough for the final official Phase 3B rerun? "
                    f"`{recommendation['strong_enough_for_final_official_phase3b_rerun']}`"
                ),
                f"- Rationale: {recommendation['reason']}",
            ]
        )
        _write_text(report_path, "\n".join(lines) + "\n")
        return report_path

    def run(self) -> dict:
        selection = self._build_recipe_selection()
        start_lat, start_lon, start_time = resolve_spill_origin()
        summary_rows: list[dict] = []
        by_window_rows: list[dict] = []
        diagnostics_rows: list[dict] = []

        for element_count in CONVERGENCE_ELEMENT_COUNTS:
            run_name = get_convergence_after_shoreline_run_name(element_count, run_name=self.base_run_name)
            run_output_dir = get_case_output_dir(run_name)
            start_clock = time.perf_counter()
            appendix_result = {"status": "not_computed", "reason": "Appendix event-corridor scoring not requested."}
            try:
                with _temporary_element_count_override(element_count):
                    run_context = {
                        "shoreline_aware_convergence": True,
                        "requested_element_count": int(element_count),
                        "recipe_choice_source_kind": self.recipe_choice.source_kind,
                        "recipe_choice_source_path": self.recipe_choice.source_path,
                        "recipe_choice_note": self.recipe_choice.note,
                        "frozen_baseline_recipe": self.frozen_selection.recipe,
                        "selected_convergence_recipe": selection.recipe,
                    }
                    forecast_result = run_official_spill_forecast(
                        selection=selection,
                        start_time=start_time,
                        start_lat=start_lat,
                        start_lon=start_lon,
                        output_run_name=run_name,
                        sensitivity_context=run_context,
                        historical_baseline_provenance=self.historical_baseline_provenance,
                    )
                    if forecast_result.get("status") != "success":
                        raise RuntimeError(str(forecast_result))

                    phase3b_results = run_phase3b_scoring(
                        output_dir=run_output_dir / "phase3b",
                        forecast_run_name=run_name,
                        observation_run_name=self.base_run_name,
                        run_context=run_context,
                    )

                    primary_row, _ = self._load_primary_row(run_name)
                    forecast_manifest = _load_json(get_forecast_manifest_path(run_name))
                    ensemble_manifest = _load_json(get_ensemble_manifest_path(run_name))
                    occupancy = self._load_march6_occupancy_metrics(run_name)
                    actual_element_count = int(
                        (forecast_manifest.get("ensemble") or {}).get("actual_element_count")
                        or (ensemble_manifest.get("ensemble_configuration") or {}).get("element_count")
                        or element_count
                    )

                    if self.compute_appendix_eventcorridor:
                        appendix_result = self._score_appendix_eventcorridor(run_name)

                    runtime_minutes = (time.perf_counter() - start_clock) / 60.0
                    summary_rows.append(
                        {
                            "status": "completed",
                            "stop_reason": "",
                            "recipe_id": selection.recipe,
                            "recipe_choice_source_kind": self.recipe_choice.source_kind,
                            "recipe_choice_source_path": self.recipe_choice.source_path,
                            "recipe_choice_note": self.recipe_choice.note,
                            "used_event_scale_recipe_result": self.recipe_choice.uses_event_scale_recipe_result,
                            "frozen_baseline_recipe": self.frozen_selection.recipe,
                            "element_count_requested": int(element_count),
                            "element_count_actual": actual_element_count,
                            "run_name": run_name,
                            "forecast_manifest_path": str(get_forecast_manifest_path(run_name)),
                            "ensemble_manifest_path": str(get_ensemble_manifest_path(run_name)),
                            "phase2_loading_audit_json": str(get_phase2_loading_audit_paths(run_name)["json"]),
                            "phase2_loading_audit_csv": str(get_phase2_loading_audit_paths(run_name)["csv"]),
                            "phase3b_summary_path": str(run_output_dir / "phase3b" / "phase3b_summary.csv"),
                            "phase3b_run_manifest_path": str(phase3b_results.run_manifest),
                            "official_main_forecast_path": str(primary_row["forecast_path"]),
                            "official_main_observation_path": str(primary_row["observation_path"]),
                            "official_main_fss_1km": _coerce_float(primary_row["fss_1km"]),
                            "official_main_fss_3km": _coerce_float(primary_row["fss_3km"]),
                            "official_main_fss_5km": _coerce_float(primary_row["fss_5km"]),
                            "official_main_fss_10km": _coerce_float(primary_row["fss_10km"]),
                            "official_main_forecast_nonzero_cells": int(primary_row["forecast_nonzero_cells"]),
                            "official_main_obs_nonzero_cells": int(primary_row["obs_nonzero_cells"]),
                            "official_main_centroid_distance_m": _coerce_float(primary_row["centroid_distance_m"]),
                            "official_main_area_ratio_forecast_to_obs": _coerce_float(primary_row["area_ratio_forecast_to_obs"]),
                            "official_main_iou": _coerce_float(primary_row["iou"]),
                            "official_main_dice": _coerce_float(primary_row["dice"]),
                            "official_main_nearest_distance_to_obs_m": _coerce_float(primary_row["nearest_distance_to_obs_m"]),
                            "max_march6_probability": occupancy["max_march6_probability"],
                            "max_march6_occupancy_members": occupancy["max_march6_occupancy_members"],
                            "p50_nonzero_cells_march6": int(primary_row["forecast_nonzero_cells"]),
                            "runtime_minutes": float(runtime_minutes),
                            "appendix_eventcorridor_computed": appendix_result.get("status") == "completed",
                            "appendix_eventcorridor_status": appendix_result.get("status"),
                            "appendix_eventcorridor_reason": appendix_result.get("reason", ""),
                            "appendix_eventcorridor_fss_1km": _coerce_float((appendix_result.get("summary") or {}).get("fss_1km")),
                            "appendix_eventcorridor_fss_3km": _coerce_float((appendix_result.get("summary") or {}).get("fss_3km")),
                            "appendix_eventcorridor_fss_5km": _coerce_float((appendix_result.get("summary") or {}).get("fss_5km")),
                            "appendix_eventcorridor_fss_10km": _coerce_float((appendix_result.get("summary") or {}).get("fss_10km")),
                            "appendix_eventcorridor_forecast_nonzero_cells": _coerce_float((appendix_result.get("summary") or {}).get("forecast_nonzero_cells")),
                            "appendix_eventcorridor_obs_nonzero_cells": _coerce_float((appendix_result.get("summary") or {}).get("obs_nonzero_cells")),
                            "appendix_eventcorridor_centroid_distance_m": _coerce_float((appendix_result.get("summary") or {}).get("centroid_distance_m")),
                            "appendix_eventcorridor_area_ratio_forecast_to_obs": _coerce_float((appendix_result.get("summary") or {}).get("area_ratio_forecast_to_obs")),
                            "appendix_eventcorridor_iou": _coerce_float((appendix_result.get("summary") or {}).get("iou")),
                            "appendix_eventcorridor_dice": _coerce_float((appendix_result.get("summary") or {}).get("dice")),
                        }
                    )

                    for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
                        by_window_rows.append(
                            {
                                "score_group": "official_main",
                                "element_count_requested": int(element_count),
                                "element_count_actual": actual_element_count,
                                "recipe_id": selection.recipe,
                                "window_km": int(window_km),
                                "fss": _coerce_float(primary_row[f"fss_{window_km}km"]),
                                "forecast_path": str(primary_row["forecast_path"]),
                                "observation_path": str(primary_row["observation_path"]),
                            }
                        )
                    diagnostics_rows.append(
                        {
                            "score_group": "official_main",
                            "element_count_requested": int(element_count),
                            "element_count_actual": actual_element_count,
                            "recipe_id": selection.recipe,
                            "forecast_path": str(primary_row["forecast_path"]),
                            "observation_path": str(primary_row["observation_path"]),
                            "forecast_nonzero_cells": int(primary_row["forecast_nonzero_cells"]),
                            "obs_nonzero_cells": int(primary_row["obs_nonzero_cells"]),
                            "area_ratio_forecast_to_obs": _coerce_float(primary_row["area_ratio_forecast_to_obs"]),
                            "centroid_distance_m": _coerce_float(primary_row["centroid_distance_m"]),
                            "iou": _coerce_float(primary_row["iou"]),
                            "dice": _coerce_float(primary_row["dice"]),
                            "nearest_distance_to_obs_m": _coerce_float(primary_row["nearest_distance_to_obs_m"]),
                            "max_march6_probability": occupancy["max_march6_probability"],
                            "max_march6_occupancy_members": occupancy["max_march6_occupancy_members"],
                            "p50_nonzero_cells_march6": int(primary_row["forecast_nonzero_cells"]),
                            "runtime_minutes": float(runtime_minutes),
                        }
                    )

                    if appendix_result.get("status") == "completed":
                        appendix_summary = appendix_result["summary"]
                        for row in appendix_result["fss_rows"]:
                            by_window_rows.append(
                                {
                                    "score_group": "appendix_eventcorridor",
                                    "element_count_requested": int(element_count),
                                    "element_count_actual": actual_element_count,
                                    "recipe_id": selection.recipe,
                                    "window_km": int(row["window_km"]),
                                    "fss": _coerce_float(row["fss"]),
                                    "forecast_path": row["forecast_path"],
                                    "observation_path": row["observation_path"],
                                }
                            )
                        diagnostics_rows.append(
                            {
                                "score_group": "appendix_eventcorridor",
                                "element_count_requested": int(element_count),
                                "element_count_actual": actual_element_count,
                                "recipe_id": selection.recipe,
                                "forecast_path": appendix_summary["forecast_path"],
                                "observation_path": appendix_summary["observation_path"],
                                "forecast_nonzero_cells": appendix_summary["forecast_nonzero_cells"],
                                "obs_nonzero_cells": appendix_summary["obs_nonzero_cells"],
                                "area_ratio_forecast_to_obs": appendix_summary["area_ratio_forecast_to_obs"],
                                "centroid_distance_m": appendix_summary["centroid_distance_m"],
                                "iou": appendix_summary["iou"],
                                "dice": appendix_summary["dice"],
                                "nearest_distance_to_obs_m": appendix_summary["nearest_distance_to_obs_m"],
                                "max_march6_probability": occupancy["max_march6_probability"],
                                "max_march6_occupancy_members": occupancy["max_march6_occupancy_members"],
                                "p50_nonzero_cells_march6": appendix_summary["forecast_nonzero_cells"],
                                "runtime_minutes": float(runtime_minutes),
                            }
                        )
            except Exception as exc:
                summary_rows.append(
                    {
                        "status": "failed",
                        "stop_reason": f"{type(exc).__name__}: {exc}",
                        "recipe_id": selection.recipe,
                        "recipe_choice_source_kind": self.recipe_choice.source_kind,
                        "recipe_choice_source_path": self.recipe_choice.source_path,
                        "recipe_choice_note": self.recipe_choice.note,
                        "used_event_scale_recipe_result": self.recipe_choice.uses_event_scale_recipe_result,
                        "frozen_baseline_recipe": self.frozen_selection.recipe,
                        "element_count_requested": int(element_count),
                        "element_count_actual": np.nan,
                        "run_name": run_name,
                        "forecast_manifest_path": str(get_forecast_manifest_path(run_name)),
                        "ensemble_manifest_path": str(get_ensemble_manifest_path(run_name)),
                        "phase2_loading_audit_json": str(get_phase2_loading_audit_paths(run_name)["json"]),
                        "phase2_loading_audit_csv": str(get_phase2_loading_audit_paths(run_name)["csv"]),
                        "phase3b_summary_path": str(get_case_output_dir(run_name) / "phase3b" / "phase3b_summary.csv"),
                        "phase3b_run_manifest_path": str(get_case_output_dir(run_name) / "phase3b" / "phase3b_run_manifest.json"),
                        "official_main_forecast_path": "",
                        "official_main_observation_path": str(self.official_obs_mask_path),
                        "official_main_fss_1km": np.nan,
                        "official_main_fss_3km": np.nan,
                        "official_main_fss_5km": np.nan,
                        "official_main_fss_10km": np.nan,
                        "official_main_forecast_nonzero_cells": np.nan,
                        "official_main_obs_nonzero_cells": np.nan,
                        "official_main_centroid_distance_m": np.nan,
                        "official_main_area_ratio_forecast_to_obs": np.nan,
                        "official_main_iou": np.nan,
                        "official_main_dice": np.nan,
                        "official_main_nearest_distance_to_obs_m": np.nan,
                        "max_march6_probability": np.nan,
                        "max_march6_occupancy_members": np.nan,
                        "p50_nonzero_cells_march6": np.nan,
                        "runtime_minutes": float((time.perf_counter() - start_clock) / 60.0),
                        "appendix_eventcorridor_computed": False,
                        "appendix_eventcorridor_status": appendix_result.get("status", ""),
                        "appendix_eventcorridor_reason": appendix_result.get("reason", ""),
                        "appendix_eventcorridor_fss_1km": np.nan,
                        "appendix_eventcorridor_fss_3km": np.nan,
                        "appendix_eventcorridor_fss_5km": np.nan,
                        "appendix_eventcorridor_fss_10km": np.nan,
                        "appendix_eventcorridor_forecast_nonzero_cells": np.nan,
                        "appendix_eventcorridor_obs_nonzero_cells": np.nan,
                        "appendix_eventcorridor_centroid_distance_m": np.nan,
                        "appendix_eventcorridor_area_ratio_forecast_to_obs": np.nan,
                        "appendix_eventcorridor_iou": np.nan,
                        "appendix_eventcorridor_dice": np.nan,
                    }
                )

        summary_df = pd.DataFrame(summary_rows)
        by_window_df = pd.DataFrame(by_window_rows)
        diagnostics_df = pd.DataFrame(diagnostics_rows)
        recommendation = self._build_recommendation(summary_df)

        summary_path = self.output_root / "convergence_after_shoreline_summary.csv"
        by_window_path = self.output_root / "convergence_after_shoreline_by_window.csv"
        diagnostics_path = self.output_root / "convergence_after_shoreline_diagnostics.csv"
        _write_csv(summary_path, summary_df)
        _write_csv(by_window_path, by_window_df)
        _write_csv(diagnostics_path, diagnostics_df)

        fss_plot = self._write_fss_plot(summary_df)
        nonzero_plot = self._write_nonzero_plot(summary_df)
        overlay_plot = self._write_overlay_plot(summary_df)
        report_path = self._write_report(summary_df, recommendation, selection)

        manifest = {
            "run_category": "convergence_after_shoreline",
            "base_run_name": self.base_run_name,
            "official_main_pair_locked": {
                "forecast": "mask_p50_2023-03-06_datecomposite.tif",
                "observation": "obs_mask_2023-03-06.tif",
                "windows_km": list(OFFICIAL_PHASE3B_WINDOWS_KM),
            },
            "recipe_choice": asdict(self.recipe_choice),
            "frozen_baseline_provenance": self.historical_baseline_provenance,
            "attempted_element_counts": list(CONVERGENCE_ELEMENT_COUNTS),
            "appendix_eventcorridor_computed": bool(self.compute_appendix_eventcorridor),
            "appendix_eventcorridor_dates": list(self.accepted_appendix_dates),
            "appendix_eventcorridor_obs_union_path": str(self.appendix_obs_union_path) if self.appendix_obs_union_path else "",
            "recommendation": recommendation,
            "artifacts": {
                "summary_csv": str(summary_path),
                "by_window_csv": str(by_window_path),
                "diagnostics_csv": str(diagnostics_path),
                "report_md": str(report_path),
                "qa_fss_png": str(fss_plot) if fss_plot else "",
                "qa_nonzero_png": str(nonzero_plot) if nonzero_plot else "",
                "qa_overlays_png": str(overlay_plot) if overlay_plot else "",
            },
            "completed_runs": summary_df[summary_df["status"] == "completed"].to_dict(orient="records"),
            "failed_runs": summary_df[summary_df["status"] != "completed"].to_dict(orient="records"),
        }
        manifest_path = self.output_root / "convergence_after_shoreline_run_manifest.json"
        _write_json(manifest_path, manifest)

        if not (summary_df["status"] == "completed").any():
            raise RuntimeError(
                "No shoreline-aware convergence runs completed successfully. "
                f"See {manifest_path} for stop reasons."
            )

        return {
            "summary_csv": str(summary_path),
            "by_window_csv": str(by_window_path),
            "diagnostics_csv": str(diagnostics_path),
            "report_md": str(report_path),
            "run_manifest_json": str(manifest_path),
            "qa_fss_png": str(fss_plot) if fss_plot else "",
            "qa_nonzero_png": str(nonzero_plot) if nonzero_plot else "",
            "qa_overlays_png": str(overlay_plot) if overlay_plot else "",
            "recommendation": recommendation,
        }


def run_convergence_after_shoreline() -> dict:
    return ConvergenceAfterShorelineService().run()
