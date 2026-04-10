"""Read-only final validation package builder for completed Mindoro and DWH outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PHASE = "final_validation_package"
OUTPUT_DIR = Path("output") / PHASE

MINDORO_CASE_ID = "CASE_MINDORO_RETRO_2023"
DWH_CASE_ID = "CASE_DWH_RETRO_2010_72H"

MINDORO_DIR = Path("output") / MINDORO_CASE_ID
DWH_DIR = Path("output") / DWH_CASE_ID

TRACK_SEQUENCE = {"A": 1, "B1": 2, "B2": 3, "C1": 4, "C2": 5, "C3": 6}
FSS_WINDOWS_KM = (1, 3, 5, 10)
DWH_TRACK_LABELS = {
    "opendrift_control": "OpenDrift deterministic control",
    "ensemble_p50": "OpenDrift ensemble p50",
    "ensemble_p90": "OpenDrift ensemble p90",
    "pygnome_deterministic": "PyGNOME deterministic comparator",
}


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    return value


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required artifact is missing: {path}")
    return pd.read_csv(path)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required artifact is missing: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def mean_fss(record: pd.Series | dict[str, Any]) -> float:
    values: list[float] = []
    for window in FSS_WINDOWS_KM:
        try:
            value = float(record.get(f"fss_{window}km", np.nan))
        except (TypeError, ValueError):
            value = np.nan
        if np.isfinite(value):
            values.append(value)
    return float(np.mean(values)) if values else 0.0


def decide_final_structure() -> str:
    return (
        "Main text should emphasize Mindoro B1 as the sparse Philippine stress test and DWH Phase 3C as the "
        "rich-data transfer-validation success; comparative discussion should emphasize that PyGNOME leads the "
        "Mindoro March 4-6 comparator benchmark while OpenDrift deterministic and ensemble lead the DWH external "
        "case with mixed deterministic-vs-ensemble leadership; appendix and sensitivity sections should retain the "
        "Mindoro broader-support appendix, recipe/init/source-history sensitivities, and any future DWH threshold "
        "or harmonization extensions."
    )


class FinalValidationPackageService:
    def __init__(self):
        self.output_dir = OUTPUT_DIR
        self.required_paths = [
            MINDORO_DIR / "phase3b" / "phase3b_summary.csv",
            MINDORO_DIR / "phase3b" / "phase3b_pairing_manifest.csv",
            MINDORO_DIR / "public_obs_appendix" / "public_obs_inventory.csv",
            MINDORO_DIR / "public_obs_appendix" / "appendix_perdate_summary.csv",
            MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_fss_by_window.csv",
            MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_diagnostics.csv",
            MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_summary.csv",
            MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_eventcorridor_summary.csv",
            MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_model_ranking.csv",
            MINDORO_DIR / "recipe_sensitivity_r1_multibranch" / "recipe_sensitivity_r1_multibranch_ranking.csv",
            MINDORO_DIR / "recipe_sensitivity_r1_multibranch" / "recipe_sensitivity_r1_multibranch_run_manifest.json",
            MINDORO_DIR / "init_mode_sensitivity_r1" / "init_mode_sensitivity_r1_run_manifest.json",
            MINDORO_DIR / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_summary.csv",
            MINDORO_DIR / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_run_manifest.json",
            MINDORO_DIR / "phase3b_extended_public" / "phase3b_extended_summary.csv",
            DWH_DIR / "phase3c_external_case_setup" / "external_case_source_taxonomy.csv",
            DWH_DIR / "phase3c_external_case_setup" / "external_case_service_inventory.csv",
            DWH_DIR / "phase3c_external_case_setup" / "phase3c_external_case_setup_manifest.json",
            DWH_DIR / "dwh_phase3c_scientific_forcing_ready" / "dwh_scientific_forcing_status.csv",
            DWH_DIR / "phase3c_external_case_run" / "phase3c_summary.csv",
            DWH_DIR / "phase3c_external_case_run" / "phase3c_eventcorridor_summary.csv",
            DWH_DIR / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_summary.csv",
            DWH_DIR / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_eventcorridor_summary.csv",
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_summary.csv",
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_eventcorridor_summary.csv",
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_run_manifest.json",
        ]

    def _assert_required_artifacts(self) -> None:
        missing = [str(path) for path in self.required_paths if not path.exists()]
        if missing:
            raise FileNotFoundError("Final validation packaging is missing required artifacts: " + "; ".join(missing))

    def _load_inputs(self) -> None:
        self.phase3b_summary = _read_csv(MINDORO_DIR / "phase3b" / "phase3b_summary.csv")
        self.phase3b_pairing = _read_csv(MINDORO_DIR / "phase3b" / "phase3b_pairing_manifest.csv")
        self.public_obs_inventory = _read_csv(MINDORO_DIR / "public_obs_appendix" / "public_obs_inventory.csv")
        self.appendix_perdate_summary = _read_csv(MINDORO_DIR / "public_obs_appendix" / "appendix_perdate_summary.csv")
        self.appendix_eventcorridor_fss = _read_csv(
            MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_fss_by_window.csv"
        )
        self.appendix_eventcorridor_diag = _read_csv(
            MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_diagnostics.csv"
        )
        self.mindoro_pygnome_summary = _read_csv(
            MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_summary.csv"
        )
        self.mindoro_pygnome_event = _read_csv(
            MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_eventcorridor_summary.csv"
        )
        self.mindoro_pygnome_ranking = _read_csv(
            MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_model_ranking.csv"
        )
        self.recipe_ranking = _read_csv(
            MINDORO_DIR / "recipe_sensitivity_r1_multibranch" / "recipe_sensitivity_r1_multibranch_ranking.csv"
        )
        self.recipe_manifest = _read_json(
            MINDORO_DIR / "recipe_sensitivity_r1_multibranch" / "recipe_sensitivity_r1_multibranch_run_manifest.json"
        )
        self.init_manifest = _read_json(
            MINDORO_DIR / "init_mode_sensitivity_r1" / "init_mode_sensitivity_r1_run_manifest.json"
        )
        self.source_history_summary = _read_csv(
            MINDORO_DIR / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_summary.csv"
        )
        self.source_history_manifest = _read_json(
            MINDORO_DIR / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_run_manifest.json"
        )
        self.extended_public_summary = _read_csv(
            MINDORO_DIR / "phase3b_extended_public" / "phase3b_extended_summary.csv"
        )
        self.dwh_source_taxonomy = _read_csv(
            DWH_DIR / "phase3c_external_case_setup" / "external_case_source_taxonomy.csv"
        )
        self.dwh_service_inventory = _read_csv(
            DWH_DIR / "phase3c_external_case_setup" / "external_case_service_inventory.csv"
        )
        self.dwh_setup_manifest = _read_json(
            DWH_DIR / "phase3c_external_case_setup" / "phase3c_external_case_setup_manifest.json"
        )
        self.dwh_forcing_status = _read_csv(
            DWH_DIR / "dwh_phase3c_scientific_forcing_ready" / "dwh_scientific_forcing_status.csv"
        )
        self.dwh_deterministic_summary = _read_csv(
            DWH_DIR / "phase3c_external_case_run" / "phase3c_summary.csv"
        )
        self.dwh_deterministic_event = _read_csv(
            DWH_DIR / "phase3c_external_case_run" / "phase3c_eventcorridor_summary.csv"
        )
        self.dwh_ensemble_summary = _read_csv(
            DWH_DIR / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_summary.csv"
        )
        self.dwh_ensemble_event = _read_csv(
            DWH_DIR / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_eventcorridor_summary.csv"
        )
        self.dwh_cross_model_summary = _read_csv(
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_summary.csv"
        )
        self.dwh_cross_model_event = _read_csv(
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_eventcorridor_summary.csv"
        )
        self.dwh_cross_model_manifest = _read_json(
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_run_manifest.json"
        )

    @staticmethod
    def _coerce_value(value: Any) -> Any:
        if pd.isna(value):
            return ""
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating, float)):
            return float(value)
        return value

    def _format_validation_dates(self, row: pd.Series | dict[str, Any]) -> str:
        explicit = str(row.get("validation_dates", "") or "").strip()
        if explicit:
            return explicit
        obs_date = str(row.get("obs_date", "") or "").strip()
        if obs_date:
            return obs_date
        pair_id = str(row.get("pair_id", "") or "")
        if "2010-05-21_2010-05-23" in pair_id:
            return "2010-05-21_to_2010-05-23"
        for date in ("2010-05-21", "2010-05-22", "2010-05-23", "2023-03-06"):
            if pair_id.endswith(date):
                return date
        if "2023-03-04_to_2023-03-06" in pair_id:
            return "2023-03-04_to_2023-03-06"
        if "2023-03-03_to_2023-03-06" in pair_id:
            return "2023-03-03_to_2023-03-06"
        return ""

    def _build_dwh_main_row(self, row: pd.Series, track_id: str) -> dict[str, Any]:
        track_name = str(row.get("track_id", ""))
        model_comparator = DWH_TRACK_LABELS.get(track_name, str(row.get("track_id", "")))
        if track_id == "C1":
            track_label = "DWH deterministic external transfer validation"
            notes = "Scientific deterministic OpenDrift control using the frozen real historical forcing stack."
        elif track_id == "C2":
            track_label = "DWH ensemble extension and deterministic-vs-ensemble comparison"
            if track_name == "ensemble_p50":
                notes = "Scientific ensemble p50 track; strongest DWH overall mean FSS across per-date and event-corridor rows."
            else:
                notes = "Scientific ensemble threshold track used for comparison against deterministic and p50."
        else:
            track_label = "DWH PyGNOME comparator against the same DWH truth masks"
            notes = (
                "Comparator only; DWH observed masks remain truth. PyGNOME wave/Stokes handling is not identical to the "
                "OpenDrift scientific stack and should be interpreted as a cross-model comparison, not as truth."
            )
        transport_model = "pygnome" if track_name == "pygnome_deterministic" else "opendrift_oceandrift"
        return {
            "case_id": DWH_CASE_ID,
            "track_id": track_id,
            "track_label": track_label,
            "model_comparator": model_comparator,
            "validation_dates": self._format_validation_dates(row),
            "result_scope": str(row.get("pair_role", "")),
            "fss_1km": float(row["fss_1km"]),
            "fss_3km": float(row["fss_3km"]),
            "fss_5km": float(row["fss_5km"]),
            "fss_10km": float(row["fss_10km"]),
            "mean_fss": mean_fss(row),
            "iou": float(row["iou"]),
            "dice": float(row["dice"]),
            "centroid_distance_m": self._coerce_value(row["centroid_distance_m"]),
            "forecast_nonzero_cells": int(row["forecast_nonzero_cells"]),
            "obs_nonzero_cells": int(row["obs_nonzero_cells"]),
            "transport_model": transport_model,
            "provisional_transport_model": bool(row.get("provisional_transport_model", True)),
            "shoreline_mask_status": "dwh_epsg32616_scoring_grid_with_sea_mask_applied",
            "notes": notes,
            "source_summary_path": str(
                DWH_DIR
                / (
                    "phase3c_dwh_pygnome_comparator/phase3c_dwh_pygnome_summary.csv"
                    if track_id == "C3"
                    else (
                        "phase3c_external_case_ensemble_comparison/phase3c_ensemble_summary.csv"
                        if track_id == "C2"
                        else "phase3c_external_case_run/phase3c_summary.csv"
                    )
                )
            ),
            "source_pairing_path": "",
            "forecast_product_type": str(row.get("run_type", row.get("track_id", ""))),
        }

    def _build_main_table(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []

        strict_row = self.phase3b_summary[self.phase3b_summary["pair_id"].astype(str) == "official_primary_march6"].iloc[0]
        strict_pairing = self.phase3b_pairing[self.phase3b_pairing["pair_id"].astype(str) == "official_primary_march6"].iloc[0]
        rows.append(
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": "B1",
                "track_label": "Mindoro strict March 6 single-date stress test",
                "model_comparator": "OpenDrift ensemble p50 official primary",
                "validation_dates": "2023-03-06",
                "result_scope": "strict_single_date",
                "fss_1km": float(strict_row["fss_1km"]),
                "fss_3km": float(strict_row["fss_3km"]),
                "fss_5km": float(strict_row["fss_5km"]),
                "fss_10km": float(strict_row["fss_10km"]),
                "mean_fss": mean_fss(strict_row),
                "iou": float(strict_row["iou"]),
                "dice": float(strict_row["dice"]),
                "centroid_distance_m": self._coerce_value(strict_row["centroid_distance_m"]),
                "forecast_nonzero_cells": int(strict_row["forecast_nonzero_cells"]),
                "obs_nonzero_cells": int(strict_row["obs_nonzero_cells"]),
                "transport_model": "oceandrift",
                "provisional_transport_model": bool(strict_row["provisional_transport_model"]),
                "shoreline_mask_status": "canonical_mindoro_scoring_grid_ocean_mask_applied",
                "notes": (
                    "Sparse hard stress test using the accepted WWF March 6 validation polygon; official primary "
                    "track is the p50 date-composite mask and remains provisional."
                ),
                "source_summary_path": str(MINDORO_DIR / "phase3b" / "phase3b_summary.csv"),
                "source_pairing_path": str(MINDORO_DIR / "phase3b" / "phase3b_pairing_manifest.csv"),
                "forecast_product_type": str(strict_pairing["forecast_product_type"]),
            }
        )

        appendix_row = self.appendix_eventcorridor_diag.iloc[0]
        rows.append(
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": "B2",
                "track_label": "Mindoro broader public-observation / event-corridor support",
                "model_comparator": "OpenDrift ensemble p50 appendix support union",
                "validation_dates": "2023-03-03_to_2023-03-06",
                "result_scope": "event_corridor_support",
                "fss_1km": float(appendix_row["fss_1km"]),
                "fss_3km": float(appendix_row["fss_3km"]),
                "fss_5km": float(appendix_row["fss_5km"]),
                "fss_10km": float(appendix_row["fss_10km"]),
                "mean_fss": mean_fss(appendix_row),
                "iou": float(appendix_row["iou"]),
                "dice": float(appendix_row["dice"]),
                "centroid_distance_m": self._coerce_value(appendix_row["centroid_distance_m"]),
                "forecast_nonzero_cells": int(appendix_row["forecast_nonzero_cells"]),
                "obs_nonzero_cells": int(appendix_row["obs_nonzero_cells"]),
                "transport_model": "oceandrift",
                "provisional_transport_model": True,
                "shoreline_mask_status": "canonical_mindoro_scoring_grid_ocean_mask_applied",
                "notes": (
                    "Secondary appendix-only within-horizon public observation union. This supports broader spatial "
                    "context and should not be confused with the strict March 6 single-date test."
                ),
                "source_summary_path": str(MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_diagnostics.csv"),
                "source_pairing_path": str(MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_pairing_manifest.csv"),
                "forecast_product_type": "appendix_eventcorridor_model_union",
            }
        )

        for _, row in self.mindoro_pygnome_event.iterrows():
            rows.append(
                {
                    "case_id": MINDORO_CASE_ID,
                    "track_id": "A",
                    "track_label": "Mindoro Phase 3A benchmark comparator",
                    "model_comparator": str(row["model_name"]),
                    "validation_dates": self._format_validation_dates(row),
                    "result_scope": "event_corridor_benchmark",
                    "fss_1km": float(row["fss_1km"]),
                    "fss_3km": float(row["fss_3km"]),
                    "fss_5km": float(row["fss_5km"]),
                    "fss_10km": float(row["fss_10km"]),
                    "mean_fss": mean_fss(row),
                    "iou": float(row["iou"]),
                    "dice": float(row["dice"]),
                    "centroid_distance_m": self._coerce_value(row["centroid_distance_m"]),
                    "forecast_nonzero_cells": int(row["forecast_nonzero_cells"]),
                    "obs_nonzero_cells": int(row["obs_nonzero_cells"]),
                    "transport_model": str(row["transport_model"]),
                    "provisional_transport_model": bool(row["provisional_transport_model"]),
                    "shoreline_mask_status": "canonical_mindoro_scoring_grid_ocean_mask_applied",
                    "notes": (
                        "Comparator track only; accepted public observation-derived event-corridor masks remain truth. "
                        + str(row.get("structural_limitations", "") or "")
                    ).strip(),
                    "source_summary_path": str(MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_eventcorridor_summary.csv"),
                    "source_pairing_path": str(MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_pairing_manifest.csv"),
                    "forecast_product_type": str(row["forecast_product"]),
                }
            )

        for _, row in self.dwh_deterministic_summary.iterrows():
            rows.append(self._build_dwh_main_row(row, track_id="C1"))
        dwh_ensemble_rows = self.dwh_ensemble_summary[
            self.dwh_ensemble_summary["track_id"].astype(str).isin(["ensemble_p50", "ensemble_p90"])
        ]
        for _, row in dwh_ensemble_rows.iterrows():
            rows.append(self._build_dwh_main_row(row, track_id="C2"))
        dwh_pygnome_rows = self.dwh_cross_model_summary[
            self.dwh_cross_model_summary["track_id"].astype(str) == "pygnome_deterministic"
        ]
        for _, row in dwh_pygnome_rows.iterrows():
            rows.append(self._build_dwh_main_row(row, track_id="C3"))

        table = pd.DataFrame(rows)
        table["track_order"] = table["track_id"].map(TRACK_SEQUENCE).fillna(99)
        table.sort_values(["track_order", "case_id", "model_comparator", "validation_dates"], inplace=True)
        table.drop(columns=["track_order"], inplace=True)
        return table

    def _build_case_registry(self) -> pd.DataFrame:
        rows = [
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": "A",
                "track_label": "Mindoro Phase 3A benchmark comparator",
                "status": "complete",
                "truth_source": "accepted public observation-derived event-corridor masks",
                "primary_output_dir": str(MINDORO_DIR / "pygnome_public_comparison"),
                "reporting_role": "comparative discussion",
                "main_text_priority": "secondary",
                "notes": "Comparator role only. PyGNOME is not truth.",
            },
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": "B1",
                "track_label": "Mindoro strict March 6 single-date stress test",
                "status": "complete",
                "truth_source": "accepted WWF March 6 validation mask",
                "primary_output_dir": str(MINDORO_DIR / "phase3b"),
                "reporting_role": "main-text stress test",
                "main_text_priority": "primary",
                "notes": "Sparse hard stress test with only two ocean cells in the accepted March 6 observation mask.",
            },
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": "B2",
                "track_label": "Mindoro broader public-observation / event-corridor support",
                "status": "complete",
                "truth_source": "accepted within-horizon public observation union",
                "primary_output_dir": str(MINDORO_DIR / "public_obs_appendix"),
                "reporting_role": "supporting interpretation",
                "main_text_priority": "secondary",
                "notes": "Broader support track only; do not confuse with the strict March 6 test.",
            },
            {
                "case_id": DWH_CASE_ID,
                "track_id": "C1",
                "track_label": "DWH deterministic external transfer validation",
                "status": "complete",
                "truth_source": "DWH daily public observation-derived masks for 2010-05-21 to 2010-05-23",
                "primary_output_dir": str(DWH_DIR / "phase3c_external_case_run"),
                "reporting_role": "main-text scientific result",
                "main_text_priority": "primary",
                "notes": "Real historical HYCOM + ERA5 + CMEMS wave/Stokes forcing stack.",
            },
            {
                "case_id": DWH_CASE_ID,
                "track_id": "C2",
                "track_label": "DWH ensemble extension and deterministic-vs-ensemble comparison",
                "status": "complete",
                "truth_source": "same DWH daily public masks as C1",
                "primary_output_dir": str(DWH_DIR / "phase3c_external_case_ensemble_comparison"),
                "reporting_role": "comparative discussion",
                "main_text_priority": "primary",
                "notes": "p50 leads by overall mean FSS; deterministic remains strongest on the May 21-23 event corridor.",
            },
            {
                "case_id": DWH_CASE_ID,
                "track_id": "C3",
                "track_label": "DWH PyGNOME comparator",
                "status": "complete",
                "truth_source": "same DWH daily public masks as C1",
                "primary_output_dir": str(DWH_DIR / "phase3c_dwh_pygnome_comparator"),
                "reporting_role": "comparative discussion",
                "main_text_priority": "secondary",
                "notes": "Comparator only; DWH observed masks remain truth.",
            },
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": "appendix_sensitivity",
                "track_label": "Mindoro recipe/init/source-history sensitivities",
                "status": "reviewed_not_promoted",
                "truth_source": "same accepted public observation-derived masks used by the sensitivity branches",
                "primary_output_dir": "; ".join(
                    [
                        str(MINDORO_DIR / "recipe_sensitivity_r1_multibranch"),
                        str(MINDORO_DIR / "init_mode_sensitivity_r1"),
                        str(MINDORO_DIR / "source_history_reconstruction_r1"),
                    ]
                ),
                "reporting_role": "appendix_only",
                "main_text_priority": "appendix",
                "notes": "Sensitivity branches remain informative but do not replace the main thesis tracks.",
            },
        ]
        return pd.DataFrame(rows)

    def _build_benchmark_table(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for _, row in self.mindoro_pygnome_ranking.iterrows():
            rows.append(
                {
                    "case_id": MINDORO_CASE_ID,
                    "benchmark_context": "Mindoro Phase 3A event-corridor comparator benchmark",
                    "track_id": "A",
                    "model_comparator": str(row["model_name"]),
                    "eventcorridor_mean_fss": float(row["eventcorridor_mean_fss"]),
                    "eventcorridor_iou": float(row["eventcorridor_iou"]),
                    "eventcorridor_dice": float(row["eventcorridor_dice"]),
                    "eventcorridor_rank": int(row["eventcorridor_rank"]),
                    "strict_march6_mean_fss": float(row["strict_march6_mean_fss"]),
                    "multidate_mean_fss": float(row["multidate_mean_fss"]),
                    "truth_source": "accepted public observation-derived event-corridor masks",
                    "notes": str(row["structural_limitations"]),
                }
            )

        dwh_rows = self.dwh_cross_model_summary.copy()
        dwh_rows["mean_fss"] = dwh_rows.apply(mean_fss, axis=1)
        for track_id, group in dwh_rows.groupby("track_id", dropna=False):
            event_group = group[group["pair_role"].astype(str) == "event_corridor"]
            event_mean = float(event_group["mean_fss"].mean()) if not event_group.empty else float(group["mean_fss"].mean())
            overall_mean = float(group["mean_fss"].mean())
            event_row = event_group.iloc[0] if not event_group.empty else group.iloc[0]
            rows.append(
                {
                    "case_id": DWH_CASE_ID,
                    "benchmark_context": "DWH Phase 3C cross-model validation benchmark",
                    "track_id": "C1/C2/C3",
                    "model_comparator": DWH_TRACK_LABELS.get(str(track_id), str(track_id)),
                    "eventcorridor_mean_fss": event_mean,
                    "eventcorridor_iou": float(event_row["iou"]),
                    "eventcorridor_dice": float(event_row["dice"]),
                    "overall_mean_fss": overall_mean,
                    "truth_source": "DWH daily public observation-derived masks",
                    "notes": "Comparator only." if str(track_id) == "pygnome_deterministic" else "Scientific OpenDrift track.",
                }
            )

        table = pd.DataFrame(rows)
        table["rank_by_context"] = 0
        for case_id in (MINDORO_CASE_ID, DWH_CASE_ID):
            ordered_index = table[table["case_id"].astype(str) == case_id].sort_values(
                "eventcorridor_mean_fss", ascending=False
            ).index
            for rank, row_index in enumerate(ordered_index, start=1):
                table.at[row_index, "rank_by_context"] = rank
        table.sort_values(["case_id", "rank_by_context", "model_comparator"], inplace=True)
        return table

    def _build_observation_table(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        mindoro_inventory = self.public_obs_inventory.copy()
        mindoro_inventory["obs_date"] = mindoro_inventory["obs_date"].astype(str)
        within_horizon = mindoro_inventory[
            mindoro_inventory["obs_date"].isin(["2023-03-03", "2023-03-04", "2023-03-05", "2023-03-06"])
        ]
        for _, row in within_horizon.iterrows():
            quantitative = bool(row.get("accept_for_appendix_quantitative", False))
            rows.append(
                {
                    "case_id": MINDORO_CASE_ID,
                    "track_id": "B1" if str(row["obs_date"]) == "2023-03-06" else "B2",
                    "observation_date": str(row["obs_date"]),
                    "source_name": str(row["source_name"]),
                    "provider": str(row["provider"]),
                    "source_type": str(row["source_type"]),
                    "truth_status": (
                        "strict_truth"
                        if str(row["obs_date"]) == "2023-03-06" and quantitative
                        else "broader_support_truth_candidate"
                        if quantitative
                        else "context_only"
                    ),
                    "observation_usage": (
                        "strict_single_date_validation"
                        if str(row["obs_date"]) == "2023-03-06" and quantitative
                        else "broader_public_support"
                        if quantitative
                        else "qualitative_context"
                    ),
                    "machine_readable": bool(row["machine_readable"]),
                    "observation_derived": bool(row["observation_derived"]),
                    "within_current_72h_horizon": bool(row["within_current_72h_horizon"]),
                    "source_url": str(row["source_url"]),
                    "service_url": str(row["service_url"]),
                    "notes": str(row.get("notes", "") or row.get("rejection_reason", "") or ""),
                }
            )

        dwh_selected = self.dwh_source_taxonomy[self.dwh_source_taxonomy["selected_for_phase3c"].astype(bool)].copy()
        dwh_primary_service = self.dwh_service_inventory[
            self.dwh_service_inventory["service_role"].astype(str) == "public_observation_primary"
        ]["service_url"].iloc[0]
        for _, row in dwh_selected.iterrows():
            role = str(row["role"])
            rows.append(
                {
                    "case_id": DWH_CASE_ID,
                    "track_id": "C1/C2/C3",
                    "observation_date": str(row.get("event_date", "") or ""),
                    "source_name": str(row["layer_name"]),
                    "provider": "DWH FeatureServer",
                    "source_type": str(row["geometry_type"]),
                    "truth_status": (
                        "daily_truth"
                        if bool(row["use_as_truth"])
                        else "initialization_reference"
                        if role == "initialization_polygon"
                        else "context_only"
                    ),
                    "observation_usage": (
                        "daily_validation_mask"
                        if bool(row["use_as_truth"])
                        else "initialization_reference"
                        if role == "initialization_polygon"
                        else "provenance_only"
                    ),
                    "machine_readable": True,
                    "observation_derived": str(row["source_taxonomy"]) == "observation_derived_quantitative",
                    "within_current_72h_horizon": str(row.get("event_date", "") or "") in {"2010-05-20", "2010-05-21", "2010-05-22", "2010-05-23"},
                    "source_url": str(dwh_primary_service),
                    "service_url": str(dwh_primary_service),
                    "notes": str(row["truth_handling"]),
                }
            )
        table = pd.DataFrame(rows)
        table.sort_values(["case_id", "observation_date", "source_name"], inplace=True)
        return table

    def _build_limitations_table(self) -> pd.DataFrame:
        best_recipe = self.recipe_ranking[self.recipe_ranking["model_family"].astype(str) == "OpenDrift"].sort_values(
            "eventcorridor_mean_fss", ascending=False
        ).iloc[0]
        best_source_history = self.source_history_summary[
            self.source_history_summary["pair_role"].astype(str) == "march3_reconstruction_checkpoint"
        ].copy()
        best_source_history["mean_fss"] = best_source_history.apply(mean_fss, axis=1)
        best_source_history_row = best_source_history.sort_values("mean_fss", ascending=False).iloc[0]
        extended_row = self.extended_public_summary.iloc[0]

        rows = [
            {
                "limitation_id": "M1",
                "case_id": MINDORO_CASE_ID,
                "track_id": "B1",
                "category": "observation_sparsity",
                "statement": "Mindoro strict March 6 is a sparse hard stress test with only two observed ocean cells.",
                "implication": "Do not overgeneralize zero-overlap March 6 scores to the broader public-observation story.",
                "source_artifact": str(MINDORO_DIR / "phase3b" / "phase3b_summary.csv"),
            },
            {
                "limitation_id": "M2",
                "case_id": MINDORO_CASE_ID,
                "track_id": "B2",
                "category": "track_separation",
                "statement": "The broader public-observation support union is not the same test as the strict March 6 single-date validation.",
                "implication": "Keep B2 as supporting context rather than a replacement for B1.",
                "source_artifact": str(MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_diagnostics.csv"),
            },
            {
                "limitation_id": "M3",
                "case_id": MINDORO_CASE_ID,
                "track_id": "A",
                "category": "comparator_role",
                "statement": "PyGNOME is a comparator in Mindoro Phase 3A and is not the truth source.",
                "implication": "Benchmark interpretations must remain model-vs-observation, not model-vs-model truth claims.",
                "source_artifact": str(MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_eventcorridor_summary.csv"),
            },
            {
                "limitation_id": "M4",
                "case_id": MINDORO_CASE_ID,
                "track_id": "appendix_sensitivity",
                "category": "recipe_sensitivity",
                "statement": (
                    f"The best OpenDrift sensitivity branch ({best_recipe['track_id']}) reached eventcorridor mean FSS "
                    f"{float(best_recipe['eventcorridor_mean_fss']):.4f}, still below the fixed PyGNOME comparator at "
                    f"{float(self.recipe_manifest['recommendation']['pygnome_eventcorridor_mean_fss']):.4f}."
                ),
                "implication": "Recipe choice alone was not enough to overturn the Mindoro benchmark result.",
                "source_artifact": str(MINDORO_DIR / "recipe_sensitivity_r1_multibranch" / "recipe_sensitivity_r1_multibranch_ranking.csv"),
            },
            {
                "limitation_id": "M5",
                "case_id": MINDORO_CASE_ID,
                "track_id": "appendix_sensitivity",
                "category": "source_history_reconstruction",
                "statement": (
                    f"Source-history reconstruction improved the March 3 checkpoint most strongly for {best_source_history_row['pair_id']} "
                    f"(mean FSS {float(best_source_history_row['mean_fss']):.4f}) but did not materially improve strict March 6 or event-corridor skill."
                ),
                "implication": "Keep A2 source-history work in appendix/sensitivity framing rather than promoting it to the main case definition.",
                "source_artifact": str(MINDORO_DIR / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_run_manifest.json"),
            },
            {
                "limitation_id": "M6",
                "case_id": MINDORO_CASE_ID,
                "track_id": "appendix_sensitivity",
                "category": "initialization_sensitivity",
                "statement": str(self.init_manifest["recommendation"]["reason"]),
                "implication": "Keep observation-initialized and reconstruction-initialized branches separate in the narrative.",
                "source_artifact": str(MINDORO_DIR / "init_mode_sensitivity_r1" / "init_mode_sensitivity_r1_run_manifest.json"),
            },
            {
                "limitation_id": "M7",
                "case_id": MINDORO_CASE_ID,
                "track_id": "appendix_sensitivity",
                "category": "extended_horizon",
                "statement": str(extended_row["reason"]),
                "implication": "Extended-horizon public observation support remains appendix-only until forcing windows are expanded.",
                "source_artifact": str(MINDORO_DIR / "phase3b_extended_public" / "phase3b_extended_summary.csv"),
            },
            {
                "limitation_id": "D1",
                "case_id": DWH_CASE_ID,
                "track_id": "C1/C2/C3",
                "category": "date_composite_logic",
                "statement": "DWH public layers support date-composite logic, not defensible exact sub-daily acquisition times.",
                "implication": "Phase 3C claims must remain date-composite honest rather than inventing exact observation times.",
                "source_artifact": str(DWH_DIR / "phase3c_external_case_setup" / "chapter3_phase3c_external_case_memo.md"),
            },
            {
                "limitation_id": "D2",
                "case_id": DWH_CASE_ID,
                "track_id": "C3",
                "category": "cross_model_harmonization",
                "statement": "The DWH PyGNOME comparator does not reproduce OpenDrift wave/Stokes handling with exact parity.",
                "implication": "Use PyGNOME as a comparator only, not as a forcing-identical twin.",
                "source_artifact": str(DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_run_manifest.json"),
            },
            {
                "limitation_id": "D3",
                "case_id": DWH_CASE_ID,
                "track_id": "C2",
                "category": "case_dependent_ensemble_benefit",
                "statement": "On DWH, ensemble p50 leads by overall mean FSS while deterministic remains strongest on the May 21-23 event corridor.",
                "implication": "Present ensemble benefit as case-dependent rather than universal.",
                "source_artifact": str(DWH_DIR / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_summary.csv"),
            },
        ]
        return pd.DataFrame(rows)

    def _build_headlines(self, main_table: pd.DataFrame) -> dict[str, dict[str, Any]]:
        def _row_for(track_id: str, model_contains: str | None = None, validation_dates: str | None = None) -> pd.Series:
            subset = main_table[main_table["track_id"].astype(str) == track_id]
            if model_contains:
                subset = subset[subset["model_comparator"].astype(str).str.contains(model_contains, case=False, regex=False)]
            if validation_dates:
                subset = subset[subset["validation_dates"].astype(str) == validation_dates]
            if subset.empty:
                raise ValueError(f"Headline row not found for track {track_id}, model filter {model_contains}, validation {validation_dates}")
            return subset.iloc[0]

        return {
            "mindoro_strict": _row_for("B1").to_dict(),
            "mindoro_broader_support": _row_for("B2").to_dict(),
            "dwh_deterministic_event": _row_for("C1", "OpenDrift deterministic", "2010-05-21_to_2010-05-23").to_dict(),
            "dwh_ensemble_p50_event": _row_for("C2", "ensemble p50", "2010-05-21_to_2010-05-23").to_dict(),
            "dwh_ensemble_p90_event": _row_for("C2", "ensemble p90", "2010-05-21_to_2010-05-23").to_dict(),
            "dwh_pygnome_event": _row_for("C3", "PyGNOME", "2010-05-21_to_2010-05-23").to_dict(),
            "mindoro_benchmark_top": self.mindoro_pygnome_ranking.sort_values("eventcorridor_rank").iloc[0].to_dict(),
            "dwh_eventcorridor_top": self.dwh_cross_model_event.assign(mean_fss=self.dwh_cross_model_event.apply(mean_fss, axis=1))
            .sort_values("mean_fss", ascending=False)
            .iloc[0]
            .to_dict(),
            "dwh_overall_mean_top": self.dwh_cross_model_summary.assign(mean_fss=self.dwh_cross_model_summary.apply(mean_fss, axis=1))
            .groupby("track_id", dropna=False)["mean_fss"]
            .mean()
            .sort_values(ascending=False)
            .head(1)
            .rename("overall_mean_fss")
            .reset_index()
            .assign(model_comparator=lambda df: df["track_id"].map(DWH_TRACK_LABELS))
            .iloc[0]
            .to_dict(),
        }

    def _write_claims_guardrails(self) -> Path:
        path = self.output_dir / "final_validation_claims_guardrails.md"
        lines = [
            "# Final Validation Claims Guardrails",
            "",
            "- Mindoro strict March 6 is a sparse hard stress test and should be described that way in the thesis.",
            "- Mindoro broader public-observation support should not be confused with the strict March 6 single-date test.",
            "- PyGNOME is a comparator, not truth, in both the Mindoro benchmark and the DWH cross-model comparison.",
            "- DWH observed masks are truth for Phase 3C.",
            "- DWH currently demonstrates workflow transferability and meaningful spatial skill under real historical forcing.",
            "- On DWH, OpenDrift outperforms PyGNOME under the current case definition.",
            "- On DWH, ensemble p50 improves overall mean FSS while deterministic remains strongest on the May 21-23 event corridor.",
            "- DWH Phase 3C is scientifically reportable even if some optional future extensions remain.",
            "- Do not relabel appendix-only broader-support or sensitivity products as replacements for the strict Mindoro stress test.",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def _write_chapter_sync_memo(self) -> Path:
        path = self.output_dir / "final_validation_chapter_sync_memo.md"
        lines = [
            "# Final Validation Chapter 3 Sync Memo",
            "",
            "Recommended revised structure:",
            "",
            "1. Phase 1 = Transport Validation and Baseline Configuration Selection",
            "2. Phase 2 = Standardized Machine-Readable Forecast Product Generation",
            "3. Phase 3A = Mindoro Cross-Model Spatial Benchmarking with PyGNOME",
            "4. Phase 3B1 = Mindoro Strict Single-Date Observation Stress Test (March 6)",
            "5. Phase 3B2 = Mindoro Broader Public-Observation / Event-Corridor Support",
            "6. Phase 3C = External Rich-Data Spill Transfer Validation (Deepwater Horizon 2010)",
            "7. Phase 4 = Oil-Type Fate and Shoreline Impact Analysis",
            "8. Phase 5 = Reproducibility, Packaging, and Deliverables",
            "",
            "Packaging guidance:",
            "",
            "- Keep Mindoro as the main Philippine case.",
            "- Keep DWH as the rich-data external transfer-validation branch.",
            "- Present Phase 3A as comparator-only benchmarking, not as a truth-source replacement.",
            "- Keep the broader-support appendix separate from the strict March 6 stress test.",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def _write_interpretation_memo(self) -> Path:
        path = self.output_dir / "final_validation_interpretation_memo.md"
        lines = [
            "# Final Validation Interpretation Memo",
            "",
            "Key scientific takeaway:",
            "",
            "- Mindoro is the hard sparse-data Philippine stress test.",
            "- DWH is the rich-data external transfer-validation success.",
            "- Ensemble benefit is case-dependent, not universal.",
            "",
            "Interpretation notes:",
            "",
            "- The Mindoro strict March 6 result should be interpreted as a difficult sparse-data edge case rather than as the whole validation story.",
            "- The Mindoro broader-support appendix shows that some spatial support exists in a broader public-observation framing, but it is not the same claim as the strict single-date test.",
            "- The Mindoro comparator benchmark shows PyGNOME leading the March 4-6 event-corridor comparison under the current case definition.",
            "- The DWH external case shows that the workflow transfers to a richer observation setting with meaningful spatial skill.",
            "- On DWH, ensemble p50 improves overall mean FSS, while deterministic retains the strongest event-corridor result.",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def _write_summary(self, headlines: dict[str, dict[str, Any]], recommendation: str) -> Path:
        path = self.output_dir / "final_validation_summary.md"
        lines = [
            "# Final Validation Summary",
            "",
            "This package is read-only with respect to completed scientific outputs. No Mindoro or DWH scientific result files were overwritten.",
            "",
            "## Headline Results",
            "",
            (
                f"- Mindoro strict March 6 (B1): FSS(1/3/5/10 km) = "
                f"{headlines['mindoro_strict']['fss_1km']:.4f}, {headlines['mindoro_strict']['fss_3km']:.4f}, "
                f"{headlines['mindoro_strict']['fss_5km']:.4f}, {headlines['mindoro_strict']['fss_10km']:.4f}; "
                f"IoU={headlines['mindoro_strict']['iou']:.4f}; Dice={headlines['mindoro_strict']['dice']:.4f}."
            ),
            (
                f"- Mindoro broader-support appendix (B2): FSS(1/3/5/10 km) = "
                f"{headlines['mindoro_broader_support']['fss_1km']:.4f}, {headlines['mindoro_broader_support']['fss_3km']:.4f}, "
                f"{headlines['mindoro_broader_support']['fss_5km']:.4f}, {headlines['mindoro_broader_support']['fss_10km']:.4f}; "
                f"IoU={headlines['mindoro_broader_support']['iou']:.4f}; Dice={headlines['mindoro_broader_support']['dice']:.4f}."
            ),
            (
                f"- DWH deterministic event corridor (C1): FSS(1/3/5/10 km) = "
                f"{headlines['dwh_deterministic_event']['fss_1km']:.4f}, {headlines['dwh_deterministic_event']['fss_3km']:.4f}, "
                f"{headlines['dwh_deterministic_event']['fss_5km']:.4f}, {headlines['dwh_deterministic_event']['fss_10km']:.4f}; "
                f"IoU={headlines['dwh_deterministic_event']['iou']:.4f}; Dice={headlines['dwh_deterministic_event']['dice']:.4f}."
            ),
            (
                f"- DWH ensemble p50 event corridor (C2): FSS(1/3/5/10 km) = "
                f"{headlines['dwh_ensemble_p50_event']['fss_1km']:.4f}, {headlines['dwh_ensemble_p50_event']['fss_3km']:.4f}, "
                f"{headlines['dwh_ensemble_p50_event']['fss_5km']:.4f}, {headlines['dwh_ensemble_p50_event']['fss_10km']:.4f}; "
                f"overall mean leader = {headlines['dwh_overall_mean_top']['model_comparator']} "
                f"({headlines['dwh_overall_mean_top']['overall_mean_fss']:.4f})."
            ),
            (
                f"- DWH PyGNOME comparator (C3) event corridor: FSS(1/3/5/10 km) = "
                f"{headlines['dwh_pygnome_event']['fss_1km']:.4f}, {headlines['dwh_pygnome_event']['fss_3km']:.4f}, "
                f"{headlines['dwh_pygnome_event']['fss_5km']:.4f}, {headlines['dwh_pygnome_event']['fss_10km']:.4f}; "
                f"IoU={headlines['dwh_pygnome_event']['iou']:.4f}; Dice={headlines['dwh_pygnome_event']['dice']:.4f}."
            ),
            "",
            "## Recommended Final Structure",
            "",
            "- Main text: Mindoro B1 as the sparse Philippine stress test plus DWH Phase 3C as the rich-data transfer-validation success.",
            "- Comparative discussion: Mindoro Phase 3A comparator benchmark and DWH deterministic-vs-ensemble-vs-PyGNOME comparison.",
            "- Appendix and sensitivities: broader-support appendix, recipe/init/source-history sensitivities, and optional future DWH extensions.",
            "",
            "## Final Recommendation",
            "",
            f"- {recommendation}",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def run(self) -> dict[str, Any]:
        self._assert_required_artifacts()
        self._load_inputs()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        main_table = self._build_main_table()
        case_registry = self._build_case_registry()
        benchmark_table = self._build_benchmark_table()
        observation_table = self._build_observation_table()
        limitations_table = self._build_limitations_table()
        headlines = self._build_headlines(main_table)
        recommendation = decide_final_structure()

        main_table_path = self.output_dir / "final_validation_main_table.csv"
        case_registry_path = self.output_dir / "final_validation_case_registry.csv"
        benchmark_table_path = self.output_dir / "final_validation_benchmark_table.csv"
        observation_table_path = self.output_dir / "final_validation_observation_table.csv"
        limitations_table_path = self.output_dir / "final_validation_limitations.csv"

        main_table.to_csv(main_table_path, index=False)
        case_registry.to_csv(case_registry_path, index=False)
        benchmark_table.to_csv(benchmark_table_path, index=False)
        observation_table.to_csv(observation_table_path, index=False)
        limitations_table.to_csv(limitations_table_path, index=False)

        guardrails_path = self._write_claims_guardrails()
        chapter_sync_path = self._write_chapter_sync_memo()
        interpretation_path = self._write_interpretation_memo()
        summary_path = self._write_summary(headlines, recommendation)

        manifest = {
            "phase": PHASE,
            "output_dir": str(self.output_dir),
            "inputs_preserved": [str(path) for path in self.required_paths],
            "artifacts": {
                "final_validation_main_table": str(main_table_path),
                "final_validation_case_registry": str(case_registry_path),
                "final_validation_benchmark_table": str(benchmark_table_path),
                "final_validation_observation_table": str(observation_table_path),
                "final_validation_limitations": str(limitations_table_path),
                "final_validation_claims_guardrails": str(guardrails_path),
                "final_validation_chapter_sync_memo": str(chapter_sync_path),
                "final_validation_interpretation_memo": str(interpretation_path),
                "final_validation_summary": str(summary_path),
            },
            "headlines": headlines,
            "final_recommendation": recommendation,
            "recommended_final_chapter_structure": [
                "Phase 1 = Transport Validation and Baseline Configuration Selection",
                "Phase 2 = Standardized Machine-Readable Forecast Product Generation",
                "Phase 3A = Mindoro Cross-Model Spatial Benchmarking with PyGNOME",
                "Phase 3B1 = Mindoro Strict Single-Date Observation Stress Test (March 6)",
                "Phase 3B2 = Mindoro Broader Public-Observation / Event-Corridor Support",
                "Phase 3C = External Rich-Data Spill Transfer Validation (Deepwater Horizon 2010)",
                "Phase 4 = Oil-Type Fate and Shoreline Impact Analysis",
                "Phase 5 = Reproducibility, Packaging, and Deliverables",
            ],
        }
        _write_json(self.output_dir / "final_validation_manifest.json", manifest)
        return manifest


def run_final_validation_package() -> dict[str, Any]:
    return FinalValidationPackageService().run()


__all__ = [
    "PHASE",
    "OUTPUT_DIR",
    "FinalValidationPackageService",
    "decide_final_structure",
    "mean_fss",
    "run_final_validation_package",
]
