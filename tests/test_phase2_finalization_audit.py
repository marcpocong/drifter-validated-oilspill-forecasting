import json
import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.services.phase2_finalization_audit import Phase2FinalizationAuditService
from src.utils.io import get_phase2_recipe_family_status


class Phase2FinalizationAuditTests(unittest.TestCase):
    def test_recipe_family_status_reports_partial_official_family(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            forcing_dir = root / "data" / "forcing" / "CASE_MINDORO_RETRO_2023"
            forcing_dir.mkdir(parents=True, exist_ok=True)

            (root / "config" / "recipes.yaml").write_text(
                """
recipes:
  cmems_ncep:
    currents_file: cmems_curr.nc
    wind_file: ncep_wind.nc
    wave_file: cmems_wave.nc
    duration_hours: 72
    time_step_minutes: 60
  cmems_era5:
    currents_file: cmems_curr.nc
    wind_file: era5_wind.nc
    wave_file: cmems_wave.nc
    duration_hours: 72
    time_step_minutes: 60
  hycom_era5:
    currents_file: hycom_curr.nc
    wind_file: era5_wind.nc
    wave_file: cmems_wave.nc
    duration_hours: 72
    time_step_minutes: 60
phase1_recipe_architecture:
  official_recipe_family:
    - cmems_era5
    - cmems_gfs
    - hycom_era5
    - hycom_gfs
  legacy_recipe_name_aliases:
    cmems_ncep:
      chapter3_target_recipe: cmems_gfs
      status: legacy_name_only
""",
                encoding="utf-8",
            )
            (root / "config" / "phase1_baseline_selection.yaml").write_text(
                """
selected_recipe: cmems_era5
chapter3_finalization_audit:
  audit_status:
    classification: implemented_but_provisional
    full_production_rerun_required: true
""",
                encoding="utf-8",
            )
            for filename in ("cmems_curr.nc", "hycom_curr.nc", "era5_wind.nc", "cmems_wave.nc"):
                (forcing_dir / filename).write_text("", encoding="utf-8")

            cwd = os.getcwd()
            try:
                os.chdir(root)
                status = get_phase2_recipe_family_status(
                    run_name="CASE_MINDORO_RETRO_2023",
                    selected_recipe="cmems_era5",
                    config_path=root / "config" / "recipes.yaml",
                    selection_path=root / "config" / "phase1_baseline_selection.yaml",
                )
            finally:
                os.chdir(cwd)

            self.assertEqual(status["official_recipe_family_locally_available"], ["cmems_era5", "hycom_era5"])
            self.assertEqual(
                status["official_recipe_family_unavailable"],
                ["cmems_gfs", "hycom_gfs"],
            )
            self.assertIn("cmems_ncep", status["legacy_recipe_ids_present_in_runtime"])
            self.assertFalse(status["gfs_wind_present_for_active_case"])
            self.assertTrue(status["requires_phase1_production_rerun_for_full_freeze"])
            self.assertTrue(status["legacy_recipe_drift_leaks_into_official_mode"])

    def test_audit_writes_required_outputs_and_marks_phase2_provisional_story(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "docs").mkdir(parents=True, exist_ok=True)
            (root / "src" / "services").mkdir(parents=True, exist_ok=True)
            (root / "src").mkdir(parents=True, exist_ok=True)
            forecast_dir = root / "output" / "CASE_MINDORO_RETRO_2023" / "forecast"
            ensemble_dir = root / "output" / "CASE_MINDORO_RETRO_2023" / "ensemble"
            forcing_dir = root / "data" / "forcing" / "CASE_MINDORO_RETRO_2023"
            forecast_dir.mkdir(parents=True, exist_ok=True)
            ensemble_dir.mkdir(parents=True, exist_ok=True)
            forcing_dir.mkdir(parents=True, exist_ok=True)

            (root / "README.md").write_text(
                "forecast_manifest.json\nphase2_loading_audit.json\nprob_presence_\nmask_p50_\nmask_p90_\nphase2_finalization_audit\n",
                encoding="utf-8",
            )
            (root / "docs" / "PHASE_STATUS.md").write_text("prob_presence_\nmask_p50_\nmask_p90_\n", encoding="utf-8")
            (root / "docs" / "ARCHITECTURE.md").write_text("forecast_manifest.json\nphase2_loading_audit.json\n", encoding="utf-8")
            (root / "docs" / "OUTPUT_CATALOG.md").write_text("prob_presence_\nmask_p50_\nmask_p90_\n", encoding="utf-8")
            (root / "start.ps1").write_text(
                "forecast_manifest.json\nphase2_loading_audit.json\nprob_presence_\nmask_p50_\nmask_p90_\nprototype legacy debug/regression\n",
                encoding="utf-8",
            )
            (root / "src" / "__main__.py").write_text(
                "forecast_manifest.json\nphase2_loading_audit.json\nprob_presence_\nmask_p50_\nmask_p90_\n",
                encoding="utf-8",
            )
            (root / "src" / "services" / "ensemble.py").write_text("phase2_finalization\n", encoding="utf-8")
            (root / "src" / "services" / "source_history_reconstruction_r1.py").write_text(
                "GFS wind forcing was requested but is not available locally.\n",
                encoding="utf-8",
            )
            (root / "src" / "services" / "phase3b_extended_public_scored.py").write_text(
                "GFS wind forcing was requested but is not available locally.\n",
                encoding="utf-8",
            )
            (root / "src" / "services" / "weathering.py").write_text("cmems_ncep\n", encoding="utf-8")
            (root / "config" / "settings.yaml").write_text("ncep: Legacy NCEP Prototype Winds\n", encoding="utf-8")
            (root / "config" / "recipes.yaml").write_text(
                """
recipes:
  cmems_ncep:
    currents_file: cmems_curr.nc
    wind_file: ncep_wind.nc
    wave_file: cmems_wave.nc
    duration_hours: 72
    time_step_minutes: 60
  cmems_era5:
    currents_file: cmems_curr.nc
    wind_file: era5_wind.nc
    wave_file: cmems_wave.nc
    duration_hours: 72
    time_step_minutes: 60
  hycom_era5:
    currents_file: hycom_curr.nc
    wind_file: era5_wind.nc
    wave_file: cmems_wave.nc
    duration_hours: 72
    time_step_minutes: 60
phase1_recipe_architecture:
  official_recipe_family:
    - cmems_era5
    - cmems_gfs
    - hycom_era5
    - hycom_gfs
  legacy_recipe_name_aliases:
    cmems_ncep:
      chapter3_target_recipe: cmems_gfs
      status: legacy_name_only
""",
                encoding="utf-8",
            )
            (root / "config" / "phase1_baseline_selection.yaml").write_text(
                """
selected_recipe: cmems_era5
chapter3_finalization_audit:
  audit_status:
    classification: implemented_but_provisional
    full_production_rerun_required: true
""",
                encoding="utf-8",
            )
            for filename in ("cmems_curr.nc", "hycom_curr.nc", "era5_wind.nc", "cmems_wave.nc"):
                (forcing_dir / filename).write_text("", encoding="utf-8")

            validation_time = "2023-03-06T09:59:00Z"
            validation_date = "2023-03-06"
            forecast_manifest = {
                "manifest_type": "official_phase2_forecast",
                "simulation_window_utc": {"end": validation_time},
                "grid": {"grid_id": "GRID123"},
                "transport": {"provisional_transport_model": True},
                "recipe_selection": {"recipe": "cmems_era5", "valid": True, "provisional": False, "rerun_required": False},
                "baseline_provenance": {"recipe": "cmems_era5"},
                "status_flags": {"valid": False, "provisional": True, "rerun_required": False},
                "deterministic_control": {
                    "netcdf_path": "output/CASE_MINDORO_RETRO_2023/forecast/deterministic_control_cmems_era5.nc",
                    "products": [
                        {
                            "product_type": "control_footprint_mask",
                            "timestamp_utc": validation_time,
                            "relative_path": "forecast/control_footprint_mask_2023-03-06T09-59-00Z.tif",
                            "semantics": "Binary deterministic control footprint mask on the canonical scoring grid.",
                        },
                        {
                            "product_type": "control_density_norm",
                            "timestamp_utc": validation_time,
                            "relative_path": "forecast/control_density_norm_2023-03-06T09-59-00Z.tif",
                            "semantics": "Normalized deterministic control particle density on the canonical scoring grid.",
                        },
                    ],
                },
                "ensemble": {
                    "manifest_path": "output/CASE_MINDORO_RETRO_2023/ensemble/ensemble_manifest.json",
                    "actual_member_count": 50,
                },
                "canonical_products": {
                    "control_footprint_mask": "output/CASE_MINDORO_RETRO_2023/forecast/control_footprint_mask_2023-03-06T09-59-00Z.tif",
                    "control_density_norm": "output/CASE_MINDORO_RETRO_2023/forecast/control_density_norm_2023-03-06T09-59-00Z.tif",
                    "prob_presence": "output/CASE_MINDORO_RETRO_2023/ensemble/prob_presence_2023-03-06T09-59-00Z.tif",
                    "mask_p50": "output/CASE_MINDORO_RETRO_2023/ensemble/mask_p50_2023-03-06T09-59-00Z.tif",
                    "mask_p90": "output/CASE_MINDORO_RETRO_2023/ensemble/mask_p90_2023-03-06T09-59-00Z.tif",
                    "mask_p50_datecomposite": "output/CASE_MINDORO_RETRO_2023/ensemble/mask_p50_2023-03-06_datecomposite.tif",
                },
            }
            ensemble_manifest = {
                "manifest_type": "official_phase2_ensemble",
                "grid": {"grid_id": "GRID123"},
                "status_flags": {"valid": False, "provisional": True, "rerun_required": False},
                "baseline_provenance": {"recipe": "cmems_era5"},
                "ensemble_configuration": {"ensemble_size": 50},
                "member_runs": [{"member_id": idx} for idx in range(1, 51)],
                "products": [
                    {
                        "product_type": "prob_presence",
                        "timestamp_utc": validation_time,
                        "relative_path": "ensemble/prob_presence_2023-03-06T09-59-00Z.tif",
                        "semantics": "Per-cell ensemble probability of member presence at the product timestamp.",
                    },
                    {
                        "product_type": "mask_p50",
                        "timestamp_utc": validation_time,
                        "relative_path": "ensemble/mask_p50_2023-03-06T09-59-00Z.tif",
                        "semantics": "Binary ensemble mask where probability of presence is at least 0.50.",
                    },
                    {
                        "product_type": "mask_p90",
                        "timestamp_utc": validation_time,
                        "relative_path": "ensemble/mask_p90_2023-03-06T09-59-00Z.tif",
                        "semantics": "Binary ensemble mask where probability of presence is at least 0.90.",
                    },
                    {
                        "product_type": "prob_presence_datecomposite",
                        "date_utc": validation_date,
                        "relative_path": "ensemble/prob_presence_2023-03-06_datecomposite.tif",
                        "semantics": "Per-cell ensemble probability of any member presence across the target UTC date.",
                    },
                    {
                        "product_type": "mask_p50_datecomposite",
                        "date_utc": validation_date,
                        "relative_path": "ensemble/mask_p50_2023-03-06_datecomposite.tif",
                        "semantics": "Binary date-composite mask where probability of presence is at least 0.50.",
                    },
                ],
            }
            loading_audit = {
                "runs": [
                    {
                        "status": "completed",
                        "forcings": {
                            "current": {"covers_requested_window": True, "reader_attach_status": "loaded"},
                            "wind": {"covers_requested_window": True, "reader_attach_status": "loaded"},
                            "wave": {"covers_requested_window": True, "reader_attach_status": "loaded"},
                        },
                    }
                ]
            }

            (forecast_dir / "forecast_manifest.json").write_text(json.dumps(forecast_manifest), encoding="utf-8")
            (ensemble_dir / "ensemble_manifest.json").write_text(json.dumps(ensemble_manifest), encoding="utf-8")
            (forecast_dir / "phase2_loading_audit.json").write_text(json.dumps(loading_audit), encoding="utf-8")
            pd.DataFrame([{"status": "completed"}]).to_csv(forecast_dir / "phase2_loading_audit.csv", index=False)

            for relative_path in [
                "output/CASE_MINDORO_RETRO_2023/forecast/deterministic_control_cmems_era5.nc",
                "output/CASE_MINDORO_RETRO_2023/forecast/control_footprint_mask_2023-03-06T09-59-00Z.tif",
                "output/CASE_MINDORO_RETRO_2023/forecast/control_density_norm_2023-03-06T09-59-00Z.tif",
                "output/CASE_MINDORO_RETRO_2023/ensemble/prob_presence_2023-03-06T09-59-00Z.tif",
                "output/CASE_MINDORO_RETRO_2023/ensemble/mask_p50_2023-03-06T09-59-00Z.tif",
                "output/CASE_MINDORO_RETRO_2023/ensemble/mask_p90_2023-03-06T09-59-00Z.tif",
                "output/CASE_MINDORO_RETRO_2023/ensemble/prob_presence_2023-03-06_datecomposite.tif",
                "output/CASE_MINDORO_RETRO_2023/ensemble/mask_p50_2023-03-06_datecomposite.tif",
            ]:
                artifact = root / relative_path
                artifact.parent.mkdir(parents=True, exist_ok=True)
                artifact.write_text("", encoding="utf-8")

            cwd = os.getcwd()
            try:
                os.chdir(root)
                results = Phase2FinalizationAuditService(repo_root=root).run()
            finally:
                os.chdir(cwd)

            self.assertTrue(results["overall_verdict"]["scientifically_usable_as_implemented"])
            self.assertTrue(results["overall_verdict"]["requires_phase1_production_rerun_for_full_freeze"])
            self.assertTrue(results["overall_verdict"]["legacy_recipe_drift_only_documented"])
            self.assertTrue(Path(results["status_csv"]).exists())
            self.assertTrue(Path(results["status_json"]).exists())
            self.assertTrue(Path(results["memo_md"]).exists())
            self.assertTrue(Path(results["output_catalog_csv"]).exists())
            self.assertTrue(Path(results["verdict_md"]).exists())

            status_df = pd.read_csv(results["status_csv"])
            provenance_row = status_df[status_df["requirement_id"] == "explicit_provenance_status_fields"].iloc[0]
            drift_row = status_df[status_df["requirement_id"] == "phase1_recipe_family_drift_honesty"].iloc[0]
            self.assertEqual(provenance_row["classification"], "implemented_but_provisional")
            self.assertEqual(drift_row["classification"], "implemented_but_provisional")

            catalog_df = pd.read_csv(results["output_catalog_csv"])
            self.assertTrue((catalog_df["product_type"] == "mask_p50_datecomposite").any())


if __name__ == "__main__":
    unittest.main()
