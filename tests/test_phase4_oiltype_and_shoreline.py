import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd

from src.services.phase4_oiltype_and_shoreline import (
    Phase4OilTypeAndShorelineService,
    load_phase4_scenario_registry,
    resolve_phase4_transport_source,
)


class TestPhase4OilTypeAndShoreline(unittest.TestCase):
    def test_load_phase4_scenario_registry_reads_required_scenarios(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            registry_path = Path(tmpdir) / "phase4_oil_scenarios.csv"
            registry_path.write_text(
                "\n".join(
                    [
                        "scenario_id,oil_record_id,oil_label,category,source_note,enabled_yes_no",
                        "lighter_oil,GENERIC_DIESEL,Light oil,light,light scenario,yes",
                        "fixed_base_medium_heavy_proxy,GENERIC_IFO_180,Medium-heavy proxy,medium,anchor scenario,yes",
                        "heavier_oil,GENERIC_HEAVY_FUEL_OIL,Heavier oil,heavy,heavy scenario,yes",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            scenarios = load_phase4_scenario_registry(registry_path)

            self.assertEqual(
                [scenario["scenario_id"] for scenario in scenarios],
                ["lighter_oil", "fixed_base_medium_heavy_proxy", "heavier_oil"],
            )
            self.assertEqual(scenarios[1]["adios_id"], "GENERIC INTERMEDIATE FUEL OIL 180")

    def test_resolve_phase4_transport_source_prefers_recommended_shoreline_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = root / "output" / "CASE_MINDORO_RETRO_2023"
            convergence = base / "convergence_after_shoreline"
            recommended = convergence / "elements_100000"
            (recommended / "forecast").mkdir(parents=True, exist_ok=True)
            (recommended / "ensemble").mkdir(parents=True, exist_ok=True)
            (recommended / "forecast" / "forecast_manifest.json").write_text("{}", encoding="utf-8")
            (recommended / "ensemble" / "ensemble_manifest.json").write_text("{}", encoding="utf-8")
            (convergence / "convergence_after_shoreline_run_manifest.json").write_text(
                json.dumps({"recommendation": {"recommended_final_official_element_count": 100000}}),
                encoding="utf-8",
            )

            source = resolve_phase4_transport_source(root, "CASE_MINDORO_RETRO_2023")

            self.assertEqual(source.selected_run_name, "CASE_MINDORO_RETRO_2023/convergence_after_shoreline/elements_100000")
            self.assertTrue(source.shoreline_aware_workflow_reused)

    def test_phase4_service_writes_manifest_bundle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "data_processed" / "grids").mkdir(parents=True, exist_ok=True)
            (root / "data" / "forcing" / "CASE_MINDORO_RETRO_2023").mkdir(parents=True, exist_ok=True)
            (root / "output" / "phase2_finalization_audit").mkdir(parents=True, exist_ok=True)
            (root / "output" / "CASE_MINDORO_RETRO_2023" / "convergence_after_shoreline" / "elements_100000" / "forecast").mkdir(parents=True, exist_ok=True)
            (root / "output" / "CASE_MINDORO_RETRO_2023" / "convergence_after_shoreline" / "elements_100000" / "ensemble").mkdir(parents=True, exist_ok=True)
            (root / "output" / "CASE_MINDORO_RETRO_2023" / "convergence_after_shoreline").mkdir(parents=True, exist_ok=True)

            (root / "config" / "phase4_oil_scenarios.csv").write_text(
                "\n".join(
                    [
                        "scenario_id,oil_record_id,oil_label,category,source_note,enabled_yes_no",
                        "lighter_oil,GENERIC_DIESEL,Light oil,light,light scenario,yes",
                        "fixed_base_medium_heavy_proxy,GENERIC_IFO_180,Medium-heavy proxy,medium,anchor scenario,yes",
                        "heavier_oil,GENERIC_HEAVY_FUEL_OIL,Heavier oil,heavy,heavy scenario,yes",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (root / "config" / "oil.yaml").write_text(
                "simulation:\n  num_particles: 1000\n  initial_mass_tonnes: 50.0\n",
                encoding="utf-8",
            )
            (root / "output" / "phase2_finalization_audit" / "phase2_finalization_status.json").write_text(
                json.dumps(
                    {
                        "overall_verdict": {
                            "scientifically_usable_as_implemented": True,
                            "scientifically_frozen": False,
                            "transport_model_provisional": True,
                            "biggest_remaining_phase2_provisional_item": "Phase 2 still depends on the later Phase 1 rerun.",
                        }
                    }
                ),
                encoding="utf-8",
            )
            (root / "output" / "CASE_MINDORO_RETRO_2023" / "convergence_after_shoreline" / "convergence_after_shoreline_run_manifest.json").write_text(
                json.dumps({"recommendation": {"recommended_final_official_element_count": 100000}}),
                encoding="utf-8",
            )
            (root / "data" / "forcing" / "CASE_MINDORO_RETRO_2023" / "cmems_curr.nc").write_text("fake-current", encoding="utf-8")
            (root / "data" / "forcing" / "CASE_MINDORO_RETRO_2023" / "era5_wind.nc").write_text("fake-wind", encoding="utf-8")
            (root / "data" / "forcing" / "CASE_MINDORO_RETRO_2023" / "cmems_wave.nc").write_text("fake-wave", encoding="utf-8")
            (root / "output" / "CASE_MINDORO_RETRO_2023" / "convergence_after_shoreline" / "elements_100000" / "forecast" / "phase2_loading_audit.json").write_text(
                json.dumps(
                    {
                        "runs": [
                            {
                                "run_kind": "deterministic_control",
                                "status": "completed",
                                "requested_start_time_utc": "2023-03-03T09:59:00Z",
                                "requested_end_time_utc": "2023-03-06T09:59:00Z",
                                "forcings": {
                                    "current": {
                                        "used_path": "data/forcing/CASE_MINDORO_RETRO_2023/cmems_curr.nc",
                                        "reader_attach_status": "loaded",
                                        "covers_requested_window": True,
                                        "tail_extension_applied": True,
                                        "requested_start_time_utc": "2023-03-03T09:59:00Z",
                                        "requested_end_time_utc": "2023-03-06T09:59:00Z",
                                    },
                                    "wind": {
                                        "used_path": "data/forcing/CASE_MINDORO_RETRO_2023/era5_wind.nc",
                                        "reader_attach_status": "loaded",
                                        "covers_requested_window": True,
                                        "tail_extension_applied": False,
                                        "requested_start_time_utc": "2023-03-03T09:59:00Z",
                                        "requested_end_time_utc": "2023-03-06T09:59:00Z",
                                    },
                                    "wave": {
                                        "used_path": "data/forcing/CASE_MINDORO_RETRO_2023/cmems_wave.nc",
                                        "reader_attach_status": "loaded",
                                        "covers_requested_window": True,
                                        "tail_extension_applied": False,
                                        "requested_start_time_utc": "2023-03-03T09:59:00Z",
                                        "requested_end_time_utc": "2023-03-06T09:59:00Z",
                                    },
                                },
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (root / "output" / "CASE_MINDORO_RETRO_2023" / "convergence_after_shoreline" / "elements_100000" / "forecast" / "forecast_manifest.json").write_text(
                json.dumps(
                    {
                        "transport": {"model": "oceandrift", "provisional_transport_model": True},
                        "recipe_selection": {
                            "recipe": "cmems_era5",
                            "source_path": "config/phase1_baseline_selection.yaml",
                        },
                        "baseline_provenance": {
                            "recipe": "cmems_era5",
                            "source_path": "config/phase1_baseline_selection.yaml",
                        },
                        "loading_audit": {
                            "json": "output/CASE_MINDORO_RETRO_2023/convergence_after_shoreline/elements_100000/forecast/phase2_loading_audit.json",
                        },
                        "grid": {
                            "shoreline_segments_path": "data_processed/grids/shoreline_segments.gpkg",
                            "shoreline_mask_status": "gshhg_test_status",
                            "shoreline_mask_signature": "test-signature",
                        },
                    }
                ),
                encoding="utf-8",
            )
            (root / "output" / "CASE_MINDORO_RETRO_2023" / "convergence_after_shoreline" / "elements_100000" / "ensemble" / "ensemble_manifest.json").write_text(
                json.dumps({"member_runs": []}),
                encoding="utf-8",
            )
            (root / "data_processed" / "grids" / "shoreline_segments.gpkg").write_text("placeholder", encoding="utf-8")

            def fake_weathering_runner(best_recipe, start_time, start_lat, start_lon, *, scenarios, output_dir, phase_label, forcing_paths):
                output_dir = Path(output_dir)
                output_dir.mkdir(parents=True, exist_ok=True)
                self.assertTrue(str(forcing_paths["currents"]).endswith("cmems_curr.nc"))
                self.assertTrue(str(forcing_paths["wind"]).endswith("era5_wind.nc"))
                results = {}
                for index, scenario_id in enumerate(scenarios.keys(), start=1):
                    nc_path = output_dir / f"openoil_{scenario_id}.nc"
                    nc_path.write_text("fake-nc", encoding="utf-8")
                    budget_df = pd.DataFrame(
                        {
                            "hours_elapsed": [0, 24, 72],
                            "surface_pct": [100.0, 70.0 - index, 40.0 - index],
                            "evaporated_pct": [0.0, 15.0 + index, 25.0 + index],
                            "dispersed_pct": [0.0, 10.0, 20.0],
                            "beached_pct": [0.0, 5.0 + index, 15.0 + index],
                        }
                    )
                    results[scenario_id] = {
                        "display_name": scenarios[scenario_id]["display_name"],
                        "budget_df": budget_df,
                        "nc_path": nc_path,
                        "csv_path": output_dir / f"budget_{scenario_id}.csv",
                        "qc": {"passed": True, "max_deviation_pct": 0.5},
                    }
                return results

            def fake_shoreline_analyzer(*, nc_path, initial_mass_tonnes, canonical_segments_path, prefer_canonical_segments):
                scenario_id = Path(nc_path).stem.replace("openoil_", "")
                offset = {
                    "lighter_oil": 12.0,
                    "fixed_base_medium_heavy_proxy": 18.0,
                    "heavier_oil": 24.0,
                }[scenario_id]
                return pd.DataFrame(
                    [
                        {
                            "segment_id": f"shoreline_{scenario_id}",
                            "segment_assignment_method": "canonical_shoreline_segments_gpkg",
                            "total_beached_kg": 100.0 + offset,
                            "n_particles": 10,
                            "first_arrival_h": offset,
                            "first_arrival_utc": "2023-03-04T00:00:00Z",
                        }
                    ]
                )

            fake_case = SimpleNamespace(
                is_official=True,
                run_name="CASE_MINDORO_RETRO_2023",
                case_id="CASE_MINDORO_RETRO_2023",
            )

            with mock.patch("src.services.phase4_oiltype_and_shoreline.get_case_context", return_value=fake_case), \
                mock.patch("src.services.phase4_oiltype_and_shoreline.resolve_spill_origin", return_value=(13.0, 121.5, "2023-03-03T09:59:00Z")), \
                mock.patch(
                    "src.services.phase4_oiltype_and_shoreline.get_phase1_baseline_audit_status",
                    return_value={
                        "classification": "implemented_but_provisional",
                        "full_production_rerun_required": True,
                        "blocker": "Phase 1 regional segment registry still missing.",
                    },
                ), \
                mock.patch(
                    "src.services.phase4_oiltype_and_shoreline.get_phase2_recipe_family_status",
                    return_value={
                        "official_recipe_family_expected": ["cmems_era5", "cmems_gfs", "hycom_era5", "hycom_gfs"],
                        "official_recipe_family_locally_available": ["cmems_era5", "hycom_era5"],
                        "legacy_recipe_id_detected": True,
                        "requires_phase1_production_rerun_for_full_freeze": True,
                        "legacy_recipe_drift_leaks_into_official_mode": True,
                    },
                ):
                service = Phase4OilTypeAndShorelineService(
                    repo_root=root,
                    weathering_runner=fake_weathering_runner,
                    shoreline_analyzer=fake_shoreline_analyzer,
                )
                results = service.run()

            manifest_path = Path(results["manifest_path"])
            summary_path = Path(results["summary_csv"])
            arrival_path = Path(results["shoreline_arrival_csv"])
            verdict_path = Path(results["final_verdict_md"])

            self.assertTrue(manifest_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertTrue(arrival_path.exists())
            self.assertTrue(verdict_path.exists())

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(
                manifest["selected_transport_run_name"],
                "CASE_MINDORO_RETRO_2023/convergence_after_shoreline/elements_100000",
            )
            self.assertTrue(manifest["shoreline_aware_workflow_reused"])
            self.assertEqual(
                manifest["transport_forcing_resolution_mode"],
                "phase2_loading_audit_deterministic_control_used_paths",
            )
            self.assertEqual(manifest["weathering_path_audit"]["decision"], "partially_implemented_but_needs_refactor")
            self.assertTrue(manifest["overall_verdict"]["scientifically_reportable_now"])
