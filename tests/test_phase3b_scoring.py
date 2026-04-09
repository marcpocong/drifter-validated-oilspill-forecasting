import tempfile
import unittest
from pathlib import Path

from src.services.scoring import (
    derive_phase3b_status_from_upstream,
    resolve_official_phase3b_pairs,
)


class Phase3BScoringTests(unittest.TestCase):
    def test_derive_phase3b_status_marks_provisional_upstream_transport(self):
        forecast_manifest = {
            "transport": {"model": "oceandrift", "provisional_transport_model": True},
            "status_flags": {"valid": False, "provisional": True, "rerun_required": False},
            "recipe_selection": {"provisional": False},
        }
        ensemble_manifest = {
            "status_flags": {"valid": False, "provisional": True, "rerun_required": False},
            "baseline_provenance": {"provisional": False},
        }

        status = derive_phase3b_status_from_upstream(forecast_manifest, ensemble_manifest)

        self.assertEqual(status["status_flag"], "provisional")
        self.assertFalse(status["valid"])
        self.assertTrue(status["provisional"])
        self.assertFalse(status["rerun_required"])
        self.assertIn("transport.provisional_transport_model=true", status["reasons"])

    def test_resolve_official_phase3b_pairs_uses_manifest_backed_primary_and_sensitivity_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            case_output_dir = base / "output" / "CASE_TEST"
            forecast_dir = case_output_dir / "forecast"
            ensemble_dir = case_output_dir / "ensemble"
            forecast_dir.mkdir(parents=True, exist_ok=True)
            ensemble_dir.mkdir(parents=True, exist_ok=True)

            obs_path = base / "data" / "obs_mask_2023-03-06.tif"
            obs_path.parent.mkdir(parents=True, exist_ok=True)
            obs_path.touch()

            control_path = forecast_dir / "control_footprint_mask_2023-03-06T09-59-00Z.tif"
            datecomposite_path = ensemble_dir / "mask_p50_2023-03-06_datecomposite.tif"
            control_path.touch()
            datecomposite_path.touch()

            forecast_manifest = {
                "manifest_type": "official_phase2_forecast",
                "canonical_products": {
                    "control_footprint_mask": str(control_path),
                    "mask_p50_datecomposite": str(datecomposite_path),
                },
                "deterministic_control": {
                    "products": [
                        {
                            "product_type": "control_footprint_mask",
                            "timestamp_utc": "2023-03-06T09:59:00Z",
                            "relative_path": "forecast/control_footprint_mask_2023-03-06T09-59-00Z.tif",
                            "semantics": "Binary deterministic control footprint mask on the canonical scoring grid.",
                        }
                    ]
                },
            }
            ensemble_manifest = {
                "manifest_type": "official_phase2_ensemble",
                "products": [
                    {
                        "product_type": "mask_p50_datecomposite",
                        "date_utc": "2023-03-06",
                        "relative_path": "ensemble/mask_p50_2023-03-06_datecomposite.tif",
                        "semantics": "Binary date-composite mask where probability of presence is at least 0.50.",
                    }
                ],
            }

            pairs = resolve_official_phase3b_pairs(
                forecast_manifest=forecast_manifest,
                ensemble_manifest=ensemble_manifest,
                case_output_dir=case_output_dir,
                observation_path=obs_path,
                validation_time_utc="2023-03-06T09:59:00Z",
            )

            self.assertEqual(len(pairs), 2)
            primary = next(pair for pair in pairs if pair["pair_role"] == "primary")
            sensitivity = next(pair for pair in pairs if pair["pair_role"] == "sensitivity")
            self.assertEqual(primary["forecast_product"], "mask_p50_2023-03-06_datecomposite.tif")
            self.assertEqual(primary["source_semantics"], "March6_date_composite_vs_March6_obsmask")
            self.assertEqual(sensitivity["forecast_product"], "control_footprint_mask_2023-03-06T09-59-00Z.tif")
            self.assertEqual(sensitivity["source_semantics"], "March6_control_footprint_vs_March6_obsmask")


if __name__ == "__main__":
    unittest.main()
