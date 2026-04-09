import unittest

from src.services.phase3b_forensics import (
    choose_phase3b_failure_class,
    observation_collapse_flag,
)


class Phase3BForensicsTests(unittest.TestCase):
    def test_observation_collapse_flag_marks_tiny_observed_masks(self):
        self.assertTrue(observation_collapse_flag(0))
        self.assertTrue(observation_collapse_flag(5))
        self.assertFalse(observation_collapse_flag(6))

    def test_choose_phase3b_failure_class_prefers_displacement_when_threshold_sweep_never_overlaps(self):
        primary_metrics = {
            "forecast_nonzero_cells": 10,
            "forecast_empty_or_near_empty": True,
        }
        threshold_metrics = [
            {
                "forecast_variant": "datecomposite_threshold",
                "threshold": 0.10,
                "centroid_distance_m": 113000.0,
                "nearest_distance_to_obs_m": 108000.0,
                "iou": 0.0,
                "dice": 0.0,
                "fss_1km": 0.0,
                "fss_3km": 0.0,
                "fss_5km": 0.0,
                "fss_10km": 0.0,
            },
            {
                "forecast_variant": "datecomposite_threshold",
                "threshold": 0.50,
                "centroid_distance_m": 116000.0,
                "nearest_distance_to_obs_m": 113000.0,
                "iou": 0.0,
                "dice": 0.0,
                "fss_1km": 0.0,
                "fss_3km": 0.0,
                "fss_5km": 0.0,
                "fss_10km": 0.0,
            },
        ]

        diagnosis = choose_phase3b_failure_class(
            primary_metrics=primary_metrics,
            threshold_metrics=threshold_metrics,
            observation_collapse_on_1km_grid=True,
        )

        self.assertEqual(diagnosis["class"], "B")
        self.assertIn("Observation collapses to <=5 cells", diagnosis["secondary_factors"][0])


if __name__ == "__main__":
    unittest.main()
