import unittest

import pandas as pd

from src.services.phase3b_post_recipe_decision import classify_post_recipe_outcome, rank_completed_recipes


class Phase3BPostRecipeDecisionTests(unittest.TestCase):
    def test_rank_prefers_lower_distance_when_fss_ties(self):
        df = pd.DataFrame(
            [
                {
                    "recipe_id": "baseline",
                    "status": "completed",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "mean_fss": 0.0,
                    "centroid_distance_m": 120000.0,
                    "area_ratio_forecast_to_obs": 5.0,
                    "iou": 0.0,
                    "dice": 0.0,
                    "nearest_distance_to_obs_m": 118000.0,
                    "p50_nonzero_cells": 10.0,
                    "obs_nonzero_cells": 2.0,
                    "max_march6_probability": 0.8,
                    "max_march6_occupancy_members": 40.0,
                    "provisional_transport_model": True,
                    "matches_frozen_historical_baseline": True,
                },
                {
                    "recipe_id": "candidate",
                    "status": "completed",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "mean_fss": 0.0,
                    "centroid_distance_m": 112000.0,
                    "area_ratio_forecast_to_obs": 20.0,
                    "iou": 0.0,
                    "dice": 0.0,
                    "nearest_distance_to_obs_m": 103000.0,
                    "p50_nonzero_cells": 50.0,
                    "obs_nonzero_cells": 2.0,
                    "max_march6_probability": 1.0,
                    "max_march6_occupancy_members": 50.0,
                    "provisional_transport_model": True,
                    "matches_frozen_historical_baseline": False,
                },
            ]
        )

        ranked = rank_completed_recipes(df)

        self.assertEqual(ranked.iloc[0]["recipe_id"], "candidate")
        self.assertEqual(ranked.iloc[0]["decision_rank"], 1)

    def test_zero_overlap_across_recipes_is_r3(self):
        df = pd.DataFrame(
            [
                {
                    "recipe_id": "cmems_era5",
                    "status": "completed",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "mean_fss": 0.0,
                    "centroid_distance_m": 115946.97,
                    "area_ratio_forecast_to_obs": 5.0,
                    "iou": 0.0,
                    "dice": 0.0,
                    "nearest_distance_to_obs_m": 113017.70,
                    "p50_nonzero_cells": 10.0,
                    "obs_nonzero_cells": 2.0,
                    "max_march6_probability": 0.82,
                    "max_march6_occupancy_members": 41.0,
                    "provisional_transport_model": True,
                    "matches_frozen_historical_baseline": True,
                },
                {
                    "recipe_id": "hycom_era5",
                    "status": "completed",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "mean_fss": 0.0,
                    "centroid_distance_m": 112377.39,
                    "area_ratio_forecast_to_obs": 26.5,
                    "iou": 0.0,
                    "dice": 0.0,
                    "nearest_distance_to_obs_m": 103043.68,
                    "p50_nonzero_cells": 53.0,
                    "obs_nonzero_cells": 2.0,
                    "max_march6_probability": 1.0,
                    "max_march6_occupancy_members": 50.0,
                    "provisional_transport_model": True,
                    "matches_frozen_historical_baseline": False,
                },
            ]
        )

        decision = classify_post_recipe_outcome(df, baseline_recipe="cmems_era5")

        self.assertEqual(decision["class"], "R3")
        self.assertEqual(decision["best_recipe_id"], "hycom_era5")
        self.assertEqual(decision["next_action"], "run a coastal/initialization/domain displacement audit")


if __name__ == "__main__":
    unittest.main()
