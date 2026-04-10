import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from src.core.case_context import get_case_context
from src.services.convergence_after_shoreline import (
    ConvergenceAfterShorelineService,
    _rank_recipe_sensitivity_rows,
    _recipe_rows_are_tied,
)
from src.utils.io import (
    get_convergence_after_shoreline_output_dir,
    get_convergence_after_shoreline_run_name,
)


class ConvergenceAfterShorelineTests(unittest.TestCase):
    def setUp(self):
        get_case_context.cache_clear()
        self.addCleanup(get_case_context.cache_clear)

    def test_convergence_run_name_helpers(self):
        with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()
            self.assertEqual(
                get_convergence_after_shoreline_output_dir().as_posix(),
                "output/CASE_MINDORO_RETRO_2023/convergence_after_shoreline",
            )
            self.assertEqual(
                get_convergence_after_shoreline_run_name(5000),
                "CASE_MINDORO_RETRO_2023/convergence_after_shoreline/elements_005000",
            )

    def test_rank_recipe_sensitivity_rows_prefers_unique_best(self):
        df = pd.DataFrame(
            [
                {
                    "recipe_id": "cmems_era5",
                    "status": "completed",
                    "mean_fss": 0.10,
                    "fss_1km": 0.08,
                    "fss_3km": 0.09,
                    "fss_5km": 0.10,
                    "fss_10km": 0.11,
                    "iou": 0.05,
                    "dice": 0.10,
                    "centroid_distance_m": 15000.0,
                },
                {
                    "recipe_id": "hycom_era5",
                    "status": "completed",
                    "mean_fss": 0.12,
                    "fss_1km": 0.09,
                    "fss_3km": 0.11,
                    "fss_5km": 0.12,
                    "fss_10km": 0.13,
                    "iou": 0.06,
                    "dice": 0.11,
                    "centroid_distance_m": 12000.0,
                },
            ]
        )
        ranked = _rank_recipe_sensitivity_rows(df)
        self.assertEqual(ranked.iloc[0]["recipe_id"], "hycom_era5")
        self.assertFalse(_recipe_rows_are_tied(ranked.iloc[0], ranked.iloc[1]))

    def test_build_recommendation_marks_100k_feasible_and_5000_sparse(self):
        service = object.__new__(ConvergenceAfterShorelineService)
        summary_df = pd.DataFrame(
            [
                {
                    "status": "completed",
                    "element_count_actual": 5000,
                    "official_main_forecast_nonzero_cells": 0,
                    "max_march6_occupancy_members": 5,
                    "official_main_fss_1km": 0.0,
                    "official_main_fss_3km": 0.0,
                    "official_main_fss_5km": 0.0,
                    "official_main_fss_10km": 0.0,
                },
                {
                    "status": "completed",
                    "element_count_actual": 50000,
                    "official_main_forecast_nonzero_cells": 12,
                    "max_march6_occupancy_members": 18,
                    "official_main_fss_1km": 0.0,
                    "official_main_fss_3km": 0.0,
                    "official_main_fss_5km": 0.0,
                    "official_main_fss_10km": 0.0,
                },
                {
                    "status": "completed",
                    "element_count_actual": 100000,
                    "official_main_forecast_nonzero_cells": 13,
                    "max_march6_occupancy_members": 19,
                    "official_main_fss_1km": 0.0,
                    "official_main_fss_3km": 0.0,
                    "official_main_fss_5km": 0.0,
                    "official_main_fss_10km": 0.0,
                },
            ]
        )
        recommendation = ConvergenceAfterShorelineService._build_recommendation(service, summary_df)
        self.assertEqual(recommendation["recommended_final_official_element_count"], 100000)
        self.assertTrue(recommendation["five_thousand_too_sparse"])
        self.assertTrue(recommendation["one_hundred_thousand_feasible"])


if __name__ == "__main__":
    unittest.main()
