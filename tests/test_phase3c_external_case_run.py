import unittest

import numpy as np

from src.helpers.scoring import ScoringGridSpec
from src.services.phase3c_external_case_run import (
    build_event_corridor_mask,
    mask_diagnostics,
    window_cells_for_km,
)


class Phase3CExternalCaseRunTests(unittest.TestCase):
    def _spec(self):
        return ScoringGridSpec(
            min_x=0,
            max_x=4000,
            min_y=0,
            max_y=4000,
            resolution=1000,
            crs="EPSG:32616",
        )

    def test_window_cells_use_projected_one_km_grid(self):
        spec = self._spec()
        self.assertEqual(window_cells_for_km(1, spec), 1)
        self.assertEqual(window_cells_for_km(3, spec), 3)
        self.assertEqual(window_cells_for_km(10, spec), 10)

    def test_event_corridor_mask_is_union_of_daily_masks(self):
        first = np.zeros((3, 3), dtype=np.float32)
        second = np.zeros((3, 3), dtype=np.float32)
        first[0, 0] = 1
        second[2, 2] = 1

        corridor = build_event_corridor_mask([first, second])

        self.assertEqual(int(corridor.sum()), 2)
        self.assertEqual(corridor[0, 0], 1)
        self.assertEqual(corridor[2, 2], 1)

    def test_mask_diagnostics_include_overlap_and_distance_fields(self):
        spec = self._spec()
        forecast = np.zeros((4, 4), dtype=np.float32)
        observed = np.zeros((4, 4), dtype=np.float32)
        forecast[1, 1] = 1
        observed[1, 1] = 1
        observed[1, 2] = 1

        diagnostics = mask_diagnostics(forecast, observed, spec, sea_mask=np.ones((4, 4), dtype=np.float32))

        self.assertEqual(diagnostics["forecast_nonzero_cells"], 1)
        self.assertEqual(diagnostics["obs_nonzero_cells"], 2)
        self.assertAlmostEqual(diagnostics["iou"], 0.5)
        self.assertAlmostEqual(diagnostics["dice"], 2 / 3)
        self.assertEqual(diagnostics["nearest_distance_to_obs_m"], 0.0)


if __name__ == "__main__":
    unittest.main()
