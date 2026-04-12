import unittest
from types import SimpleNamespace

import numpy as np

from src.helpers.metrics import calculate_kl_divergence
from src.services.benchmark import ensure_point_within_benchmark_grid


class BenchmarkMetricTests(unittest.TestCase):
    def test_kl_divergence_renormalizes_on_valid_mask(self):
        forecast = np.array([[0.6, 0.4], [0.0, 0.0]], dtype=float)
        observed = np.array([[0.3, 0.7], [10.0, 10.0]], dtype=float)
        valid_mask = np.array([[True, True], [False, False]])

        actual = calculate_kl_divergence(forecast, observed, epsilon=1e-12, valid_mask=valid_mask)

        expected_forecast = np.array([0.6, 0.4], dtype=float)
        expected_observed = np.array([0.3, 0.7], dtype=float)
        expected_forecast /= expected_forecast.sum()
        expected_observed /= expected_observed.sum()
        expected = float(np.sum(expected_observed * np.log(expected_observed / expected_forecast)))

        self.assertAlmostEqual(actual, expected, places=10)

    def test_kl_divergence_requires_positive_mass(self):
        forecast = np.zeros((2, 2), dtype=float)
        observed = np.ones((2, 2), dtype=float)
        with self.assertRaises(ValueError):
            calculate_kl_divergence(forecast, observed, valid_mask=np.ones((2, 2), dtype=bool))

    def test_benchmark_preflight_rejects_spill_origin_outside_grid(self):
        grid = SimpleNamespace(min_lon=115.0, max_lon=122.0, min_lat=6.0, max_lat=14.5)

        with self.assertRaises(RuntimeError) as exc:
            ensure_point_within_benchmark_grid(
                lon=112.6630,
                lat=16.2980,
                grid=grid,
            )

        message = str(exc.exception)
        self.assertIn("112.6630E, 16.2980N", message)
        self.assertIn("outside the benchmark grid", message)
        self.assertIn("defensible Phase 3A rasters", message)


if __name__ == "__main__":
    unittest.main()
