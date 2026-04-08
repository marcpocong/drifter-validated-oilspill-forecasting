import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import rasterio
from rasterio.transform import from_origin

from src.core.case_context import get_case_context


class TestScoringGrid(unittest.TestCase):
    def setUp(self):
        get_case_context.cache_clear()
        self.addCleanup(get_case_context.cache_clear)

    def test_official_grid_is_projected_metric_and_artifacts_exist(self):
        with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()

            from src.helpers.scoring import build_official_scoring_grid, get_scoring_grid_artifact_paths

            spec = build_official_scoring_grid(force_refresh=True)
            artifacts = get_scoring_grid_artifact_paths()

            self.assertEqual(spec.crs, "EPSG:32651")
            self.assertEqual(spec.resolution, 1000.0)
            self.assertEqual(spec.units, "meters")
            self.assertGreater(spec.width, 0)
            self.assertGreater(spec.height, 0)
            self.assertIsNotNone(spec.display_bounds_wgs84)

            for key in ("metadata_yaml", "template_tif", "extent_gpkg", "land_mask_tif", "sea_mask_tif"):
                self.assertTrue(artifacts[key].exists(), f"Missing artifact: {artifacts[key]}")

            with rasterio.open(artifacts["template_tif"]) as src:
                self.assertEqual(src.crs.to_string(), "EPSG:32651")
                self.assertEqual(src.res, (1000.0, 1000.0))
                self.assertEqual(src.width, spec.width)
                self.assertEqual(src.height, spec.height)

    def test_same_grid_precheck_reports_pass_and_fail_cases(self):
        with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()

            from src.helpers.raster import GridBuilder, save_raster
            from src.helpers.scoring import precheck_same_grid

            grid = GridBuilder()
            data = np.zeros((grid.height, grid.width), dtype=np.float32)

            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                forecast_path = tmp_path / "forecast.tif"
                target_path = tmp_path / "target.tif"
                shifted_path = tmp_path / "shifted.tif"

                save_raster(grid, data, forecast_path)
                save_raster(grid, data, target_path)

                passing = precheck_same_grid(forecast_path, target_path, tmp_path / "passing")
                self.assertTrue(passing.passed)
                self.assertTrue(passing.csv_report_path.exists())
                self.assertTrue(passing.json_report_path.exists())

                shifted_transform = from_origin(
                    grid.min_x + grid.resolution,
                    grid.max_y,
                    grid.resolution,
                    grid.resolution,
                )
                with rasterio.open(
                    shifted_path,
                    "w",
                    driver="GTiff",
                    height=grid.height,
                    width=grid.width,
                    count=1,
                    dtype=data.dtype,
                    crs=grid.crs,
                    transform=shifted_transform,
                    compress="lzw",
                ) as dst:
                    dst.write(data, 1)

                failing = precheck_same_grid(forecast_path, shifted_path, tmp_path / "failing")
                self.assertFalse(failing.passed)
                self.assertFalse(failing.checks["transform_match"])


if __name__ == "__main__":
    unittest.main()
