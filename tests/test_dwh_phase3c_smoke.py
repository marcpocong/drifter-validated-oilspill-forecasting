import tempfile
import unittest
from pathlib import Path

import xarray as xr

from src.helpers.scoring import ScoringGridSpec
from src.services.dwh_phase3c_smoke import (
    derive_forcing_bbox_from_grid,
    inspect_forcing_file,
    write_smoke_forcing_files,
)


class DWHPhase3CSmokeTests(unittest.TestCase):
    def test_forcing_bbox_uses_grid_display_bounds_plus_half_degree_halo(self):
        spec = ScoringGridSpec(
            min_x=0,
            max_x=1000,
            min_y=0,
            max_y=1000,
            resolution=1000,
            crs="EPSG:32616",
            display_bounds_wgs84=[-92.0, -84.0, 26.5, 31.0],
        )

        self.assertEqual(
            derive_forcing_bbox_from_grid(spec, halo_degrees=0.5),
            [-92.5, -83.5, 26.0, 31.5],
        )

    def test_smoke_forcing_files_are_nonzero_and_reader_variable_compatible(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            paths = write_smoke_forcing_files(
                Path(tmp_dir),
                [-92.5, -83.5, 26.0, 31.5],
                "2010-05-20T00:00:00Z",
                "2010-05-21T00:00:00Z",
            )

            current_status = inspect_forcing_file(paths["current"], "current")
            wind_status = inspect_forcing_file(paths["wind"], "wind")
            wave_status = inspect_forcing_file(paths["wave"], "wave")

            self.assertEqual(current_status["adapter_compatibility_status"], "reader_variable_compatible")
            self.assertEqual(wind_status["adapter_compatibility_status"], "reader_variable_compatible")
            self.assertEqual(wave_status["adapter_compatibility_status"], "reader_variable_compatible")
            self.assertEqual(current_status["coverage_start_utc"], "2010-05-20T00:00:00Z")
            self.assertEqual(current_status["coverage_end_utc"], "2010-05-21T00:00:00Z")

            with xr.open_dataset(paths["current"]) as ds:
                self.assertGreater(float(abs(ds["water_u"]).max()), 0.0)
                self.assertEqual(ds.attrs["non_scientific_smoke"], "true")
                self.assertEqual(ds.attrs["scientific_ready"], "false")


if __name__ == "__main__":
    unittest.main()
