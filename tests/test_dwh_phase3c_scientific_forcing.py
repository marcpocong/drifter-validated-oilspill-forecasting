import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from src.services.dwh_phase3c_scientific_forcing import (
    attrs_mark_smoke_only,
    coverage_spans_window,
    path_is_smoke_only,
    validate_prepared_forcing_file,
)


class DWHScientificForcingReadyTests(unittest.TestCase):
    def test_coverage_window_requires_full_required_span(self):
        self.assertTrue(
            coverage_spans_window(
                "2010-05-20T00:00:00Z",
                "2010-05-24T00:00:00Z",
                "2010-05-20T00:00:00Z",
                "2010-05-23T23:59:59Z",
            )
        )
        self.assertFalse(
            coverage_spans_window(
                "2010-05-20T01:00:00Z",
                "2010-05-24T00:00:00Z",
                "2010-05-20T00:00:00Z",
                "2010-05-23T23:59:59Z",
            )
        )

    def test_smoke_only_paths_and_attrs_are_detected(self):
        self.assertTrue(path_is_smoke_only("output/CASE_DWH_RETRO_2010_72H/dwh_phase3c_forcing_adapter_and_non_scientific_smoke_forecast/prepared_forcing/current.nc"))
        self.assertTrue(path_is_smoke_only("dwh_smoke_current_non_scientific.nc"))
        self.assertTrue(attrs_mark_smoke_only({"non_scientific_smoke": "true"}))
        self.assertTrue(attrs_mark_smoke_only({"source_is_smoke_only": True}))
        self.assertFalse(path_is_smoke_only("output/CASE_DWH_RETRO_2010_72H/dwh_phase3c_scientific_forcing_ready/prepared_forcing/hycom.nc"))

    def test_validation_rejects_dataset_marked_smoke_only_before_reader_open(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "real_name_current.nc"
            times = pd.date_range("2010-05-20", "2010-05-24", freq="1h")
            ds = xr.Dataset(
                {
                    "x_sea_water_velocity": (("time", "lat", "lon"), np.zeros((len(times), 1, 1), dtype=np.float32)),
                    "y_sea_water_velocity": (("time", "lat", "lon"), np.zeros((len(times), 1, 1), dtype=np.float32)),
                },
                coords={"time": times, "lat": [28.0], "lon": [-88.0]},
                attrs={"non_scientific_smoke": "true"},
            )
            ds.to_netcdf(path)

            row = validate_prepared_forcing_file(
                path,
                "current",
                "2010-05-20T00:00:00Z",
                "2010-05-23T23:59:59Z",
                {
                    "source_role": "current",
                    "provider": "test",
                    "dataset_product_id": "test",
                    "access_method": "file",
                    "scientific_ready": False,
                    "source_is_smoke_only": False,
                    "exact_reason_if_false": "",
                },
            )

        self.assertEqual(row["reader_compatibility_status"], "rejected_smoke_attrs")
        self.assertFalse(row["scientific_ready"])
        self.assertTrue(row["source_is_smoke_only"])


if __name__ == "__main__":
    unittest.main()
