import tempfile
import unittest
from pathlib import Path

from src.services.recipe_sensitivity import RecipeSensitivityService
from src.utils.io import get_recipe_sensitivity_run_name


class RecipeSensitivityTests(unittest.TestCase):
    def test_recipe_sensitivity_run_name_is_nested_and_posix(self):
        run_name = get_recipe_sensitivity_run_name("cmems_era5", run_name="CASE_MINDORO_RETRO_2023")
        self.assertEqual(run_name, "CASE_MINDORO_RETRO_2023/recipe_sensitivity/cmems_era5")

    def test_candidate_specs_flag_missing_gfs_wind(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            forcing_dir = Path(tmpdir)
            for name in ("cmems_curr.nc", "hycom_curr.nc", "era5_wind.nc", "cmems_wave.nc"):
                (forcing_dir / name).write_text("", encoding="utf-8")

            service = object.__new__(RecipeSensitivityService)
            service.current_forcing_dir = forcing_dir

            specs = {spec.recipe_id: spec for spec in service.get_candidate_specs()}

            self.assertTrue(specs["cmems_era5"].available)
            self.assertTrue(specs["hycom_era5"].available)
            self.assertFalse(specs["cmems_gfs"].available)
            self.assertFalse(specs["hycom_gfs"].available)
            self.assertTrue(any(path.endswith("gfs_wind.nc") for path in specs["cmems_gfs"].missing_inputs))


if __name__ == "__main__":
    unittest.main()
