import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import geopandas as gpd
import yaml
from shapely.geometry import Point, Polygon

from src.core.case_context import get_case_context
from src.services.phase3c_external_case_setup import (
    SOURCE_TAXONOMY_MODELED,
    SOURCE_TAXONOMY_OBS,
    build_external_case_service_inventory,
    build_scoring_grid_spec_from_projected_gdfs,
    classify_dwh_public_layer,
    derive_projected_crs_from_wgs84,
    parse_external_case_layer_registry,
)


CONFIG_PATH = Path("config/case_dwh_retro_2010_72h.yaml")


class Phase3CExternalCaseSetupTests(unittest.TestCase):
    def setUp(self):
        get_case_context.cache_clear()
        self.addCleanup(get_case_context.cache_clear)

    def _load_cfg(self):
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}

    def test_dwh_workflow_mode_resolves_case_config(self):
        with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "dwh_retro_2010"}, clear=False):
            get_case_context.cache_clear()
            case = get_case_context()

        self.assertEqual(case.workflow_mode, "dwh_retro_2010")
        self.assertEqual(case.case_id, "CASE_DWH_RETRO_2010_72H")
        self.assertFalse(case.is_prototype)
        self.assertEqual(case.initialization_layer.layer_id, 5)
        self.assertEqual(case.validation_layer.layer_id, 6)
        self.assertEqual(case.provenance_layer.layer_id, 0)

    def test_service_inventory_contains_observation_and_forcing_services(self):
        cfg = self._load_cfg()
        inventory = build_external_case_service_inventory(cfg)
        roles = {row["service_role"] for row in inventory}

        self.assertIn("public_observation_primary", roles)
        self.assertIn("currents_primary", roles)
        self.assertIn("winds_primary", roles)
        self.assertIn("waves_primary", roles)
        self.assertTrue(any("FeatureServer" in row["service_url"] for row in inventory))

    def test_layer_registry_parses_selected_daily_layers_and_truth_policy(self):
        cfg = self._load_cfg()
        layers = parse_external_case_layer_registry(cfg)
        selected_ids = [layer.layer_id for layer in layers if layer.selected_for_phase3c]
        truth_dates = [layer.event_date for layer in layers if layer.use_as_truth]

        self.assertEqual(selected_ids, [0, 5, 6, 7, 8])
        self.assertEqual(truth_dates, ["2010-05-21", "2010-05-22", "2010-05-23"])
        self.assertEqual(classify_dwh_public_layer(7, "T20100522_Composite", "esriGeometryPolygon")[0], SOURCE_TAXONOMY_OBS)
        self.assertEqual(classify_dwh_public_layer(None, "DWH trajectory forecast", "polygon")[0], SOURCE_TAXONOMY_MODELED)

    def test_external_scoring_grid_spec_uses_geometry_derived_utm_not_mindoro(self):
        wellhead = gpd.GeoDataFrame(
            {"name": ["wellhead"]},
            geometry=[Point(-88.3659, 28.7381)],
            crs="EPSG:4326",
        )
        slick = gpd.GeoDataFrame(
            {"name": ["slick"]},
            geometry=[
                Polygon(
                    [
                        (-88.6, 28.6),
                        (-88.1, 28.6),
                        (-88.1, 29.0),
                        (-88.6, 29.0),
                        (-88.6, 28.6),
                    ]
                )
            ],
            crs="EPSG:4326",
        )
        target_crs = derive_projected_crs_from_wgs84([wellhead, slick])

        self.assertEqual(target_crs, "EPSG:32616")
        self.assertNotEqual(target_crs, "EPSG:32651")

        with tempfile.TemporaryDirectory() as tmp_dir:
            projected = [wellhead.to_crs(target_crs), slick.to_crs(target_crs)]
            spec = build_scoring_grid_spec_from_projected_gdfs(
                projected,
                target_crs=target_crs,
                output_dir=tmp_dir,
            )

        self.assertEqual(spec.crs, "EPSG:32616")
        self.assertEqual(spec.resolution, 1000.0)
        self.assertGreater(spec.width, 0)
        self.assertGreater(spec.height, 0)
        self.assertTrue(str(spec.metadata_path).endswith("scoring_grid.yaml"))


if __name__ == "__main__":
    unittest.main()
