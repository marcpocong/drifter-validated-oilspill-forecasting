import unittest

from src.services.phase3b_extended_public import classify_extended_public_source
from src.services.phase3b_multidate_public import (
    SOURCE_TAXONOMY_MODELED,
    SOURCE_TAXONOMY_OBS,
    SOURCE_TAXONOMY_QUALITATIVE,
)


class Phase3BExtendedPublicClassificationTests(unittest.TestCase):
    def _base_row(self, **overrides):
        row = {
            "source_name": "MindoroOilSpill_Philsa_230307",
            "provider": "PhilSA",
            "obs_date": "2023-03-07",
            "source_type": "feature service",
            "machine_readable": True,
            "public": True,
            "observation_derived": True,
            "reproducibly_ingestible": True,
            "geometry_type": "polygon",
            "service_url": "https://example.test/FeatureServer",
            "layer_id": "0",
            "notes": "RCM acquired 07/03/2023.",
        }
        row.update(overrides)
        return row

    def test_accepts_beyond_horizon_observation_polygon(self):
        taxonomy, reason, accepted = classify_extended_public_source(self._base_row())
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_OBS)
        self.assertTrue(accepted)
        self.assertIn("beyond-horizon", reason)

    def test_accepts_numeric_zero_layer_id_from_csv(self):
        taxonomy, _, accepted = classify_extended_public_source(self._base_row(layer_id=0.0))
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_OBS)
        self.assertTrue(accepted)

    def test_excludes_trajectory_model_from_truth(self):
        taxonomy, reason, accepted = classify_extended_public_source(
            self._base_row(
                source_name="MindoroOilSpill_MSI_230307",
                provider="UP MSI",
                notes="Trajectory Model product.",
            )
        )
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_MODELED)
        self.assertFalse(accepted)
        self.assertIn("modeled", reason)

    def test_excludes_march3_initialization_date(self):
        taxonomy, reason, accepted = classify_extended_public_source(self._base_row(obs_date="2023-03-03"))
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_QUALITATIVE)
        self.assertFalse(accepted)
        self.assertIn("initialization", reason)

    def test_excludes_within_horizon_date(self):
        taxonomy, reason, accepted = classify_extended_public_source(self._base_row(obs_date="2023-03-06"))
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_QUALITATIVE)
        self.assertFalse(accepted)
        self.assertIn("within-horizon", reason)


if __name__ == "__main__":
    unittest.main()
