import unittest

from src.services.phase3b_multidate_public import (
    SOURCE_TAXONOMY_MODELED,
    SOURCE_TAXONOMY_OBS,
    classify_public_source,
)


class Phase3BMultidatePublicTests(unittest.TestCase):
    def test_classifies_machine_readable_observation_layer_as_quantitative(self):
        taxonomy, reason = classify_public_source(
            {
                "source_name": "Possible_oil_slick_(March_6,_2023)",
                "provider": "WWF Philippines",
                "obs_date": "2023-03-06",
                "source_type": "feature layer",
                "machine_readable": True,
                "public": True,
                "observation_derived": True,
                "reproducibly_ingestible": True,
                "geometry_type": "polygon",
                "accept_for_appendix_quantitative": True,
                "notes": "Public March 6 validation polygon.",
            }
        )
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_OBS)
        self.assertIn("observation-derived", reason)

    def test_excludes_trajectory_model_from_truth_even_if_previous_appendix_accepted_it(self):
        taxonomy, reason = classify_public_source(
            {
                "source_name": "MindoroOilSpill_MSI_230305",
                "provider": "UP MSI",
                "obs_date": "2023-03-05",
                "source_type": "feature service",
                "machine_readable": True,
                "public": True,
                "observation_derived": True,
                "reproducibly_ingestible": True,
                "geometry_type": "polygon",
                "accept_for_appendix_quantitative": True,
                "notes": "MT Princess Empress Oil Spill Trajectory Model from UP Marine Science Institute",
            }
        )
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_MODELED)
        self.assertIn("modeled", reason)


if __name__ == "__main__":
    unittest.main()
