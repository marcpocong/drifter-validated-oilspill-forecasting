import unittest

from src.services.public_obs_appendix import (
    classify_inventory_acceptance,
    is_within_current_horizon,
    parse_obs_date,
)


class PublicObservationAppendixTests(unittest.TestCase):
    def test_parse_obs_date_handles_short_and_long_title_dates(self):
        self.assertEqual(parse_obs_date("MindoroOilSpill_Philsa_230304"), "2023-03-04")
        self.assertEqual(parse_obs_date("MindoroOilSpill_NOAA_20230331"), "2023-03-31")
        self.assertEqual(parse_obs_date("Acquired: 07/03/2023"), "2023-03-07")

    def test_within_current_horizon_is_inclusive(self):
        self.assertTrue(
            is_within_current_horizon(
                "2023-03-03",
                simulation_start_utc="2023-03-03T09:59:00Z",
                simulation_end_utc="2023-03-06T09:59:00Z",
            )
        )
        self.assertTrue(
            is_within_current_horizon(
                "2023-03-06",
                simulation_start_utc="2023-03-03T09:59:00Z",
                simulation_end_utc="2023-03-06T09:59:00Z",
            )
        )
        self.assertFalse(
            is_within_current_horizon(
                "2023-03-07",
                simulation_start_utc="2023-03-03T09:59:00Z",
                simulation_end_utc="2023-03-06T09:59:00Z",
            )
        )

    def test_classify_accepts_within_horizon_polygon_and_rejects_wrappers(self):
        accept_quant, accept_qual, rejection = classify_inventory_acceptance(
            public=True,
            source_type="feature service",
            observation_derived=True,
            reproducibly_ingestible=True,
            geometry_type="polygon",
            obs_date="2023-03-04",
            within_current_72h_horizon=True,
        )
        self.assertTrue(accept_quant)
        self.assertTrue(accept_qual)
        self.assertEqual(rejection, "")

        accept_quant, accept_qual, rejection = classify_inventory_acceptance(
            public=True,
            source_type="web mapping application",
            observation_derived=False,
            reproducibly_ingestible=False,
            geometry_type="unknown",
            obs_date="",
            within_current_72h_horizon=False,
        )
        self.assertFalse(accept_quant)
        self.assertTrue(accept_qual)
        self.assertIn("wrapper", rejection)

        accept_quant, accept_qual, rejection = classify_inventory_acceptance(
            public=True,
            source_type="feature service",
            observation_derived=True,
            reproducibly_ingestible=True,
            geometry_type="polygon",
            obs_date="2023-03-07",
            within_current_72h_horizon=False,
        )
        self.assertFalse(accept_quant)
        self.assertTrue(accept_qual)
        self.assertIn("extended-horizon", rejection)


if __name__ == "__main__":
    unittest.main()
