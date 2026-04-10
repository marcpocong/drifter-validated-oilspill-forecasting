import unittest

import pandas as pd

from src.services.phase3c_dwh_pygnome_comparator import determine_recommendation


class Phase3CDwhPyGnomeComparatorTests(unittest.TestCase):
    def test_recommendation_returns_mixed_when_event_and_mean_disagree(self):
        summary = pd.DataFrame(
            [
                {
                    "track_id": "opendrift_control",
                    "pair_role": "per_date",
                    "fss_1km": 0.42,
                    "fss_3km": 0.44,
                    "fss_5km": 0.46,
                    "fss_10km": 0.48,
                },
                {
                    "track_id": "opendrift_control",
                    "pair_role": "event_corridor",
                    "fss_1km": 0.60,
                    "fss_3km": 0.62,
                    "fss_5km": 0.64,
                    "fss_10km": 0.66,
                },
                {
                    "track_id": "ensemble_p50",
                    "pair_role": "per_date",
                    "fss_1km": 0.55,
                    "fss_3km": 0.57,
                    "fss_5km": 0.58,
                    "fss_10km": 0.60,
                },
                {
                    "track_id": "ensemble_p50",
                    "pair_role": "event_corridor",
                    "fss_1km": 0.50,
                    "fss_3km": 0.52,
                    "fss_5km": 0.53,
                    "fss_10km": 0.54,
                },
            ]
        )

        recommendation = determine_recommendation(summary)

        self.assertEqual(recommendation, "the result is mixed by metric/date; next step: final packaging/chapter sync")

    def test_recommendation_prefers_clear_pygnome_winner(self):
        summary = pd.DataFrame(
            [
                {
                    "track_id": "pygnome_deterministic",
                    "pair_role": "per_date",
                    "fss_1km": 0.70,
                    "fss_3km": 0.72,
                    "fss_5km": 0.74,
                    "fss_10km": 0.76,
                },
                {
                    "track_id": "pygnome_deterministic",
                    "pair_role": "event_corridor",
                    "fss_1km": 0.71,
                    "fss_3km": 0.73,
                    "fss_5km": 0.75,
                    "fss_10km": 0.77,
                },
                {
                    "track_id": "opendrift_control",
                    "pair_role": "per_date",
                    "fss_1km": 0.40,
                    "fss_3km": 0.42,
                    "fss_5km": 0.44,
                    "fss_10km": 0.46,
                },
                {
                    "track_id": "opendrift_control",
                    "pair_role": "event_corridor",
                    "fss_1km": 0.45,
                    "fss_3km": 0.47,
                    "fss_5km": 0.49,
                    "fss_10km": 0.51,
                },
            ]
        )

        recommendation = determine_recommendation(summary)

        self.assertEqual(recommendation, "PyGNOME deterministic is strongest on DWH; next step: more DWH model harmonization")


if __name__ == "__main__":
    unittest.main()
