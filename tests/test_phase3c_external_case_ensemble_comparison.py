import unittest

import pandas as pd

from src.services.phase3c_external_case_ensemble_comparison import determine_recommendation


class Phase3CExternalCaseEnsembleComparisonTests(unittest.TestCase):
    def _summary(self, values):
        return pd.DataFrame(values)

    def test_recommendation_prefers_clear_best_track(self):
        deterministic = self._summary(
            [
                {"fss_1km": 0.30, "fss_3km": 0.35, "fss_5km": 0.36, "fss_10km": 0.40},
                {"fss_1km": 0.31, "fss_3km": 0.36, "fss_5km": 0.37, "fss_10km": 0.41},
            ]
        )
        p50 = self._summary(
            [
                {"fss_1km": 0.50, "fss_3km": 0.55, "fss_5km": 0.56, "fss_10km": 0.60},
                {"fss_1km": 0.49, "fss_3km": 0.54, "fss_5km": 0.55, "fss_10km": 0.59},
            ]
        )

        recommendation = determine_recommendation(
            {"opendrift_control": deterministic, "ensemble_p50": p50}
        )

        self.assertEqual(recommendation, "ensemble p50 is stronger; next step: final packaging/chapter sync")

    def test_recommendation_returns_mixed_when_tracks_are_close(self):
        deterministic = self._summary([{"fss_1km": 0.50, "fss_3km": 0.50, "fss_5km": 0.50, "fss_10km": 0.50}])
        p90 = self._summary([{"fss_1km": 0.505, "fss_3km": 0.505, "fss_5km": 0.505, "fss_10km": 0.505}])

        recommendation = determine_recommendation(
            {"opendrift_control": deterministic, "ensemble_p90": p90}
        )

        self.assertEqual(recommendation, "the result is mixed by metric/date; next step: final packaging/chapter sync")


if __name__ == "__main__":
    unittest.main()
