import unittest

from src.services.final_validation_package import decide_final_structure, mean_fss


class FinalValidationPackageTests(unittest.TestCase):
    def test_mean_fss_averages_available_windows(self):
        row = {
            "fss_1km": 0.1,
            "fss_3km": 0.2,
            "fss_5km": 0.3,
            "fss_10km": 0.4,
        }
        self.assertAlmostEqual(mean_fss(row), 0.25)

    def test_decide_final_structure_returns_thesis_packaging_guidance(self):
        recommendation = decide_final_structure()
        self.assertIn("Mindoro B1", recommendation)
        self.assertIn("DWH Phase 3C", recommendation)
        self.assertIn("appendix", recommendation.lower())


if __name__ == "__main__":
    unittest.main()
