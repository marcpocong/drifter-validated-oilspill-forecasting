import unittest

from src.services.horizon_survival_audit import classify_horizon_survival


class HorizonSurvivalAuditTests(unittest.TestCase):
    def test_classifies_terminal_stranding_as_beaching_retention(self):
        diagnosis, rerun, reason = classify_horizon_survival(
            {
                "terminal_stranding_fraction": 0.95,
                "runs_with_any_active": 50,
                "last_active_time_utc": "2023-03-05T01:59:00Z",
                "writer_mismatch_detected": False,
                "late_active_outside_domain": False,
                "late_active_masked_by_ocean": False,
                "late_active_low_probability": False,
                "march7_9_prob_presence_zero": True,
            }
        )
        self.assertEqual(diagnosis, "C")
        self.assertEqual(rerun, "transport/retention fix rerun")
        self.assertIn("stranded", reason)

    def test_prioritizes_writer_mismatch_when_raw_signal_exists(self):
        diagnosis, rerun, _ = classify_horizon_survival(
            {
                "writer_mismatch_detected": True,
                "terminal_stranding_fraction": 1.0,
            }
        )
        self.assertEqual(diagnosis, "E")
        self.assertEqual(rerun, "aggregation/writer fix rerun")

    def test_classifies_late_low_occupancy_as_threshold_collapse(self):
        diagnosis, rerun, reason = classify_horizon_survival(
            {
                "terminal_stranding_fraction": 0.0,
                "late_active_low_probability": True,
                "late_active_outside_domain": False,
                "late_active_masked_by_ocean": False,
                "writer_mismatch_detected": False,
            }
        )
        self.assertEqual(diagnosis, "D")
        self.assertEqual(rerun, "convergence rerun")
        self.assertIn("0.50", reason)


if __name__ == "__main__":
    unittest.main()
