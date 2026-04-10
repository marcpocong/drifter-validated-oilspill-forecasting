import os
import unittest
from unittest import mock

from src.core.case_context import get_case_context
from src.services.phase3b_extended_public_scored import (
    EVENT_CORRIDOR_DATES,
    SHORT_DATES,
    resolve_short_extended_window,
)


class Phase3BExtendedPublicScoredTests(unittest.TestCase):
    def tearDown(self):
        get_case_context.cache_clear()

    def test_short_window_extends_forcing_past_last_local_validation_date(self):
        with mock.patch.dict(
            os.environ,
            {
                "WORKFLOW_MODE": "mindoro_retro_2023",
                "EXTENDED_PUBLIC_TIER": "short",
            },
            clear=False,
        ):
            get_case_context.cache_clear()
            window = resolve_short_extended_window()

        self.assertEqual(window.validation_dates, SHORT_DATES)
        self.assertEqual(window.simulation_start_utc, "2023-03-03T09:59:00Z")
        self.assertEqual(window.simulation_end_utc, "2023-03-09T15:59:00Z")
        self.assertEqual(window.required_forcing_end_utc, "2023-03-09T18:59:00Z")
        self.assertEqual(window.download_end_date, "2023-03-10")
        self.assertEqual(window.end_selection_source, "default_short_end_local")

    def test_explicit_local_end_is_recorded(self):
        with mock.patch.dict(
            os.environ,
            {
                "WORKFLOW_MODE": "mindoro_retro_2023",
                "EXTENDED_PUBLIC_TIER": "short",
                "EXTENDED_PUBLIC_END_LOCAL": "2023-03-09 23:59 Asia/Manila",
            },
            clear=False,
        ):
            get_case_context.cache_clear()
            window = resolve_short_extended_window()

        self.assertEqual(window.simulation_end_utc, "2023-03-09T15:59:00Z")
        self.assertEqual(window.end_selection_source, "EXTENDED_PUBLIC_END_LOCAL")

    def test_only_short_tier_is_enabled_for_this_patch(self):
        with mock.patch.dict(
            os.environ,
            {
                "WORKFLOW_MODE": "mindoro_retro_2023",
                "EXTENDED_PUBLIC_TIER": "medium",
            },
            clear=False,
        ):
            get_case_context.cache_clear()
            with self.assertRaisesRegex(RuntimeError, "only EXTENDED_PUBLIC_TIER=short"):
                resolve_short_extended_window()

    def test_event_corridor_model_dates_include_within_horizon_context(self):
        self.assertEqual(EVENT_CORRIDOR_DATES[0], "2023-03-04")
        self.assertEqual(EVENT_CORRIDOR_DATES[-1], "2023-03-09")
        for date in SHORT_DATES:
            self.assertIn(date, EVENT_CORRIDOR_DATES)
        self.assertNotIn("2023-03-03", EVENT_CORRIDOR_DATES)


if __name__ == "__main__":
    unittest.main()
