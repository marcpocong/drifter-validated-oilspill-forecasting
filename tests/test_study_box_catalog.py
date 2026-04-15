import unittest

from src.core.study_box_catalog import (
    ARCHIVE_ONLY_STUDY_BOX_NUMBERS,
    THESIS_FACING_STUDY_BOX_NUMBERS,
    parse_study_box_numbers,
    study_box_figure_metadata,
)


class StudyBoxCatalogTests(unittest.TestCase):
    def test_catalog_tracks_box_numbers_and_surface_rules(self):
        self.assertEqual(THESIS_FACING_STUDY_BOX_NUMBERS, ("2", "4"))
        self.assertEqual(ARCHIVE_ONLY_STUDY_BOX_NUMBERS, ("1", "3"))

        box_2 = study_box_figure_metadata("mindoro_case_domain")
        self.assertEqual(box_2["study_box_numbers"], "2")
        self.assertTrue(box_2["thesis_surface_allowed"])

        box_1 = study_box_figure_metadata("focused_phase1_validation_box")
        self.assertEqual(box_1["study_box_numbers"], "1")
        self.assertFalse(box_1["thesis_surface_allowed"])

        overview = study_box_figure_metadata("thesis_study_boxes_reference")
        self.assertEqual(overview["study_box_numbers"], "2,4")
        self.assertEqual(parse_study_box_numbers(overview["study_box_numbers"]), ("2", "4"))


if __name__ == "__main__":
    unittest.main()
