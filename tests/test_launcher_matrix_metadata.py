import json
import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
LAUNCHER_MATRIX_PATH = REPO_ROOT / "config" / "launcher_matrix.json"
ABSOLUTE_LOCAL_LINK_PATTERN = re.compile(r"(?i)(?:^|[\(\s])/?[a-z]:/users/")


class LauncherMatrixMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.matrix = json.loads(LAUNCHER_MATRIX_PATH.read_text(encoding="utf-8"))
        cls.entries = cls.matrix["entries"]
        cls.entry_map = {entry["entry_id"]: entry for entry in cls.entries}

    def test_all_entries_have_required_fields(self):
        required_fields = (
            "entry_id",
            "label",
            "category_id",
            "workflow_mode",
            "rerun_cost",
            "safe_default",
            "steps",
            "thesis_role",
            "draft_section",
            "claim_boundary",
            "run_kind",
            "recommended_for",
            "confirms_before_run",
        )

        for entry in self.entries:
            for field in required_fields:
                self.assertIn(field, entry, f"{entry.get('entry_id', '<missing>')} is missing '{field}'")
            self.assertTrue(entry["entry_id"])
            self.assertTrue(entry["label"])
            self.assertTrue(entry["category_id"])
            self.assertTrue(entry["workflow_mode"])
            self.assertTrue(entry["rerun_cost"])
            self.assertTrue(entry["thesis_role"])
            self.assertTrue(entry["draft_section"])
            self.assertTrue(entry["claim_boundary"])
            self.assertTrue(entry["run_kind"])
            self.assertTrue(entry["recommended_for"])
            self.assertIsInstance(entry["safe_default"], bool)
            self.assertIsInstance(entry["confirms_before_run"], bool)
            self.assertIsInstance(entry["steps"], list)
            self.assertTrue(entry["steps"], f"{entry['entry_id']} must define at least one step")

    def test_new_aliases_exist_and_old_ids_remain(self):
        self.assertIn("phase1_mindoro_focus_provenance", self.entry_map)
        self.assertIn("phase1_mindoro_focus_pre_spill_experiment", self.entry_map)
        self.assertEqual(
            self.entry_map["phase1_mindoro_focus_pre_spill_experiment"].get("alias_of"),
            "phase1_mindoro_focus_provenance",
        )

        self.assertIn("phase1_regional_reference_rerun", self.entry_map)
        self.assertIn("phase1_production_rerun", self.entry_map)
        self.assertEqual(
            self.entry_map["phase1_production_rerun"].get("alias_of"),
            "phase1_regional_reference_rerun",
        )

    def test_aliases_point_to_existing_entries_and_duplicate_steps_safely(self):
        for entry in self.entries:
            alias_of = entry.get("alias_of")
            if not alias_of:
                continue

            self.assertIn(alias_of, self.entry_map, f"{entry['entry_id']} points to missing alias target {alias_of}")
            target = self.entry_map[alias_of]
            self.assertEqual(entry["workflow_mode"], target["workflow_mode"])
            self.assertEqual(entry["rerun_cost"], target["rerun_cost"])
            self.assertEqual(entry["safe_default"], target["safe_default"])
            self.assertEqual(entry["steps"], target["steps"])

    def test_no_local_absolute_links_in_readme_or_docs(self):
        markdown_paths = [REPO_ROOT / "README.md", *sorted((REPO_ROOT / "docs").rglob("*.md"))]
        offenders = []

        for path in markdown_paths:
            text = path.read_text(encoding="utf-8")
            if ABSOLUTE_LOCAL_LINK_PATTERN.search(text):
                offenders.append(str(path.relative_to(REPO_ROOT)))

        self.assertFalse(offenders, f"Found local absolute paths in markdown: {offenders}")

    def test_panel_safe_entries_remain_read_only_or_packaging_only(self):
        panel_safe_entry_ids = (
            "phase1_audit",
            "phase2_audit",
            "final_validation_package",
            "phase5_sync",
            "trajectory_gallery",
            "trajectory_gallery_panel",
            "figure_package_publication",
        )

        for entry_id in panel_safe_entry_ids:
            entry = self.entry_map[entry_id]
            self.assertTrue(entry["safe_default"], entry_id)
            self.assertIn(entry["run_kind"], {"read_only", "packaging_only"}, entry_id)

    def test_b1_claim_boundary_mentions_shared_imagery_caveat(self):
        boundary = self.entry_map["mindoro_phase3b_primary_public_validation"]["claim_boundary"].lower()
        self.assertRegex(boundary, r"shared[- ]imagery")

    def test_pygnome_entries_keep_comparator_only_wording(self):
        offenders = []

        for entry in self.entries:
            payload = json.dumps(entry).lower()
            if "pygnome" not in payload:
                continue

            combined = " ".join(
                str(entry.get(field, ""))
                for field in ("label", "description", "notes", "claim_boundary")
            ).lower()
            if "comparator-only" not in combined and "comparator only" not in combined:
                offenders.append(entry["entry_id"])

        self.assertFalse(offenders, f"PyGNOME entries lost comparator-only wording: {offenders}")

    def test_dwh_entry_mentions_external_transfer_validation_not_recalibration(self):
        combined = " ".join(
            str(self.entry_map["dwh_reportable_bundle"].get(field, ""))
            for field in ("description", "notes", "claim_boundary")
        ).lower()
        self.assertIn("external transfer", combined)
        self.assertIn("not mindoro recalibration", combined)


if __name__ == "__main__":
    unittest.main()
