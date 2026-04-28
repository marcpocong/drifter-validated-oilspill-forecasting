import json
import shutil
import tempfile
import unittest
from pathlib import Path

from src.services.panel_b1_drifter_context import run_panel_b1_drifter_context


REPO_ROOT = Path(__file__).resolve().parents[1]


class PanelB1DrifterContextTests(unittest.TestCase):
    def test_service_builds_transport_provenance_context_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_paths = (
                REPO_ROOT / "output" / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_accepted_segment_registry.csv",
                REPO_ROOT / "output" / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_ranking_subset_registry.csv",
                REPO_ROOT / "output" / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_production_manifest.json",
                REPO_ROOT / "output" / "Phase 3B March13-14 Final Output" / "summary" / "opendrift_primary" / "march13_14_reinit_run_manifest.json",
                REPO_ROOT / "config" / "phase1_mindoro_focus_pre_spill_2016_2023.yaml",
            )
            for source_path in source_paths:
                destination = root / source_path.relative_to(REPO_ROOT)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, destination)

            results = run_panel_b1_drifter_context(repo_root=root)
            manifest_path = root / "output" / "panel_drifter_context" / "b1_drifter_context_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertTrue(Path(results["manifest_path"]).name.endswith("b1_drifter_context_manifest.json"))
        self.assertEqual(manifest["output_role"], "transport_provenance_context_only")
        self.assertTrue(manifest["no_science_rerun"])
        self.assertEqual(manifest["official_b1_recipe"], "cmems_gfs")
        self.assertFalse(manifest["direct_march13_14_2023_accepted_segments_found"])
        self.assertIn("No direct March 13-14 2023 accepted drifter segment is stored for B1", manifest["status"])

    def test_service_handles_missing_sources_without_exception(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results = run_panel_b1_drifter_context(repo_root=root)
            manifest_path = root / "output" / "panel_drifter_context" / "b1_drifter_context_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(manifest_path.exists())

        self.assertEqual(manifest["output_role"], "transport_provenance_context_only")
        self.assertTrue(manifest["no_science_rerun"])
        self.assertFalse(manifest["direct_march13_14_2023_accepted_segments_found"])
        self.assertFalse(manifest["map_generated"])
        self.assertEqual(results["accepted_segment_count"], 0)


if __name__ == "__main__":
    unittest.main()
