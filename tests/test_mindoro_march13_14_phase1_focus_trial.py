import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import yaml

from src.services.mindoro_march13_14_phase1_focus_trial import (
    MindoroMarch1314Phase1FocusTrialService,
    TRIAL_OUTPUT_DIR_NAME_DEFAULT,
)


class MindoroMarch1314Phase1FocusTrialTests(unittest.TestCase):
    def test_trial_replay_writes_non_canonical_comparison_against_canonical_b1(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            case_output = root / "output" / "CASE_MINDORO_RETRO_2023"
            canonical_dir = case_output / "phase3b_extended_public_scored_march13_14_reinit"
            canonical_dir.mkdir(parents=True, exist_ok=True)
            candidate_path = root / "output" / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_baseline_selection_candidate.yaml"
            candidate_path.parent.mkdir(parents=True, exist_ok=True)
            candidate_path.write_text(
                yaml.safe_dump({"selected_recipe": "cmems_gfs"}, sort_keys=False),
                encoding="utf-8",
            )
            pd.DataFrame(
                [
                    {
                        "branch_id": "R1_previous",
                        "branch_precedence": 2,
                        "mean_fss": 0.10,
                        "fss_1km": 0.00,
                        "fss_3km": 0.04,
                        "fss_5km": 0.13,
                        "fss_10km": 0.24,
                        "iou": 0.0,
                        "dice": 0.0,
                        "nearest_distance_to_obs_m": 1414.21,
                    }
                ]
            ).to_csv(canonical_dir / "march13_14_reinit_summary.csv", index=False)
            (canonical_dir / "march13_14_reinit_run_manifest.json").write_text(
                json.dumps({"recipe": {"recipe": "cmems_era5"}}),
                encoding="utf-8",
            )

            case_stub = SimpleNamespace(
                workflow_mode="mindoro_retro_2023",
                is_official=True,
                run_name="CASE_MINDORO_RETRO_2023",
            )

            def _fake_case_output_dir(run_name=None):
                return root / "output" / str(run_name or case_stub.run_name)

            def _fake_reinit_runner():
                output_dir_name = os.environ["PHASE3B_REINIT_OUTPUT_DIR_NAME"]
                trial_dir = case_output / output_dir_name
                trial_dir.mkdir(parents=True, exist_ok=True)
                pd.DataFrame(
                    [
                        {
                            "branch_id": "R1_previous",
                            "branch_precedence": 2,
                            "mean_fss": 0.15,
                            "fss_1km": 0.01,
                            "fss_3km": 0.05,
                            "fss_5km": 0.16,
                            "fss_10km": 0.28,
                            "iou": 0.01,
                            "dice": 0.02,
                            "nearest_distance_to_obs_m": 1200.0,
                        }
                    ]
                ).to_csv(trial_dir / "march13_14_reinit_summary.csv", index=False)
                (trial_dir / "march13_14_reinit_run_manifest.json").write_text(
                    json.dumps({"recipe": {"recipe": "cmems_gfs"}}),
                    encoding="utf-8",
                )
                return {
                    "output_dir": str(trial_dir),
                    "summary_csv": str(trial_dir / "march13_14_reinit_summary.csv"),
                    "run_manifest_json": str(trial_dir / "march13_14_reinit_run_manifest.json"),
                }

            with (
                mock.patch(
                    "src.services.mindoro_march13_14_phase1_focus_trial.get_case_context",
                    return_value=case_stub,
                ),
                mock.patch(
                    "src.services.mindoro_march13_14_phase1_focus_trial.get_case_output_dir",
                    side_effect=_fake_case_output_dir,
                ),
                mock.patch(
                    "src.services.mindoro_march13_14_phase1_focus_trial.run_phase3b_extended_public_scored_march13_14_reinit",
                    side_effect=_fake_reinit_runner,
                ),
            ):
                service = MindoroMarch1314Phase1FocusTrialService(repo_root=root)
                results = service.run()

            self.assertEqual(results["experimental_recipe"], "cmems_gfs")
            self.assertEqual(results["canonical_recipe"], "cmems_era5")
            self.assertEqual(results["comparison_verdict"], "improved")
            self.assertTrue((case_output / TRIAL_OUTPUT_DIR_NAME_DEFAULT / "march13_14_reinit_phase1_focus_trial_report.md").exists())
            self.assertTrue((case_output / TRIAL_OUTPUT_DIR_NAME_DEFAULT / "march13_14_reinit_phase1_focus_trial_manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
