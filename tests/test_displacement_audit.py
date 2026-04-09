import unittest

from src.services.displacement_audit import rank_displacement_hypotheses
from src.services.ingestion import derive_bbox_from_display_bounds


class DisplacementAuditTests(unittest.TestCase):
    def test_derive_bbox_from_display_bounds_applies_half_degree_halo(self):
        bbox = derive_bbox_from_display_bounds(
            [120.90964677179262, 122.0621541786303, 12.249384840763462, 13.783655303175253],
            halo_degrees=0.5,
        )
        self.assertEqual(
            bbox,
            [120.40964677179262, 122.5621541786303, 11.749384840763462, 14.283655303175253],
        )

    def test_rank_prefers_shoreline_when_mask_is_all_sea(self):
        ranked = rank_displacement_hypotheses(
            {
                "sea_mask_all_sea": True,
                "runtime_landmask_disabled": True,
                "local_strip_land_cells": 0,
                "current_tail_extension_run_count": 51,
                "current_tail_extension_max_gap_hours": 12.98,
                "recipe_best_mean_fss": 0.0,
                "recipe_best_centroid_gain_m": 3569.6,
                "forcing_bounds_match_legacy_region_plus_pad": True,
                "forcing_bounds_cover_canonical_halo": True,
                "runtime_grid_is_canonical": True,
                "march3_rescue_applied": True,
                "release_to_init_vector_centroid_m": 5277.9,
                "release_to_init_raster_centroid_m": 218.0,
                "control_distance_growth_m": 104506.6,
                "wave_reader_loaded_for_all_runs": True,
            }
        )
        self.assertEqual(ranked[0]["hypothesis_id"], "coastal_masking_or_missing_shoreline_interaction")
        self.assertEqual(ranked[0]["recommended_rerun"], "shoreline-mask rerun")


if __name__ == "__main__":
    unittest.main()
