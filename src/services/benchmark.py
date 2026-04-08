"""
Phase 1 Benchmark Pipeline Orchestrator.
Creates a single 72-hour benchmark run comparing OpenDrift and PyGNOME outputs on a standardized grid.
"""

import logging
import yaml
import pandas as pd
from pathlib import Path

from src.helpers.metrics import calculate_fss, calculate_kl_divergence
from src.helpers.raster import GridBuilder, rasterize_model_output
from src.helpers.scoring import precheck_same_grid
from src.utils.io import get_forcing_files

# We will import the weathering implementations so we can run them directly
from src.services.weathering import OilWeatheringService
from src.services.gnome_comparison import GnomeComparisonService

logger = logging.getLogger(__name__)

class BenchmarkPipeline:
    def __init__(self, output_base: str = None):
        from src.core.constants import RUN_NAME
        self.run_id = RUN_NAME
        if output_base is None:
            self.base_dir = Path("output") / self.run_id / "benchmark"
        else:
            self.base_dir = Path(output_base) / self.run_id / "benchmark"
        self.setup_directories()

        # Attach a dedicated file handler to this benchmark's named logger only,
        # so we never reconfigure the root logger (which logging.basicConfig would do).
        self.logger = logging.getLogger(f"Benchmark_{self.run_id}")
        self.logger.setLevel(logging.INFO)
        _handler = logging.FileHandler(self.base_dir / "logs" / "run.log")
        _handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(_handler)
        
    def setup_directories(self):
        import os
        os.makedirs(self.base_dir, exist_ok=True)
        for sub in ["grid", "opendrift", "pygnome", "metrics", "logs"]:
            os.makedirs(self.base_dir / sub, exist_ok=True)

    def generate_config_snapshot(self, best_recipe: str, start_lat: float, start_lon: float, start_time: str, oil_cfg: dict, grid: GridBuilder):
        config_snapshot = {
            "case_id": self.run_id,
            "start_time": start_time,
            "start_lat": start_lat,
            "start_lon": start_lon,
            "recipe": best_recipe,
            "oil": oil_cfg,
            "grid": grid.spec.to_metadata(),
        }
        with open(self.base_dir / "config_snapshot.yaml", "w") as f:
            yaml.dump(config_snapshot, f, indent=2)

    def run(self, best_recipe: str, start_lat: float, start_lon: float, start_time: str, base_config_path: str = "config/oil.yaml"):
        self.logger.info(f"Starting Benchmark RUN_ID: {self.run_id}")
        print(f"🚀 Starting Benchmark Case {self.run_id}")
        
        with open(base_config_path, "r") as f:
            full_config = yaml.safe_load(f)
        
        # Pick just the first oil type from the config for the benchmark case
        oil_key = list(full_config["oils"].keys())[0]
        oil_cfg = full_config["oils"][oil_key]
        
        # 1. Setup Grid
        grid = GridBuilder()
        grid.save_metadata(self.base_dir / "grid" / "grid.json")
        self.logger.info("Grid metadata saved.")

        # 2. Config Snapshot
        self.generate_config_snapshot(best_recipe, start_lat, start_lon, start_time, oil_cfg, grid)
        
        # Resolve forcing files via the shared helper (reads recipes.yaml).
        forcing = get_forcing_files(best_recipe)
        currents_file = str(forcing['currents'])
        winds_file = str(forcing['wind'])

        # 3. Modify internal configs so outputs go to our CASE directories
        self.logger.info("Running OpenDrift simulation...")
        print("   Running OpenDrift...")
        openoil_service = OilWeatheringService(currents_file, winds_file)
        # Override output_dir
        openoil_service.output_dir = self.base_dir / "opendrift"
        
        _, od_nc_path = openoil_service._run_single(
            oil_key=oil_key,
            oil_cfg=oil_cfg,
            start_lat=start_lat,
            start_lon=start_lon,
            start_time=start_time
        )
        
        self.logger.info("Running PyGNOME simulation...")
        print("   Running PyGNOME...")
        gnome_service = GnomeComparisonService()
        gnome_service.output_dir = self.base_dir / "pygnome"
        
        _, pg_nc_path = gnome_service._run_gnome_scenario(
            oil_key=oil_key,
            oil_cfg=oil_cfg,
            start_lat=start_lat,
            start_lon=start_lon,
            start_time=start_time
        )
        
        # 4. Rasterize Outputs
        self.logger.info("Rasterizing outputs...")
        print("   Rasterizing outputs...")
        hours = [24, 48, 72]
        od_rasters = rasterize_model_output(grid, od_nc_path, "opendrift", self.base_dir / "opendrift", hours)
        
        if pg_nc_path.exists():
            pg_rasters = rasterize_model_output(grid, pg_nc_path, "pygnome", self.base_dir / "pygnome", hours)

            # 5. Calculate Metrics
            self.logger.info("Calculating FSS and KL divergence...")
            print("   Calculating FSS and KL metrics...")

            fss_records = []
            kl_records = []

            for h in hours:
                hits_precheck = precheck_same_grid(
                    od_rasters[h]["hits"],
                    pg_rasters[h]["hits"],
                    report_base_path=self.base_dir / "metrics" / "precheck" / f"hits_{h}",
                )
                probs_precheck = precheck_same_grid(
                    od_rasters[h]["probs"],
                    pg_rasters[h]["probs"],
                    report_base_path=self.base_dir / "metrics" / "precheck" / f"probability_{h}",
                )
                if not hits_precheck.passed or not probs_precheck.passed:
                    raise RuntimeError(
                        f"Benchmark same-grid precheck failed for hour {h}. "
                        f"Hits report: {hits_precheck.json_report_path} | "
                        f"Probability report: {probs_precheck.json_report_path}"
                    )

                od_hits = od_rasters[h]["hits_data"]
                pg_hits = pg_rasters[h]["hits_data"]

                od_probs = od_rasters[h]["probs_data"]
                pg_probs = pg_rasters[h]["probs_data"]

                fss_val = calculate_fss(od_hits, pg_hits, window=5)
                kl_val = calculate_kl_divergence(od_probs, pg_probs)

                fss_records.append({"hour": h, "fss": fss_val})
                kl_records.append({"hour": h, "kl_divergence": kl_val})

                self.logger.info(f"Hour {h} | FSS: {fss_val:.4f} | KL: {kl_val:.4f}")

            pd.DataFrame(fss_records).to_csv(self.base_dir / "metrics" / "fss_vs_pygnome.csv", index=False)
            pd.DataFrame(kl_records).to_csv(self.base_dir / "metrics" / "kl_vs_pygnome.csv", index=False)

        self.logger.info("Benchmark complete.")
        print(f"✅ Benchmark Complete! Outputs saved to: {self.base_dir}")
        return str(self.base_dir)
