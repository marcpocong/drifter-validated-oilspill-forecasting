"""
Phase 3 (Supplementary): PyGNOME Cross-Comparison Service.

Runs one or two representative oil-type scenarios through PyGNOME to verify
that OpenDrift/OpenOil mass budgets remain consistent with NOAA's established
operational tool.

This module is designed to run inside the `gnome` Docker container where
the `gnome` package (PyGNOME) is installed.  It gracefully no-ops when
PyGNOME is not available (e.g., inside the `pipeline` container).
"""

import logging
import yaml
import pandas as pd
from datetime import timedelta
from pathlib import Path

from src.helpers.metrics import extract_gnome_budget_from_nc

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Guard: import PyGNOME – it is only available in the gnome container
# ---------------------------------------------------------------------------
try:
    from gnome.model import Model
    from gnome.movers import RandomMover, WindMover
    from gnome.outputters import NetCDFOutput
    from gnome.spills import surface_point_line_spill
    from gnome.environment import Wind, Water, Waves
    from gnome.weatherers import Evaporation, NaturalDispersion
    try:
        from gnome.spills.gnome_oil import GnomeOil
    except ImportError:
        from gnome.spills import GnomeOil
    GNOME_AVAILABLE = True
except ImportError as _gnome_err:                      # noqa: F841
    logger.warning(
        f"PyGNOME not available in this environment: {_gnome_err}. "
        "GnomeComparisonService will be a no-op. "
        "Run this service inside the `gnome` Docker container."
    )
    GNOME_AVAILABLE = False


class GnomeComparisonService:
    """
    Supplementary cross-model comparison using PyGNOME.

    Validates that OpenDrift/OpenOil mass budgets are consistent with
    PyGNOME's operational estimate for the same scenarios.
    """

    def __init__(self):
        from src.core.constants import BASE_OUTPUT_DIR
        self.output_dir = BASE_OUTPUT_DIR / "gnome_comparison"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        config_path = Path("config/oil.yaml")
        if not config_path.exists():
            raise FileNotFoundError(f"Phase 3 config required at {config_path}")

        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.oils_cfg = self.config["oils"]
        self.sim_cfg = self.config["simulation"]
        self.gnome_cfg = self.config.get("gnome_comparison", {})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_comparison(
        self,
        start_lat: float,
        start_lon: float,
        start_time: str,
        openoil_results: dict,
    ) -> dict:
        """
        Run PyGNOME scenarios and compare budgets with OpenOil results.

        Parameters
        ----------
        start_lat, start_lon : float
        start_time : str
        openoil_results : dict
            Result dict from OilWeatheringService.run_all() – used for the
            overlay comparison chart.

        Returns
        -------
        dict  keyed by oil_key with gnome budget DataFrames + comparison charts.
        """
        if not GNOME_AVAILABLE:
            logger.warning("Skipping PyGNOME comparison: package not installed.")
            return {}

        if not self.gnome_cfg.get("enabled", True):
            logger.info("PyGNOME comparison disabled in config.")
            return {}

        scenarios = self.gnome_cfg.get("scenarios", list(self.oils_cfg.keys()))
        gnome_results = {}

        for oil_key in scenarios:
            oil_cfg = self.oils_cfg.get(oil_key)
            if not oil_cfg:
                continue

            print(f"\n🔱  GNOME comparison | {oil_cfg['display_name']}")
            try:
                budget_df, nc_path = self._run_gnome_scenario(
                    oil_key=oil_key,
                    oil_cfg=oil_cfg,
                    start_lat=start_lat,
                    start_lon=start_lon,
                    start_time=start_time,
                )
                gnome_results[oil_key] = {
                    "display_name": oil_cfg["display_name"],
                    "budget_df": budget_df,
                    "nc_path": nc_path,
                }
            except Exception as e:
                logger.error(f"PyGNOME run failed for {oil_key}: {e}", exc_info=True)
                print(f"   ❌ PyGNOME run failed for {oil_key}: {e}")
                continue


        # Generate overlay comparison charts
        if gnome_results and openoil_results:
            self._plot_overlay_comparison(gnome_results, openoil_results)

        return gnome_results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_gnome_scenario(
        self,
        oil_key: str,
        oil_cfg: dict,
        start_lat: float,
        start_lon: float,
        start_time: str,
    ):
        """Construct and execute a minimal PyGNOME model run."""
        if not GNOME_AVAILABLE:
            return pd.DataFrame(), Path("not_found.nc")

        duration_hours = self.gnome_cfg.get(
            "duration_hours", self.sim_cfg["duration_hours"]
        )
        time_step_minutes = self.gnome_cfg.get(
            "time_step_minutes", self.sim_cfg["time_step_minutes"]
        )
        t_start = pd.to_datetime(start_time).to_pydatetime()

        # ── Model ──────────────────────────────────────────────────────
        model = Model(
            start_time=t_start,
            duration=timedelta(hours=duration_hours),
            time_step=timedelta(minutes=time_step_minutes),
        )

        # ── Environment ────────────────────────────────────────────────
        # Constant approximation for cross-check: use representative
        # wind speed (5 m/s) and water temperature (28 °C = 301.15 K)
        wind = Wind(
            timeseries=[(t_start, (5.0, 0.0))],
            units="m/s",
        )
        water = Water(temperature=301.15, salinity=33.0) # 28C in Kelvin
        waves = Waves(wind=wind, water=water)

        model.environment += [wind, water, waves]

        # ── Spill ──────────────────────────────────────────────────────
        # Use specific PyGNOME oil type if available, otherwise try the generic ID
        gnome_oil_type = oil_cfg.get("gnome_oil_type", oil_cfg["adios_id"])
        oil = GnomeOil(gnome_oil_type)
        # For the PyGNOME baseline cross-check, cap at 5,000 particles to prevent
        # indefinite hanging during synchronous single-threaded rendering, while OpenOil
        # utilizes the full 100,000 array.
        gnome_particles = min(self.sim_cfg["num_particles"], 5000)

        # Attempt to use Layer 3 authoritative polygon seeding via dispersed multi-point spills
        from src.utils.io import resolve_polygon_seeding
        lons, lats, _ = resolve_polygon_seeding(gnome_particles)
        logger.info(f"Seeding {gnome_particles} Gnome particles across Layer 3 polygon.")
        
        # Since PyGNOME's default is a point release, we approximate the polygon
        # by instantiating N distinct spills scattered across the polygon geometry.
        # To preserve performance tree, we cluster into max 100 focal points.
        num_clusters = min(gnome_particles, 100)
        particles_per_cluster = gnome_particles // num_clusters
        mass_per_cluster = self.sim_cfg["initial_mass_tonnes"] / num_clusters
        
        cluster_lons, cluster_lats, _ = resolve_polygon_seeding(num_clusters)
        for cl_lon, cl_lat in zip(cluster_lons, cluster_lats):
            model.spills += surface_point_line_spill(
                num_elements=particles_per_cluster,
                start_position=(cl_lon, cl_lat, 0.0),
                release_time=t_start,
                amount=mass_per_cluster,
                units="tonnes",
                substance=oil,
            )

        # ── Movers ────────────────────────────────────────────────────
        model.movers += RandomMover(diffusion_coef=10000)  # cm2/s
        model.movers += WindMover(wind)

        # ── Weatherers ────────────────────────────────────────────────
        model.weatherers += Evaporation(water=water, wind=wind)
        model.weatherers += NaturalDispersion(waves=waves, water=water)

        # ── Output ────────────────────────────────────────────────────
        nc_path = self.output_dir / f"gnome_{oil_key}.nc"
        if nc_path.exists():
            nc_path.unlink()

        nc_outputter = NetCDFOutput(
            filename=str(nc_path),
            which_data="most",
            output_timestep=timedelta(hours=1),
        )
        model.outputters += nc_outputter

        # ── Run ───────────────────────────────────────────────────────
        model.full_run()
        print(f"   ✅ GNOME run complete → {nc_path.name}")

        # ── Extract budget from NetCDF Output ─────────────────────────
        # Use NetCDF file because model.status_counts is deprecated
        budget_df = extract_gnome_budget_from_nc(nc_path)
        csv_path = self.output_dir / f"gnome_budget_{oil_key}.csv"
        budget_df.to_csv(csv_path, index=False)
        print(f"   📊 GNOME budget saved → {csv_path.name}")

        return budget_df, nc_path

    def _plot_overlay_comparison(
        self, gnome_results: dict, openoil_results: dict
    ):
        """Render overlay charts comparing GNOME vs OpenOil budgets."""
        from src.helpers.plotting import plot_gnome_vs_openoil

        for oil_key in gnome_results:
            if oil_key not in openoil_results:
                continue

            chart_path = self.output_dir / f"gnome_vs_openoil_{oil_key}.png"
            display_name = gnome_results[oil_key]["display_name"]

            plot_gnome_vs_openoil(
                openoil_df=openoil_results[oil_key]["budget_df"],
                gnome_df=gnome_results[oil_key]["budget_df"],
                output_file=str(chart_path),
                title=f"Phase 3 – Cross-Model Comparison: {display_name}",
            )
            print(f"   📊 GNOME vs OpenOil chart → {chart_path.name}")


# ---------------------------------------------------------------------------
# Convenience wrapper
# ---------------------------------------------------------------------------

def run_gnome_comparison(
    start_lat: float,
    start_lon: float,
    start_time: str,
    openoil_results: dict,
) -> dict:
    """Entry-point for gnome container. Safe to call from pipeline (no-op if GNOME absent)."""
    service = GnomeComparisonService()
    return service.run_comparison(
        start_lat=start_lat,
        start_lon=start_lon,
        start_time=start_time,
        openoil_results=openoil_results,
    )
