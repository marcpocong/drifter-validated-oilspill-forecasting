"""
Phase 3: Shoreline Beached-Oil Impact Analysis.

Segments the coastline into ~1 km sections and computes, for each segment:
  - Total beached oil mass (kg)
  - Number of beached particles
  - Arrival-time inter-quartile range (25th–75th percentile)
  - Earliest arrival hour

Segment IDs follow the format "<prefix>-001", "<prefix>-002", etc.
(e.g. "PWN-001" for the Palawan/North region).
"""

import logging
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path

from src.helpers.metrics import haversine

logger = logging.getLogger(__name__)

class ShorelineImpactService:
    """
    Analyse beached particle data from OpenOil NetCDF output and produce
    a per-segment impact table as described in §3.6.7.
    """

    def __init__(self, segment_length_km: float = 1.0, segment_prefix: str = "PWN"):
        self.segment_length_km = segment_length_km
        self.segment_prefix = segment_prefix
        
        from src.core.constants import BASE_OUTPUT_DIR
        self.output_dir = BASE_OUTPUT_DIR / "weathering"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze(
        self, nc_path: Path, initial_mass_tonnes: float
    ) -> pd.DataFrame | None:
        """
        Extract beached particles from an OpenOil NetCDF and build a
        per-segment impact table.

        Parameters
        ----------
        nc_path : Path
            Path to the OpenOil output NetCDF.
        initial_mass_tonnes : float
            Total initial oil mass (M₀) used in the simulation.

        Returns
        -------
        pd.DataFrame or None
            Columns: segment_id, lat_centre, lon_centre, total_beached_kg,
                     n_particles, first_arrival_h, arrival_p25_h, arrival_p75_h.
            Returns None when no particles are beached.
        """
        beached = self._extract_beached_particles(nc_path, initial_mass_tonnes)
        if beached is None or beached.empty:
            logger.info(
                f"No beached particles in {nc_path.name} – "
                "skipping shoreline analysis."
            )
            return None

        segments = self._assign_segments(beached)
        return segments

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _extract_beached_particles(
        self, nc_path: Path, initial_mass_tonnes: float
    ) -> pd.DataFrame | None:
        """
        From the OpenOil NetCDF, extract each particle's *first* beaching
        event (status == 1) together with its position, mass, and arrival hour.

        Returns
        -------
        pd.DataFrame or None
            Columns: particle_id, lat, lon, arrival_hour, mass_kg
        """
        ds = xr.open_dataset(nc_path)

        status = ds["status"].values        # shape: (n_traj, n_time)
        lat = ds["lat"].values
        lon = ds["lon"].values
        mass_oil = ds["mass_oil"].values
        ds.close()

        n_traj, n_time = status.shape
        initial_mass_kg = initial_mass_tonnes * 1000.0
        mass_per_particle_kg = initial_mass_kg / n_traj

        records = []
        for p in range(n_traj):
            p_status = status[p, :]
            beached_mask = p_status == 1
            if not np.any(beached_mask):
                continue

            # First timestep at which the particle was stranded
            first_t = int(np.argmax(beached_mask))
            p_lat = float(lat[p, first_t])
            p_lon = float(lon[p, first_t])
            p_mass = float(mass_oil[p, first_t])

            if np.isnan(p_lat) or np.isnan(p_lon):
                continue

            # Use mass at beaching time; fall back to uniform per-particle mass
            if np.isnan(p_mass) or p_mass <= 0:
                p_mass = mass_per_particle_kg

            records.append(
                {
                    "particle_id": p,
                    "lat": p_lat,
                    "lon": p_lon,
                    "arrival_hour": first_t,
                    "mass_kg": p_mass,
                }
            )

        if not records:
            return None
        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Segmentation
    # ------------------------------------------------------------------

    def _assign_segments(self, beached: pd.DataFrame) -> pd.DataFrame:
        """
        Group beached particles into sequential ~1 km coastal segments.

        Algorithm
        ---------
        1. Sort beached particles by longitude then latitude (approximates
           west → east coastal order, suitable for the study area).
        2. Walk through the sorted list; accumulate along-coast haversine
           distance and start a new segment each time the running total
           exceeds ``segment_length_km``.
        3. Aggregate mass and arrival-time statistics per segment.
        """
        beached = beached.sort_values(["lon", "lat"]).reset_index(drop=True)

        seg_ids: list[int] = []
        current_seg = 0
        ref_lat = beached.loc[0, "lat"]
        ref_lon = beached.loc[0, "lon"]
        cumulative_km = 0.0

        for idx, row in beached.iterrows():
            d = haversine(ref_lat, ref_lon, row["lat"], row["lon"])
            cumulative_km += d
            ref_lat, ref_lon = row["lat"], row["lon"]

            if cumulative_km > self.segment_length_km and idx > 0:
                current_seg += 1
                cumulative_km = 0.0

            seg_ids.append(current_seg)

        beached = beached.copy()
        beached["_seg"] = seg_ids

        # Aggregate per segment
        agg = (
            beached.groupby("_seg")
            .agg(
                lat_centre=("lat", "mean"),
                lon_centre=("lon", "mean"),
                total_beached_kg=("mass_kg", "sum"),
                n_particles=("particle_id", "count"),
                first_arrival_h=("arrival_hour", "min"),
                arrival_p25_h=(
                    "arrival_hour",
                    lambda x: float(np.percentile(x, 25)),
                ),
                arrival_p75_h=(
                    "arrival_hour",
                    lambda x: float(np.percentile(x, 75)),
                ),
            )
            .reset_index(drop=True)
        )

        # Human-readable segment IDs: PWN-001, PWN-002, …
        agg.insert(
            0,
            "segment_id",
            [f"{self.segment_prefix}-{i + 1:03d}" for i in range(len(agg))],
        )

        # Round for readability
        for col in ("lat_centre", "lon_centre"):
            agg[col] = agg[col].round(4)
        agg["total_beached_kg"] = agg["total_beached_kg"].round(1)
        for col in ("first_arrival_h", "arrival_p25_h", "arrival_p75_h"):
            agg[col] = agg[col].round(1)

        return agg


# ---------------------------------------------------------------------------
# Convenience wrapper
# ---------------------------------------------------------------------------

def run_shoreline_analysis(
    nc_path: str | Path,
    initial_mass_tonnes: float,
    segment_length_km: float = 1.0,
    segment_prefix: str = "PWN",
) -> pd.DataFrame | None:
    """
    Entry-point wrapper called from ``__main__.py``.

    Parameters
    ----------
    nc_path : str or Path
        Path to the OpenOil output NetCDF.
    initial_mass_tonnes : float
        Total initial oil mass M₀ (tonnes).
    segment_length_km : float
        Target length of each coastal segment (km).
    segment_prefix : str
        Prefix for segment IDs (e.g. "PWN").

    Returns
    -------
    pd.DataFrame or None
    """
    service = ShorelineImpactService(
        segment_length_km=segment_length_km,
        segment_prefix=segment_prefix,
    )
    return service.analyze(Path(nc_path), initial_mass_tonnes)
