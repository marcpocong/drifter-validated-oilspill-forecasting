"""
Shoreline beached-oil impact analysis.

Phase 4 prefers the canonical GSHHG-derived shoreline segments already stored in
the scoring-grid artifacts. The legacy ordered-point segmentation remains as a
fallback so the older Phase 3 path stays backward-compatible.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from src.helpers.metrics import haversine

logger = logging.getLogger(__name__)

DEFAULT_CANONICAL_SEGMENTS_PATH = Path("data_processed/grids/shoreline_segments.gpkg")


class ShorelineImpactService:
    """
    Analyze beached particle data from an OpenOil NetCDF and produce a
    per-segment impact table.

    When the canonical shoreline segments are available, each beached particle is
    attached to the nearest stored shoreline segment. If the artifact is missing
    or cannot be read, the service falls back to the legacy ordered-point
    segmentation to preserve the older workflow behavior.
    """

    def __init__(
        self,
        segment_length_km: float = 1.0,
        segment_prefix: str = "PWN",
        *,
        canonical_segments_path: str | Path | None = DEFAULT_CANONICAL_SEGMENTS_PATH,
        prefer_canonical_segments: bool = True,
    ):
        self.segment_length_km = segment_length_km
        self.segment_prefix = segment_prefix
        self.canonical_segments_path = Path(canonical_segments_path) if canonical_segments_path else None
        self.prefer_canonical_segments = bool(prefer_canonical_segments)

        from src.core.constants import BASE_OUTPUT_DIR

        self.output_dir = BASE_OUTPUT_DIR / "weathering"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze(
        self,
        nc_path: Path,
        initial_mass_tonnes: float,
    ) -> pd.DataFrame | None:
        """
        Extract beached particles from an OpenOil NetCDF and build a
        per-segment impact table.

        Parameters
        ----------
        nc_path : Path
            Path to the OpenOil output NetCDF.
        initial_mass_tonnes : float
            Total initial oil mass (M0) used in the simulation.

        Returns
        -------
        pd.DataFrame or None
            Returns None when no particles are beached.
        """
        beached = self._extract_beached_particles(nc_path, initial_mass_tonnes)
        if beached is None or beached.empty:
            logger.info("No beached particles in %s; skipping shoreline analysis.", nc_path.name)
            return None

        if self.prefer_canonical_segments and self.canonical_segments_path and self.canonical_segments_path.exists():
            try:
                return self._assign_canonical_segments(beached)
            except Exception as exc:
                logger.warning(
                    "Canonical shoreline assignment failed for %s: %s. Falling back to legacy segmentation.",
                    nc_path.name,
                    exc,
                )

        return self._assign_segments(beached)

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _extract_beached_particles(
        self,
        nc_path: Path,
        initial_mass_tonnes: float,
    ) -> pd.DataFrame | None:
        """
        Extract each particle's first beaching event (status == 1) together with
        its position, mass, and arrival time.
        """
        ds = xr.open_dataset(nc_path)
        status = ds["status"].values
        lat = ds["lat"].values
        lon = ds["lon"].values
        mass_oil = ds["mass_oil"].values
        time_values = pd.to_datetime(ds["time"].values, utc=True)
        ds.close()

        n_traj, _ = status.shape
        initial_mass_kg = initial_mass_tonnes * 1000.0
        mass_per_particle_kg = initial_mass_kg / max(n_traj, 1)

        records: list[dict[str, object]] = []
        start_time = time_values[0]
        for particle_index in range(n_traj):
            particle_status = status[particle_index, :]
            beached_mask = particle_status == 1
            if not np.any(beached_mask):
                continue

            first_index = int(np.argmax(beached_mask))
            particle_lat = float(lat[particle_index, first_index])
            particle_lon = float(lon[particle_index, first_index])
            particle_mass = float(mass_oil[particle_index, first_index])

            if np.isnan(particle_lat) or np.isnan(particle_lon):
                continue

            if np.isnan(particle_mass) or particle_mass <= 0:
                particle_mass = mass_per_particle_kg

            arrival_time = time_values[first_index]
            arrival_hour = float((arrival_time - start_time) / pd.Timedelta(hours=1))
            records.append(
                {
                    "particle_id": particle_index,
                    "lat": particle_lat,
                    "lon": particle_lon,
                    "arrival_hour": arrival_hour,
                    "arrival_time_utc": arrival_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "mass_kg": particle_mass,
                }
            )

        if not records:
            return None
        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Canonical shoreline assignment
    # ------------------------------------------------------------------

    def _assign_canonical_segments(self, beached: pd.DataFrame) -> pd.DataFrame:
        import geopandas as gpd

        if not self.canonical_segments_path or not self.canonical_segments_path.exists():
            raise FileNotFoundError("Canonical shoreline segment artifact is not available.")

        segments = gpd.read_file(self.canonical_segments_path)
        if segments.empty or "segment_id" not in segments.columns:
            raise ValueError("Canonical shoreline segment artifact is missing segment_id rows.")

        working_segments = segments[["segment_id", "length_m", "geometry"]].copy()
        point_frame = gpd.GeoDataFrame(
            beached.copy(),
            geometry=gpd.points_from_xy(beached["lon"], beached["lat"]),
            crs="EPSG:4326",
        ).to_crs(working_segments.crs)

        joined = gpd.sjoin_nearest(
            point_frame,
            working_segments,
            how="left",
            distance_col="distance_to_segment_m",
        )
        if joined.empty:
            return self._assign_segments(beached)

        midpoint_segments = working_segments.copy()
        midpoint_segments["geometry"] = midpoint_segments.geometry.interpolate(0.5, normalized=True)
        midpoint_segments = midpoint_segments.to_crs("EPSG:4326")
        midpoint_segments["segment_midpoint_lon"] = midpoint_segments.geometry.x
        midpoint_segments["segment_midpoint_lat"] = midpoint_segments.geometry.y
        midpoint_segments = midpoint_segments.drop(columns=["geometry"])

        grouped = (
            joined.groupby("segment_id")
            .agg(
                total_beached_kg=("mass_kg", "sum"),
                n_particles=("particle_id", "count"),
                first_arrival_h=("arrival_hour", "min"),
                arrival_p25_h=("arrival_hour", lambda values: float(np.percentile(values, 25))),
                arrival_p75_h=("arrival_hour", lambda values: float(np.percentile(values, 75))),
                mean_assignment_distance_m=("distance_to_segment_m", "mean"),
                max_assignment_distance_m=("distance_to_segment_m", "max"),
                beached_lat_mean=("lat", "mean"),
                beached_lon_mean=("lon", "mean"),
            )
            .reset_index()
        )
        first_arrivals = joined.groupby("segment_id")["arrival_time_utc"].min().reset_index()
        grouped = grouped.merge(first_arrivals, on="segment_id", how="left")
        grouped = grouped.merge(midpoint_segments, on="segment_id", how="left")
        grouped = grouped.rename(columns={"length_m": "segment_length_m", "arrival_time_utc": "first_arrival_utc"})
        grouped["segment_assignment_method"] = "canonical_shoreline_segments_gpkg"

        for column in (
            "total_beached_kg",
            "first_arrival_h",
            "arrival_p25_h",
            "arrival_p75_h",
            "mean_assignment_distance_m",
            "max_assignment_distance_m",
            "segment_length_m",
            "segment_midpoint_lat",
            "segment_midpoint_lon",
            "beached_lat_mean",
            "beached_lon_mean",
        ):
            if column in grouped.columns:
                grouped[column] = grouped[column].astype(float).round(3)

        return grouped.sort_values(["first_arrival_h", "segment_id"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Legacy fallback segmentation
    # ------------------------------------------------------------------

    def _assign_segments(self, beached: pd.DataFrame) -> pd.DataFrame:
        """
        Group beached particles into sequential approximate shoreline segments.

        This is the legacy fallback behavior used when the canonical shoreline
        segments are not available.
        """
        beached = beached.sort_values(["lon", "lat"]).reset_index(drop=True)

        segment_ids: list[int] = []
        current_segment = 0
        reference_lat = beached.loc[0, "lat"]
        reference_lon = beached.loc[0, "lon"]
        cumulative_km = 0.0

        for index, row in beached.iterrows():
            distance_km = haversine(reference_lat, reference_lon, row["lat"], row["lon"])
            cumulative_km += distance_km
            reference_lat, reference_lon = row["lat"], row["lon"]

            if cumulative_km > self.segment_length_km and index > 0:
                current_segment += 1
                cumulative_km = 0.0

            segment_ids.append(current_segment)

        beached = beached.copy()
        beached["_legacy_segment_id"] = segment_ids

        grouped = (
            beached.groupby("_legacy_segment_id")
            .agg(
                beached_lat_mean=("lat", "mean"),
                beached_lon_mean=("lon", "mean"),
                total_beached_kg=("mass_kg", "sum"),
                n_particles=("particle_id", "count"),
                first_arrival_h=("arrival_hour", "min"),
                arrival_p25_h=("arrival_hour", lambda values: float(np.percentile(values, 25))),
                arrival_p75_h=("arrival_hour", lambda values: float(np.percentile(values, 75))),
                first_arrival_utc=("arrival_time_utc", "min"),
            )
            .reset_index(drop=True)
        )
        grouped.insert(
            0,
            "segment_id",
            [f"{self.segment_prefix}-{index + 1:03d}" for index in range(len(grouped))],
        )
        grouped["segment_length_m"] = float(self.segment_length_km) * 1000.0
        grouped["segment_midpoint_lat"] = grouped["beached_lat_mean"]
        grouped["segment_midpoint_lon"] = grouped["beached_lon_mean"]
        grouped["mean_assignment_distance_m"] = np.nan
        grouped["max_assignment_distance_m"] = np.nan
        grouped["segment_assignment_method"] = "legacy_ordered_beached_points"

        for column in (
            "beached_lat_mean",
            "beached_lon_mean",
            "total_beached_kg",
            "first_arrival_h",
            "arrival_p25_h",
            "arrival_p75_h",
            "segment_length_m",
            "segment_midpoint_lat",
            "segment_midpoint_lon",
        ):
            grouped[column] = grouped[column].astype(float).round(3)

        return grouped


def run_shoreline_analysis(
    nc_path: str | Path,
    initial_mass_tonnes: float,
    segment_length_km: float = 1.0,
    segment_prefix: str = "PWN",
    *,
    canonical_segments_path: str | Path | None = DEFAULT_CANONICAL_SEGMENTS_PATH,
    prefer_canonical_segments: bool = True,
) -> pd.DataFrame | None:
    """
    Wrapper used by the existing CLI entrypoints.
    """
    service = ShorelineImpactService(
        segment_length_km=segment_length_km,
        segment_prefix=segment_prefix,
        canonical_segments_path=canonical_segments_path,
        prefer_canonical_segments=prefer_canonical_segments,
    )
    return service.analyze(Path(nc_path), initial_mass_tonnes)
