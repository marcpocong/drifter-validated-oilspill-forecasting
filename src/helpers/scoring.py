"""
Canonical scoring-grid helpers.

Keeps Phase 2/3 raster products on one shared grid without duplicating
region/resolution logic across services.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path

import numpy as np

from src.core.constants import REGION, GRID_RESOLUTION


@dataclass(frozen=True)
class ScoringGridSpec:
    min_lon: float
    max_lon: float
    min_lat: float
    max_lat: float
    resolution: float
    crs: str = "EPSG:4326"

    @property
    def width(self) -> int:
        return int(np.ceil((self.max_lon - self.min_lon) / self.resolution))

    @property
    def height(self) -> int:
        return int(np.ceil((self.max_lat - self.min_lat) / self.resolution))

    @property
    def region(self) -> list[float]:
        return [self.min_lon, self.max_lon, self.min_lat, self.max_lat]

    @property
    def lon_bins(self) -> np.ndarray:
        return np.linspace(self.min_lon, self.min_lon + self.width * self.resolution, self.width + 1)

    @property
    def lat_bins(self) -> np.ndarray:
        return np.linspace(self.min_lat, self.min_lat + self.height * self.resolution, self.height + 1)

    def to_metadata(self) -> dict:
        return {
            **asdict(self),
            "width": self.width,
            "height": self.height,
            "region": self.region,
        }

    def save_metadata(self, out_path: str | Path) -> dict:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        meta = self.to_metadata()
        with open(out_path, "w") as f:
            json.dump(meta, f, indent=4)
        return meta


def get_scoring_grid_spec() -> ScoringGridSpec:
    """Return the single canonical scoring grid used by benchmark/validation products."""
    min_lon, max_lon, min_lat, max_lat = REGION
    return ScoringGridSpec(
        min_lon=min_lon,
        max_lon=max_lon,
        min_lat=min_lat,
        max_lat=max_lat,
        resolution=GRID_RESOLUTION,
        crs="EPSG:32651",
    )
