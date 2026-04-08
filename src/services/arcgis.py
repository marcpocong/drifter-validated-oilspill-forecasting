"""
Generic ArcGIS FeatureServer ingestion helpers.

This module fetches feature layers from ArcGIS REST endpoints, converts them
into GeoDataFrames, and normalizes them to a requested CRS so they can be
rasterized against the shared scoring grid.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import logging

import requests

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - guarded at runtime
    gpd = None

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArcGISLayerRef:
    service_url: str
    layer_id: int
    name: str | None = None

    @property
    def query_url(self) -> str:
        return f"{self.service_url.rstrip('/')}/{self.layer_id}/query"


def get_configured_arcgis_layers() -> list[ArcGISLayerRef]:
    """Return the ArcGIS layer refs for the active workflow case."""
    from src.core.case_context import get_case_context

    case = get_case_context()
    return [
        ArcGISLayerRef(
            service_url=layer.service_url,
            layer_id=layer.layer_id,
            name=layer.local_name,
        )
        for layer in case.arcgis_layers
    ]


class ArcGISFeatureServerClient:
    def __init__(self, timeout: int = 60):
        if gpd is None:
            raise ImportError("geopandas is required for ArcGIS ingestion")
        self.timeout = timeout

    def fetch_layer(self, layer: ArcGISLayerRef, where: str = "1=1") -> gpd.GeoDataFrame:
        """Fetch all features from a FeatureServer layer."""
        params = {
            "where": where,
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
        }
        resp = requests.get(layer.query_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        payload = resp.json()
        features = payload.get("features", [])
        if not features:
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
        return gdf

    def fetch_layers(self, layer_refs: Iterable[ArcGISLayerRef]) -> dict[str, gpd.GeoDataFrame]:
        out = {}
        for ref in layer_refs:
            key = ref.name or f"layer_{ref.layer_id}"
            out[key] = self.fetch_layer(ref)
        return out

    @staticmethod
    def normalize_crs(gdf: gpd.GeoDataFrame, target_crs: str) -> gpd.GeoDataFrame:
        if gdf.empty:
            return gdf.set_crs(target_crs, allow_override=True)
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        return gdf.to_crs(target_crs)

    @staticmethod
    def to_geojson_file(gdf: gpd.GeoDataFrame, out_path: str | Path):
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(out_path, driver="GeoJSON")
        return out_path
