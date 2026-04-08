"""
Rasterization helpers for PyGNOME and OpenDrift outputs.
Builds a common grid and converts netcdf outputs to .tif.
"""

from __future__ import annotations

import numpy as np
import xarray as xr
import pandas as pd
import json
from pathlib import Path

from src.helpers.scoring import get_scoring_grid_spec, ScoringGridSpec

try:
    import rasterio
    from rasterio.transform import from_origin
    from rasterio.features import rasterize as rio_rasterize
except ImportError:
    rasterio = None
    rio_rasterize = None

try:
    import geopandas as gpd
except ImportError:
    gpd = None


class GridBuilder:
    """
    Creates the canonical common raster grid used by benchmark/validation outputs.
    """
    def __init__(self, region: list[float] = None, resolution: float = None):
        spec = get_scoring_grid_spec()

        if region is not None or resolution is not None:
            _region = spec.region if region is None else region
            _resolution = spec.resolution if resolution is None else resolution
            spec = type(spec)(
                min_lon=_region[0],
                max_lon=_region[1],
                min_lat=_region[2],
                max_lat=_region[3],
                resolution=_resolution,
                crs=spec.crs,
            )

        self.spec = spec
        self.min_lon = spec.min_lon
        self.max_lon = spec.max_lon
        self.min_lat = spec.min_lat
        self.max_lat = spec.max_lat
        self.resolution = spec.resolution
        self.width = spec.width
        self.height = spec.height

        # Transform for saving to TIFF
        self.transform = from_origin(self.min_lon, self.max_lat, self.resolution, self.resolution)

        # Generate cell edges for numpy histogramming
        self.lon_bins = spec.lon_bins
        self.lat_bins = spec.lat_bins
        self.crs = spec.crs

    def save_metadata(self, out_path: Path):
        meta = self.spec.to_metadata()
        with open(out_path, "w") as f:
            json.dump(meta, f, indent=4)
        return meta


def rasterize_polygon_gdf(gdf, grid: GridBuilder, burn_value: float = 1.0) -> np.ndarray:
    if rasterio is None or rio_rasterize is None:
        raise ImportError("rasterio is required to rasterize polygons")
    if gdf is None or gdf.empty:
        return np.zeros((grid.height, grid.width), dtype=np.float32)

    shapes = ((geom, burn_value) for geom in gdf.geometry if geom is not None and not geom.is_empty)
    arr = rio_rasterize(
        shapes,
        out_shape=(grid.height, grid.width),
        transform=grid.transform,
        fill=0,
        dtype=np.float32,
        all_touched=True,
    )
    return np.flipud(arr)


def save_mask_raster(grid: GridBuilder, data: np.ndarray, out_path: Path):
    if rasterio is None:
        raise ImportError("rasterio is required to save TIFFs")

    with rasterio.open(
        out_path,
        'w',
        driver='GTiff',
        height=grid.height,
        width=grid.width,
        count=1,
        dtype=data.dtype,
        crs=grid.crs,
        transform=grid.transform,
        compress='lzw'
    ) as dst:
        dst.write(data, 1)


def rasterize_observation_layer(gdf, grid: GridBuilder, out_path: Path | None = None):
    mask = rasterize_polygon_gdf(gdf, grid)
    if out_path is not None:
        save_mask_raster(grid, mask, out_path)
    return mask


def extract_particles_at_hour(nc_path: Path, target_hours: int, model_type: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Extracts active particle lons, lats, and mass at a specific target hour.
    """
    if model_type == 'opendrift':
        with xr.open_dataset(nc_path) as ds:
            times = pd.to_datetime(ds.time.values)
            start_time = times[0]
            target_time = start_time + pd.Timedelta(hours=target_hours)
            
            # Find closest time index
            diffs = np.abs(times - target_time)
            t_idx = np.argmin(diffs)
            
            # OpenDrift shapes: (traj, time)
            lon = ds['lon'].values[:, t_idx]
            lat = ds['lat'].values[:, t_idx]
            status = ds['status'].values[:, t_idx]
            mass = ds['mass_oil'].values[:, t_idx] if 'mass_oil' in ds else np.ones_like(lon)
            
            # Status 0 is active, filter out NaNs
            valid = (status == 0) & ~np.isnan(lon) & ~np.isnan(lat)
            return lon[valid], lat[valid], mass[valid]
            
    elif model_type == 'pygnome':
        import netCDF4
        with netCDF4.Dataset(nc_path) as nc:
            raw_times = netCDF4.num2date(
                nc.variables['time'][:], 
                nc.variables['time'].units,
                only_use_cftime_datetimes=False,
                only_use_python_datetimes=True
            )
            times = pd.to_datetime(raw_times)
            t_idx = np.argmin(np.abs(times - (times[0] + pd.Timedelta(hours=target_hours))))
            
            # PyGNOME flatten arrays, use particle_count
            particle_counts = nc.variables['particle_count'][:]
            start_idx = int(np.sum(particle_counts[:t_idx]))
            end_idx = start_idx + int(particle_counts[t_idx])
            
            lon = nc.variables['longitude'][start_idx:end_idx]
            lat = nc.variables['latitude'][start_idx:end_idx]
            status = nc.variables['status_codes'][start_idx:end_idx]
            mass = nc.variables['mass'][start_idx:end_idx] if 'mass' in nc.variables else np.ones_like(lon)
            
            # In water (2)
            valid = (status == 2)
            return lon[valid], lat[valid], mass[valid]

    else:
        raise ValueError(f"Unknown model_type: {model_type}")


def rasterize_particles(grid: GridBuilder, lon: np.ndarray, lat: np.ndarray, mass: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Converts scattered particles into binary hits and density probability matrices.
    """
    # Create 2D histogram of particle counts
    counts, _, _ = np.histogram2d(lat, lon, bins=[grid.lat_bins, grid.lon_bins])
    # Reverse counts along lat axis to match image coordinates (top-left is max_lat, min_lon)
    counts = np.flipud(counts)
    
    # Binary hits (1 if count > 0, else 0)
    hits = (counts > 0).astype(np.float32)
    
    # 2D histogram of mass
    mass_grid, _, _ = np.histogram2d(lat, lon, bins=[grid.lat_bins, grid.lon_bins], weights=mass)
    mass_grid = np.flipud(mass_grid)
    
    # Density / Probability (mass per cell / total mass)
    total_mass = np.sum(mass_grid)
    probs = (mass_grid / total_mass).astype(np.float32) if total_mass > 0 else mass_grid.astype(np.float32)
    
    return hits, probs


def save_raster(grid: GridBuilder, data: np.ndarray, out_path: Path):
    """
    Saves a 2D numpy array to a GeoTIFF using rasterio.
    """
    if rasterio is None:
        raise ImportError("rasterio is required to save TIFFs")
        
    with rasterio.open(
        out_path,
        'w',
        driver='GTiff',
        height=grid.height,
        width=grid.width,
        count=1,
        dtype=data.dtype,
        crs=grid.crs,
        transform=grid.transform,
        compress='lzw'
    ) as dst:
        dst.write(data, 1)


def rasterize_model_output(grid: GridBuilder, nc_path: Path, model_type: str, out_dir: Path, hours: list[int] = [24, 48, 72]):
    """
    Extracts particles for given hours, rasterizes them to hits and probs, and saves as GeoTIFFs.
    Returns dictionary with paths to the generated tiffs.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    results = {}
    
    for h in hours:
        lon, lat, mass = extract_particles_at_hour(nc_path, h, model_type)
        hits, probs = rasterize_particles(grid, lon, lat, mass)
        
        hits_path = out_dir / f"hits_{h}.tif"
        probs_path = out_dir / f"p_{h}.tif"
        
        save_raster(grid, hits, hits_path)
        save_raster(grid, probs, probs_path)
        
        results[h] = {
            "hits": hits_path,
            "probs": probs_path,
            "hits_data": hits,
            "probs_data": probs
        }
        
    return results
