"""
Ensemble forecasting module for uncertainty quantification.
Executes Phase 2: 50-member Monte Carlo ensemble with perturbations.
"""

import json
import logging
import shutil
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
import yaml
from opendrift.models.oceandrift import OceanDrift
from opendrift.readers import reader_constant, reader_netCDF_CF_generic

from src.core.case_context import get_case_context
from src.core.constants import BASE_OUTPUT_DIR, RUN_NAME
from src.helpers.scoring import get_scoring_grid_spec
from src.helpers.plotting import plot_probability_map
from src.helpers.raster import GridBuilder, project_points_to_grid, rasterize_model_output, save_raster
from src.utils.io import (
    RecipeSelection,
    get_deterministic_control_output_path,
    get_deterministic_control_score_raster_dir,
    get_deterministic_control_score_raster_path,
    get_ensemble_probability_score_raster_path,
    get_forcing_files,
    get_forecast_manifest_path,
    resolve_spill_origin,
)

logger = logging.getLogger(__name__)


class EnsembleForecastService:
    def __init__(self, currents_file, winds_file):
        self.output_dir = BASE_OUTPUT_DIR / "ensemble"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.forecast_dir = BASE_OUTPUT_DIR / "forecast"
        self.forecast_dir.mkdir(parents=True, exist_ok=True)

        self.currents_file = currents_file
        self.winds_file = winds_file
        self.alias_probability_cone = True

        self.config_path = Path("config/ensemble.yaml")
        if not self.config_path.exists():
            raise FileNotFoundError(f"Ensemble configuration required at {self.config_path}")

        with open(self.config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.ensemble_size = self.config["ensemble"]["ensemble_size"]

    def _build_model(self) -> OceanDrift:
        """Create a standard OceanDrift model with the configured readers."""
        o = OceanDrift(loglevel=50)

        reader_curr = reader_netCDF_CF_generic.Reader(self.currents_file)
        o.add_reader(reader_curr)

        reader_wind = reader_netCDF_CF_generic.Reader(self.winds_file)
        o.add_reader(reader_wind)

        try:
            reader_wave = reader_netCDF_CF_generic.Reader(f"data/forcing/{RUN_NAME}/cmems_wave.nc")
            o.add_reader(reader_wave)
        except Exception:
            pass

        o.set_config("general:use_auto_landmask", False)
        o.add_reader(reader_constant.Reader({"land_binary_mask": 0}))
        return o

    @staticmethod
    def _seed_polygon_release(model: OceanDrift, start_time: str | pd.Timestamp, num_elements: int = 2000):
        """Seed particles across the configured initialization polygon."""
        from src.utils.io import resolve_polygon_seeding

        lons, lats, _ = resolve_polygon_seeding(num_elements)
        model.seed_elements(
            lon=lons,
            lat=lats,
            number=num_elements,
            time=pd.to_datetime(start_time),
        )

    def run_deterministic_control(
        self,
        recipe_name: str,
        start_time: str,
        duration_hours: int = 72,
    ) -> Path:
        """Run a single deterministic spill forecast for official Phase 3B scoring."""
        logger.info("Starting deterministic control forecast for recipe %s", recipe_name)
        output_file = get_deterministic_control_output_path(recipe_name)
        if output_file.exists():
            output_file.unlink()

        model = self._build_model()
        model.set_config("drift:horizontal_diffusivity", 0.0)
        model.set_config("drift:wind_uncertainty", 0.0)
        model.set_config("drift:current_uncertainty", 0.0)
        self._seed_polygon_release(model, start_time, num_elements=2000)
        model.run(
            duration=timedelta(hours=duration_hours),
            time_step=timedelta(minutes=60),
            outfile=str(output_file),
        )
        rasterize_model_output(
            grid=GridBuilder(),
            nc_path=output_file,
            model_type="opendrift",
            out_dir=get_deterministic_control_score_raster_dir(recipe_name),
            hours=[24, 48, 72],
        )
        logger.info("Deterministic control forecast saved to %s", output_file)
        return output_file

    def run_ensemble(self, start_lat: float, start_lon: float, start_time: str, duration_hours: int = 72):
        """
        Runs a 50-member ensemble to generate snapshot probability products.
        Perturbs: Start Time, Wind Factor, and Diffusion.
        """
        logger.info("Starting Phase 2: Ensemble Forecast (%s members)...", self.ensemble_size)
        logger.info("Spill Location: %s, %s", start_lat, start_lon)
        logger.info("Nominal Start Time: %s", start_time)
        logger.info("Currents: %s", self.currents_file)
        logger.info("Winds: %s", self.winds_file)

        ensemble_files = []
        base_time = pd.to_datetime(start_time)
        rng = np.random.default_rng()
        p_cfg = self.config["perturbations"]

        for i in range(self.ensemble_size):
            member_id = i + 1

            t_shift = p_cfg["time_shift_hours"]
            time_offset_hours = rng.uniform(-t_shift, t_shift)
            run_start_time = base_time + timedelta(hours=time_offset_hours)

            diffusivity = rng.uniform(p_cfg["diffusivity_min"], p_cfg["diffusivity_max"])
            wind_uncertainty = rng.uniform(
                p_cfg["wind_uncertainty_min"],
                p_cfg["wind_uncertainty_max"],
            )

            print(
                f"   Member {member_id}/{self.ensemble_size}: "
                f"T{time_offset_hours:+.1f}h | K={diffusivity:.3f} | W_unc={wind_uncertainty:.1f}"
            )

            try:
                o = self._build_model()
            except Exception as e:
                logger.error("Error loading readers for member %s: %s", member_id, e)
                continue

            o.set_config("drift:horizontal_diffusivity", diffusivity)
            o.set_config("drift:wind_uncertainty", wind_uncertainty)
            o.set_config("drift:current_uncertainty", 0.1)
            self._seed_polygon_release(o, run_start_time, num_elements=2000)

            output_file = self.output_dir / f"member_{member_id:02d}.nc"
            if output_file.exists():
                output_file.unlink()

            o.run(
                duration=timedelta(hours=duration_hours),
                time_step=timedelta(minutes=60),
                outfile=str(output_file),
            )
            ensemble_files.append(output_file)

        logger.info("Ensemble runs complete. Generating probability products...")
        probability_outputs = self.generate_probability_products(ensemble_files, start_lat, start_lon)
        manifest = self.write_output_manifest(ensemble_files, probability_outputs)
        return manifest

    def generate_probability_products(self, file_list, start_lat, start_lon):
        """
        Generate gridded NetCDF probability fields and PNG snapshots for 24h, 48h, and 72h.
        """
        snapshots = [24, 48, 72]
        case = get_case_context()
        grid = GridBuilder()
        written_files: list[Path] = []

        metadata = {
            "ensemble_size": self.ensemble_size,
            "grid": grid.spec.to_metadata(),
            "snapshots_hours": snapshots,
            "variables": ["probability"],
        }

        metadata_path = self.output_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)
        written_files.append(metadata_path)

        for hr in snapshots:
            logger.info("   Processing T+%sh snapshot...", hr)
            all_lons = []
            all_lats = []

            for file_path in file_list:
                try:
                    with xr.open_dataset(file_path) as ds:
                        target_time = pd.to_datetime(ds.time.values[0]) + timedelta(hours=hr)
                        idx = np.abs(pd.to_datetime(ds.time.values) - target_time).argmin()

                        lons = ds.lon.values[idx, :]
                        lats = ds.lat.values[idx, :]

                        valid = ~np.isnan(lons) & ~np.isnan(lats)
                        all_lons.extend(lons[valid])
                        all_lats.extend(lats[valid])
                except Exception as e:
                    logger.warning("Could not process %s for %sh: %s", Path(file_path).name, hr, e)

            if not all_lons:
                raise RuntimeError(
                    f"Ensemble probability snapshot T+{hr}h could not be generated because "
                    "no valid particle positions were found."
                )

            x_vals, y_vals = project_points_to_grid(grid, np.asarray(all_lons), np.asarray(all_lats))
            hist, _, _ = np.histogram2d(y_vals, x_vals, bins=[grid.y_bins, grid.x_bins])
            hist = np.flipud(hist)
            prob_density = (hist / len(all_lons)).astype(np.float32)

            dims = ["time", grid.y_name, grid.x_name]
            coords = {
                "time": [hr],
                grid.y_name: grid.y_centers,
                grid.x_name: grid.x_centers,
            }
            attrs = {
                "description": f"Probability field at T+{hr}h",
                "units": "decimal_fraction",
                "crs": grid.crs,
                "resolution": grid.resolution,
                "grid_metadata_path": str(grid.spec.metadata_path or ""),
                "display_bounds_wgs84": json.dumps(grid.display_bounds_wgs84 or case.region),
            }

            ds_prob = xr.Dataset(
                data_vars={"probability": (dims, prob_density[np.newaxis, :, :])},
                coords=coords,
                attrs=attrs,
            )

            nc_out = self.output_dir / f"probability_{hr}h.nc"
            ds_prob.to_netcdf(nc_out)
            written_files.append(nc_out)

            tif_out = get_ensemble_probability_score_raster_path(hr)
            save_raster(grid, prob_density, tif_out)
            written_files.append(tif_out)

            img_out = self.output_dir / f"probability_{hr}h.png"
            plot_probability_map(
                output_file=str(img_out),
                all_lons=np.array(all_lons),
                all_lats=np.array(all_lats),
                start_lon=start_lon,
                start_lat=start_lat,
                corners=grid.display_bounds_wgs84 or case.region,
                title=f"Ensemble Forecast: T+{hr}h\nProbability Distribution (N={self.ensemble_size})",
            )
            written_files.append(img_out)

            if self.alias_probability_cone and hr == 72 and img_out.exists():
                alias_path = self.output_dir / "probability_cone.png"
                shutil.copyfile(img_out, alias_path)
                written_files.append(alias_path)

        logger.info("All Phase 2 probability products saved to %s", self.output_dir)
        return written_files

    def write_output_manifest(self, ensemble_files, probability_outputs):
        """Write a compact manifest listing the actual Phase 2 artifacts written."""
        written_paths: list[Path] = []
        for path in list(ensemble_files) + list(probability_outputs):
            path_obj = Path(path)
            if path_obj.exists():
                written_paths.append(path_obj)

        manifest_records = [
            {
                "file_name": path.name,
                "relative_path": str(path.relative_to(self.output_dir.parent)),
                "category": self._classify_output(path),
            }
            for path in written_paths
        ]

        manifest_path = self.output_dir / "ensemble_manifest.json"
        manifest_records.append(
            {
                "file_name": manifest_path.name,
                "relative_path": str(manifest_path.relative_to(self.output_dir.parent)),
                "category": "manifest",
            }
        )
        with open(manifest_path, "w") as f:
            json.dump({"written_files": manifest_records}, f, indent=2)

        logger.info("Wrote ensemble manifest to %s", manifest_path)
        return {
            "manifest": str(manifest_path),
            "written_files": [str(path) for path in written_paths] + [str(manifest_path)],
        }

    def write_official_forecast_manifest(
        self,
        selection: RecipeSelection,
        deterministic_control: Path,
        ensemble_manifest: dict,
        start_time: str,
    ) -> Path:
        """Write an official spill-forecast manifest for Phase 3B consumers."""
        from src.core.case_context import get_case_context

        case = get_case_context()
        manifest_path = get_forecast_manifest_path()
        payload = {
            "workflow_mode": case.workflow_mode,
            "case_id": case.case_id,
            "recipe_selection": {
                "recipe": selection.recipe,
                "source_kind": selection.source_kind,
                "source_path": selection.source_path,
                "status_flag": selection.status_flag,
                "valid": selection.valid,
                "provisional": selection.provisional,
                "rerun_required": selection.rerun_required,
                "note": selection.note,
            },
            "start_time": str(start_time),
            "artifacts": {
                "deterministic_control_nc": str(deterministic_control),
                "deterministic_control_hits_72h_tif": str(
                    get_deterministic_control_score_raster_path(selection.recipe, 72, raster_kind="hits")
                ),
                "ensemble_manifest": ensemble_manifest.get("manifest"),
                "ensemble_probability_72h_nc": str(self.output_dir / "probability_72h.nc"),
                "ensemble_probability_72h_tif": str(get_ensemble_probability_score_raster_path(72)),
            },
            "status_flags": {
                "valid": selection.valid,
                "provisional": selection.provisional,
                "rerun_required": selection.rerun_required,
            },
            "written_files": [
                str(deterministic_control),
                *ensemble_manifest.get("written_files", []),
            ],
        }
        with open(manifest_path, "w") as f:
            json.dump(payload, f, indent=2)
        logger.info("Wrote official forecast manifest to %s", manifest_path)
        return manifest_path

    @staticmethod
    def _classify_output(path: Path) -> str:
        if path.name == "probability_cone.png":
            return "legacy_alias"
        if path.name.startswith("member_") and path.suffix == ".nc":
            return "member_trace"
        if path.name.startswith("probability_") and path.suffix == ".tif":
            return "probability_tif"
        if path.name.startswith("probability_") and path.suffix == ".png":
            return "probability_png"
        if path.name.startswith("probability_") and path.suffix == ".nc":
            return "probability_netcdf"
        if path.name == "metadata.json":
            return "metadata"
        return "other"


def run_ensemble(best_recipe, start_time=None, start_lat=None, start_lon=None):
    """
    Wrapper to run ensemble with the winning recipe.
    """
    try:
        forcing = get_forcing_files(best_recipe)
        currents_file = str(forcing["currents"])
        winds_file = str(forcing["wind"])
    except Exception as e:
        logger.error("Invalid recipe '%s': %s", best_recipe, e)
        return {"status": "error", "message": str(e)}

    service = EnsembleForecastService(currents_file, winds_file)

    d_lat, d_lon, d_time = resolve_spill_origin()
    _start_lat = start_lat if start_lat is not None else d_lat
    _start_lon = start_lon if start_lon is not None else d_lon
    _start_time = start_time if start_time else d_time

    manifest = service.run_ensemble(
        start_lat=_start_lat,
        start_lon=_start_lon,
        start_time=_start_time,
    )
    return {
        "status": "success",
        "output": str(service.output_dir),
        "manifest": manifest["manifest"],
        "written_files": manifest["written_files"],
    }


def run_official_spill_forecast(
    selection: RecipeSelection,
    start_time: str | None = None,
    start_lat: float | None = None,
    start_lon: float | None = None,
):
    """Run the official deterministic control plus ensemble path for Phase 3B."""
    try:
        forcing = get_forcing_files(selection.recipe)
        currents_file = str(forcing["currents"])
        winds_file = str(forcing["wind"])
    except Exception as e:
        logger.error("Invalid recipe '%s': %s", selection.recipe, e)
        return {"status": "error", "message": str(e)}

    service = EnsembleForecastService(currents_file, winds_file)

    d_lat, d_lon, d_time = resolve_spill_origin()
    _start_lat = start_lat if start_lat is not None else d_lat
    _start_lon = start_lon if start_lon is not None else d_lon
    _start_time = start_time if start_time else d_time

    deterministic_control = service.run_deterministic_control(
        recipe_name=selection.recipe,
        start_time=_start_time,
    )
    ensemble_manifest = service.run_ensemble(
        start_lat=_start_lat,
        start_lon=_start_lon,
        start_time=_start_time,
    )
    forecast_manifest = service.write_official_forecast_manifest(
        selection=selection,
        deterministic_control=deterministic_control,
        ensemble_manifest=ensemble_manifest,
        start_time=_start_time,
    )
    return {
        "status": "success",
        "output": str(service.output_dir),
        "manifest": ensemble_manifest["manifest"],
        "forecast_manifest": str(forecast_manifest),
        "deterministic_control": str(deterministic_control),
        "written_files": [str(deterministic_control)] + ensemble_manifest["written_files"] + [str(forecast_manifest)],
    }
