# Drifter-Validated Oil Spill Forecasting System

A comprehensive thesis project for oil spill prediction and validation using particle tracking models (OpenDrift) validated against drifter observations using PyGNOME.

## Project Overview

This system implements a multi-phase approach to oil spill forecasting:

- **Phase 1**: Prototype transport validation using OpenDrift forcing recipes scored against drifter observations with NCS
- **Phase 2**: Deterministic control plus ensemble forecast generation. Prototype mode keeps legacy probability products; official Mindoro mode writes canonical projected raster products and machine-readable manifests
- **Phase 3A/3B**: Benchmark and public-observation validation tracks on the canonical scoring grid, plus a separate oil weathering and PyGNOME comparison lane

## Current Mindoro Progress

The repository now preserves the original `prototype_2016` workflow while adding the official `mindoro_retro_2023` workflow for the thesis-aligned Mindoro spill case.

Current official status:

- The official scoring grid is a real EPSG:32651 projected 1 km grid, with canonical template, extent, and GSHHG-derived shoreline/land/sea masks under `data_processed/grids/`.
- ArcGIS ingestion archives raw public service downloads, writes cleaned processed vectors, rasterizes accepted observation masks to the canonical grid, and records QA reports.
- Official Phase 2 forecast generation writes canonical products such as `control_footprint_mask_<timestamp>.tif`, `control_density_norm_<timestamp>.tif`, `prob_presence_<timestamp>.tif`, `mask_p50_<timestamp>.tif`, and date-composite masks with manifests and loading audits.
- Strict official Phase 3B remains locked to `mask_p50_2023-03-06_datecomposite.tif` vs `obs_mask_2023-03-06.tif`; this is now treated as a strict single-date stress test.
- A formal multi-date public-observation validation track promotes accepted dated, machine-readable, observation-derived public masks for March 4, March 5, and March 6. March 3 is initialization consistency only, not a normal forecast-skill date.
- A public-observation appendix inventory and extended-observation guardrail are in place. Accepted extended dates have been inventoried, but only the short tier March 7-9 has been model-extended and scored so far.
- The short extended appendix scoring completed with clean forcing coverage through March 9 plus offset buffer. March 7-9 p50 masks were empty, while the March 4-9 event corridor remained nonzero.
- The latest horizon-survival audit diagnosed the late-horizon loss as **Class C: shoreline/beaching/retention behavior**, not forcing truncation, scoring-grid mismatch, or a date-composite writer bug. All audited runs ended in `stranded_no_active`; the recommended next rerun is a transport/retention fix rerun before attempting the medium extended tier.

Important limitation:

- The official transport stack is still recorded as provisional where manifests report `provisional_transport_model=true`. Do not interpret current Phase 3B results as a final scientific improvement until the stranding/retention issue is addressed.

## Technology Stack

- **Language**: Python 3.10
- **Particle Tracking**: OpenDrift
- **Validation**: PyGNOME
- **Containerization**: Docker & Docker Compose
- **Dependency Management**: Poetry
- **Scientific Computing**: NumPy, SciPy, Pandas, Xarray
- **Visualization**: Matplotlib, Cartopy

## Getting Started

### Prerequisites

- Docker and Docker Compose installed
- Git
- 8GB RAM recommended for ensemble runs

### Installation

#### Option 1: Using Docker (Recommended)

This project uses a **Dual Container Strategy** to handle conflicting dependencies between data ingestion (Phase 1) and advanced modeling (Phase 3).

```bash
# Build and start both containers in the background
docker-compose up -d --build

# Prep: Download and preprocess shared inputs in the pipeline container
docker-compose exec -e PIPELINE_PHASE=prep pipeline python -m src

# Phase 1 + 2: Run Transport Validation & Ensemble Forecasting
docker-compose exec pipeline python -m src

# Phase 3: Run Oil Weathering & PyGNOME Comparison
docker-compose exec gnome python -m src
```

#### Option 2: Local Installation with Poetry

```bash
# Configure poetry to create .venv in project root
poetry config virtualenvs.in-project true

# Install dependencies
poetry install

# Activate virtual environment
poetry shell

# Run analysis
python -m src
```

### Project Structure & Directories

An important distinction in the architecture is the separation of data/ and output/:

*   **data/**: This directory acts as the **Input Data Lake**. It is strictly read-only after the initial automated download. It contains all the immutable, deterministic environmental forcing variables (currents from CMEMS/HYCOM, winds from ERA5/NCEP), as well as the ground-truth observation parameters (NOAA drifter coordinates) necessary to start a simulation. If deleted, it requires a complete re-download.
*   **output/**: This directory acts as the **Ephemeral Results Storage**. It is strictly write-only during model execution. It temporarily stores the artifacts, metrics, and figures generated by the pipeline runs (trajectories, probability rasters, mass balance CSVs). This folder is designed to be fully reproducible: you can delete any case inside output/ and the system will exactly regenerate it using the immutable files in data/.
*   **Pipeline-only prep**: All downloads and preprocessing now happen in the `pipeline` container through `PIPELINE_PHASE=prep`. The `gnome` container only reads prepared files and will fail fast if they are missing.

```
oil_spill_thesis/
├── config/                    # Configuration files
│   ├── recipes.yaml           # OpenDrift simulation recipes
│   ├── ensemble.yaml          # Ensemble perturbation parameters
│   └── oil.yaml               # Phase 3 oil types & weathering parameters
├── data/                      # Input Data Lake (Read-Only)
│   ├── drifters/              # Drifter observation data
│   ├── forcing/               # Ocean and atmospheric forcing data
│   └── shoreline/             # Shoreline/coastal geometry data
├── output/                    # Ephemeral Results Storage (Write-Only)
│   ├── validation/            # Phase 1 trajectory maps & ranking CSV
│   ├── ensemble/              # Phase 2 probability snapshots, member traces, and manifest
│   ├── weathering/            # Phase 3 mass budget CSVs & charts
│   └── gnome_comparison/      # Phase 3 PyGNOME overlay charts
├── start.ps1                  # One-click execution GUI wrapper
├── src/                       # Python source code
│   ├── core/                  # Core application logic & constants
│   ├── exceptions/            # Custom exceptions
│   ├── helpers/               # Plotting & metrics helpers
│   ├── models/                # Data models and schemas
│   ├── services/              # Business logic (simulations, validation)
│   │   ├── ensemble.py        # Phase 2: Ensemble forecasting service
│   │   ├── gnome_comparison.py  # Phase 3: PyGNOME cross-comparison
│   │   ├── ingestion.py       # Phase 1: Data ingestion service
│   │   ├── tracker.py       # Drifter search & visualization tool
│   │   ├── validation.py    # Phase 1: Transport validation service
│   │   └── weathering.py    # Phase 3: OpenOil weathering service
│   ├── utils/               # Utility functions
│   │   └── io.py            # I/O utilities
│   ├── __init__.py
│   └── __main__.py          # Main entry point (container-aware)
├── docker/
│   ├── pipeline/            # Phase 1 + 2 container (OpenDrift)
│   └── gnome/               # Phase 3 container (PyGNOME)
├── docker-compose.yml       # Dual-container setup
└── README.md                # This file
```

## Development Workflow & Cheatsheet

This project uses a **Dual Container Strategy**. The `pipeline` container runs Phases 1 & 2 (OpenDrift); the `gnome` container runs Phase 3 (PyGNOME). Each container automatically executes only its own phase via the `PIPELINE_PHASE` environment variable.

| Action | `pipeline` (Phase 1 + 2) | `gnome` (Phase 3) |
|--------|--------------------------|-------------------|
| **Start Environment** | `docker-compose up -d` | `docker-compose up -d` |
| **Prep Inputs** | `docker-compose exec -e PIPELINE_PHASE=prep pipeline python -m src` | *Not supported* |
| **Run Pipeline** | `docker-compose exec pipeline python -m src` | `docker-compose exec gnome python -m src` |
| **Open Shell** | `docker-compose exec pipeline bash` | `docker-compose exec gnome bash` |
| **View Logs** | `docker-compose logs -f pipeline` | `docker-compose logs -f gnome` |
| **Stop Environment** | `docker-compose down` | `docker-compose down` |
| **Rebuild Image** | `docker-compose up -d --build` | `docker-compose up -d --build` |

> **Note:** `prototype_2016` resolves the best recipe from `output/[CASE]/validation/validation_ranking.csv` written by the historical Phase 1 drifter-validation track. `mindoro_retro_2023` resolves the transport recipe from `config/phase1_baseline_selection.yaml` by default, or from `BASELINE_SELECTION_PATH` / `BASELINE_RECIPE_OVERRIDE` when explicitly provided.
>
> Official Mindoro deterministic control and ensemble runs write canonical forecast/ensemble manifests under `output/CASE_MINDORO_RETRO_2023/forecast/` and `output/CASE_MINDORO_RETRO_2023/ensemble/`. Prototype probability outputs such as `probability_24h` / `probability_48h` / `probability_72h` remain prototype artifacts and are not the official Phase 3B dependency.
>
> All downloads and preprocessing happen only in the `pipeline` container via `PIPELINE_PHASE=prep`, which writes `data/prepared/[CASE]/prepared_input_manifest.csv`. Later phases only read prepared files and fail fast if they are missing.

## Usage

### Workflow Modes

The pipeline now supports two explicit workflow modes:

- `prototype_2016`: preserves the original multi-date historical drifter-calibration workflow (`2016-09-01`, `2016-09-06`, `2016-09-17`)
- `mindoro_retro_2023`: runs the fixed official thesis spill case `CASE_MINDORO_RETRO_2023` using a frozen Phase 1 baseline recipe

You can switch modes either by editing `config/settings.yaml` or by overriding `WORKFLOW_MODE` at runtime:

```bash
# Pipeline-only preparation stage
docker-compose exec -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prep pipeline python -m src

# Prototype historical calibration workflow
docker-compose exec -e WORKFLOW_MODE=prototype_2016 pipeline python -m src

# Official fixed Mindoro spill-case workflow
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 pipeline python -m src

# Official minimal Phase 3B path after prep
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=official_phase3b pipeline python -m src

# Optional official-mode overrides
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e BASELINE_SELECTION_PATH=config/phase1_baseline_selection.yaml pipeline python -m src
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e BASELINE_RECIPE_OVERRIDE=cmems_era5 pipeline python -m src
```

### Running the Full Pipeline

The entry point `src/__main__.py` is **container-aware**: it reads the `PIPELINE_PHASE` environment variable set in `docker-compose.yml` to decide which phases to execute.

For explicit workflow separation, prefer the mode-specific commands below. In `mindoro_retro_2023`, the pipeline skips historical drifter validation and resolves its recipe from the frozen baseline-selection artifact instead of a case-local `validation_ranking.csv`. The highest-priority official thesis path is `prep -> official_phase3b`, which produces deterministic control, ensemble outputs, and Phase 3B scoring without requiring benchmark or weathering.

Run the prep stage in the `pipeline` container before benchmark, Phase 3, or Phase 3B:

```bash
docker-compose exec -e PIPELINE_PHASE=prep pipeline python -m src
```

```bash
# Phase 1 + 2: Validation → Ensemble (pipeline container)
docker-compose exec pipeline python -m src

# Phase 3: Oil Weathering + PyGNOME comparison (gnome container)
docker-compose exec gnome python -m src
```

Additional mode-explicit examples:

```bash
# Prototype prep stage
docker-compose exec -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prep pipeline python -m src

# Prototype Phase 1 + 2
docker-compose exec -e WORKFLOW_MODE=prototype_2016 pipeline python -m src

# Prototype Phase 3
docker-compose exec -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=3 gnome python -m src

# Official Mindoro prep stage
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=prep pipeline python -m src

# Official Mindoro minimal Phase 3B path
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=official_phase3b pipeline python -m src

# Official Mindoro scoring-only strict Phase 3B path, after forecast artifacts exist
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=3b pipeline python -m src

# Official Mindoro multi-date public-observation validation
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_multidate_public pipeline python -m src

# Official Mindoro extended public-observation inventory guardrail
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public pipeline python -m src

# Appendix-only short extended public-observation scoring, March 7-9 only
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public_scored -e EXTENDED_PUBLIC_TIER=short pipeline python -m src

# Read-only horizon-survival audit for the completed short extended run
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=horizon_survival_audit pipeline python -m src

# Official Mindoro Phase 1 + 2 only
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 pipeline python -m src

# Official Mindoro Phase 3
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=3 gnome python -m src

# Official Mindoro with explicit baseline file
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e BASELINE_SELECTION_PATH=config/phase1_baseline_selection.yaml pipeline python -m src

# Official Mindoro with explicit recipe override
docker-compose exec -e WORKFLOW_MODE=mindoro_retro_2023 -e BASELINE_RECIPE_OVERRIDE=cmems_ncep pipeline python -m src
```

### Importing Modules in Custom Scripts

If you want to use specific components in your own scripts or notebooks:

#### Running a Single Forecast

```python
from src.services.ensemble import run_ensemble

results = run_ensemble(
    config_file='config/recipes.yaml',
    ensemble_size=1,
    output_dir='output'
)
```

### Running an Ensemble Forecast

```python
from src.services.ensemble import run_ensemble

results = run_ensemble(
    config_file='config/ensemble.yaml',
    ensemble_size=50,
    output_dir='output'
)
```

### Validating Against Drifter Data

```python
from src.helpers.metrics import calculate_ncs
import numpy as np

ncs = calculate_ncs(sim_lat, sim_lon, obs_lat, obs_lon)
print(f"Normalized Cumulative Separation: {ncs}")
```
### 🔍 Drifter Tracking & Visualization

You can search for available drifter data in your region and visualize their trajectories using the tracker tool. This is useful for finding valid test dates before running a full simulation.

**Command Line Usage:**

```bash
# Scan for drifters in a specific date range (Check only)
docker-compose exec pipeline python -m src.services.tracker 2016-01-01 2016-03-01 --scan

# Generate trajectory plot for a specific date (Output: drifter_track.png)
docker-compose exec pipeline python -m src.services.tracker 2016-03-02
```

**Python Usage:**

```python
from src.services.tracker import DrifterTracker

tracker = DrifterTracker()

# Find and plot
tracker.track(start_date="2016-03-02", end_date="2016-03-05", output_path="output/track.png")

# Or just get the data
df = tracker.find_drifters("2016-03-02")
print(df)
```
## Configuration

Edit `config/recipes.yaml`, `config/ensemble.yaml`, and `config/oil.yaml` to customize:
- Simulation duration and time steps
- Advection schemes
- Weathering processes
- Ensemble perturbation magnitudes
- Spatial and temporal correlations

## Data Requirements

### Forcing Data
- Ocean currents (U, V components)
- Sea surface temperature
- Wind forcing (U, V components)
- Atmospheric pressure

Recommended sources:
- ERDDAP servers (NOAA, HYCOM, etc.)
- GEBCO for bathymetry

### Drifter Data
- GPS trajectories
- Temporal resolution: consistent with model time steps
- Format: NetCDF or CSV

### Shoreline Data
- Coastal geometry for boundary conditions
- Format: NetCDF (via PyGNOME) or shapefiles

## Citation

If you use this project in your research, please cite:

```
@thesis{year_author,
  title={Drifter-Validated Oil Spill Forecasting System},
  author={Your Name},
  year={2026},
  school={Your Institution}
}
```

## License

MIT License - See LICENSE file for details

## Contact

For questions or issues, please contact: arjayninosaguisa@gmail.com

---

**Last Updated**: 2026-04-10
**Status**: Official Mindoro Phase 3B diagnostics active; next priority is transport/retention rerun before medium extended validation
