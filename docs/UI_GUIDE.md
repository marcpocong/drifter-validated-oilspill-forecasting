# UI Guide

## Purpose

The local dashboard is a read-only exploration/support layer over the outputs that already exist in this repo. It does not rerun model branches, it does not modify scientific artifacts, and it does not pretend that missing comparisons already exist.

## Launch Command

Start the pipeline container first if needed:

```bash
docker-compose up -d pipeline
```

Then launch the UI:

```bash
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Open:

```text
http://localhost:8501
```

## Print / Export Mode

The UI now has a dedicated export snapshot mode for browser-to-PDF saves.

Open any dashboard page with:

```text
http://localhost:8501/?export=1
```

If you are already on a page route, keep the route and add the query parameter:

```text
http://localhost:8501/<page-path>?export=1
```

Export mode is read-only and intentionally simplified:

- the sidebar is hidden
- navigation chrome is hidden
- publication figures render in a wider, print-friendly layout
- tabs are flattened into sequential static sections
- download buttons and other interactive-only controls are hidden
- the page stays in the publication-first layer

Export mode is meant for panel snapshots and PDF handouts, not for deep artifact inspection.

## What The UI Reads

The UI reads existing artifacts only:

- `output/Phase 3B March13-14 Final Output/`
- `output/Phase 3C DWH Final Output/`
- `output/2016 Legacy Runs FINAL Figures/`
- `output/final_validation_package/`
- `output/final_reproducibility_package/`
- `output/figure_package_publication/`
- `output/phase4/CASE_MINDORO_RETRO_2023/`
- `output/phase4_crossmodel_comparability_audit/`
- `output/trajectory_gallery_panel/`
- `output/trajectory_gallery/`
- raw `CASE_*` trees only as advanced fallback when a curated package or synced registry does not already provide the needed browse surface

Missing optional files are tolerated. The UI shows a gentle notice instead of failing where practical.

## Panel-Friendly Mode

This is the default mode. It prioritizes:

- publication-grade figures
- plain-language study structure cards
- curated final packages
- recommended defense figures
- simplified summary tables
- soft-fail messaging instead of debug-style missing-file errors

## What Export Mode Includes

- page title and short framing note
- main plain-language callouts
- sequential sections instead of tabs
- one or a few featured figures per section instead of large figure grids
- concise summary tables and notes

## What Export Mode Omits

- sidebar controls
- advanced/raw figure layers
- download buttons
- artifact preview selectors
- tab-only or expander-only navigation patterns

Recommended first stops:

- `Home / Overview`
- `Phase 1 Recipe Selection`
- `Mindoro B1 Primary Validation`
- `DWH Phase 3C Transfer Validation`
- `Legacy 2016 Support Package`
- `Phase 4 Oil-Type and Shoreline Context`

## Advanced Mode

Advanced mode opens lower-level inspection without changing the scientific state:

- panel-gallery and raw-gallery figure layers
- manifest previews
- log previews
- output-catalog browsing
- trajectory source artifact inspection

This mode is still read-only.

## Recommended PDF Workflow

1. Launch the UI and open the page you want to export.
2. Add `?export=1` to the URL.
3. Wait for the page to re-render in export mode.
4. In the browser, use `Print` or `Save as PDF`.
5. Use portrait or landscape based on the page, but keep background graphics enabled so the title and note cards render correctly.
6. Save one page at a time if you want stable panel-ready PDFs.

## Pages

- `Home / Overview`
- `Phase 1 Recipe Selection`
- `Mindoro B1 Primary Validation`
- `Mindoro Cross-Model Comparator`
- `DWH Phase 3C Transfer Validation`
- `Phase 4 Oil-Type and Shoreline Context`
- `Legacy 2016 Support Package`
- `Artifacts / Logs / Registries`
- `Trajectory Explorer` in advanced mode

## Honesty Rules Surfaced In The UI

- Mindoro `B1` is the only primary validation row.
- Mindoro `A` is comparator-only support attached to B1 and never truth.
- Mindoro `B2` remains the legacy reference row.
- Mindoro `B3` remains broader support / appendix context only.
- `prototype_2016` is always support-only / legacy in the UI and has its own dedicated page.
- The dedicated Mindoro-focused Phase 1 provenance rerun selects the recipe that B1 inherits.
- `Phase 3B` and `Phase 3C` remain the main validation pages in the thesis-facing story.
- Mindoro B1 inherits recipe provenance from the separate focused Phase 1 rerun; Phase 3B itself does not directly ingest drifters.
- DWH stays separate from the Phase 1 drifter-baseline story and explicitly uses no drifter baseline.
- Mindoro Phase 4 is presented in plain language as OpenDrift/OpenOil scenario context only.
- No matched Mindoro Phase 4 PyGNOME package is currently shown.
- The legacy 2016 page includes a budget-only deterministic PyGNOME Phase 4 pilot, but shoreline comparison is still unavailable there.

## No Run Buttons Yet

The first dashboard version is intentionally read-only. It does not expose scientific rerun controls, write actions, or packaging rebuild buttons.

## Branding

- Preferred logo files:
  - `ui/assets/logo.svg`
  - `ui/assets/logo.png`
- Optional icon files:
  - `ui/assets/logo_icon.png`
  - `ui/assets/logo_icon.svg`
- If no logo is present, the UI falls back to text-only branding without breaking.
- See `docs/UI_BRANDING.md` for the supported filenames, recommendations, and replacement steps.
