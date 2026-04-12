# Prototype 2016 PyGNOME Similarity Summary

This package consolidates the three legacy 2016 deterministic, p50, and p90 OpenDrift transport benchmarks against deterministic PyGNOME.

Guardrails:

- legacy/debug support only
- status label: Prototype 2016 legacy debug support
- provenance: Legacy/debug regression lane preserved for reproducibility only.
- transport benchmark only
- PyGNOME is a comparator, not truth
- not final Chapter 3 evidence

Relative similarity ranking:

- `OpenDrift deterministic`:
  Rank 1: `CASE_2016-09-06` | mean FSS @ 5 km = 0.783, mean KL = 17.254, pairs = 3
  Rank 2: `CASE_2016-09-01` | mean FSS @ 5 km = 0.385, mean KL = 21.363, pairs = 3
  Rank 3: `CASE_2016-09-17` | mean FSS @ 5 km = 0.339, mean KL = 21.397, pairs = 3
- `OpenDrift p50 threshold`:
  Rank 1: `CASE_2016-09-01` | mean FSS @ 5 km = 0.000, mean KL = 0.230, pairs = 3
  Rank 2: `CASE_2016-09-06` | mean FSS @ 5 km = 0.000, mean KL = 0.230, pairs = 3
  Rank 3: `CASE_2016-09-17` | mean FSS @ 5 km = 0.000, mean KL = 0.407, pairs = 3
- `OpenDrift p90 threshold`:
  Rank 1: `CASE_2016-09-01` | mean FSS @ 5 km = 0.000, mean KL = 0.230, pairs = 3
  Rank 2: `CASE_2016-09-06` | mean FSS @ 5 km = 0.000, mean KL = 0.230, pairs = 3
  Rank 3: `CASE_2016-09-17` | mean FSS @ 5 km = 0.000, mean KL = 0.407, pairs = 3

Per-case snapshot highlights:

- `CASE_2016-09-06` / `OpenDrift deterministic`: FSS @ 5 km (24/48/72 h) = 0.812 / 0.747 / 0.789; KL (24/48/72 h) = 19.676 / 21.453 / 10.632
- `CASE_2016-09-01` / `OpenDrift deterministic`: FSS @ 5 km (24/48/72 h) = 0.693 / 0.357 / 0.105; KL (24/48/72 h) = 21.365 / 21.362 / 21.361
- `CASE_2016-09-17` / `OpenDrift deterministic`: FSS @ 5 km (24/48/72 h) = 0.542 / 0.318 / 0.158; KL (24/48/72 h) = 21.414 / 21.398 / 21.381
- `CASE_2016-09-01` / `OpenDrift p50 threshold`: FSS @ 5 km (24/48/72 h) = 0.000 / 0.000 / 0.000; KL (24/48/72 h) = 0.000 / 0.000 / 0.691
- `CASE_2016-09-06` / `OpenDrift p50 threshold`: FSS @ 5 km (24/48/72 h) = 0.000 / 0.000 / 0.000; KL (24/48/72 h) = 0.000 / 0.000 / 0.691
- `CASE_2016-09-17` / `OpenDrift p50 threshold`: FSS @ 5 km (24/48/72 h) = 0.000 / 0.000 / 0.000; KL (24/48/72 h) = 0.334 / 0.451 / 0.434
- `CASE_2016-09-01` / `OpenDrift p90 threshold`: FSS @ 5 km (24/48/72 h) = 0.000 / 0.000 / 0.000; KL (24/48/72 h) = 0.000 / 0.000 / 0.691
- `CASE_2016-09-06` / `OpenDrift p90 threshold`: FSS @ 5 km (24/48/72 h) = 0.000 / 0.000 / 0.000; KL (24/48/72 h) = 0.000 / 0.000 / 0.691
- `CASE_2016-09-17` / `OpenDrift p90 threshold`: FSS @ 5 km (24/48/72 h) = 0.000 / 0.000 / 0.000; KL (24/48/72 h) = 0.334 / 0.451 / 0.434

Interpretation:

- Higher FSS means stronger footprint overlap between the named OpenDrift track and deterministic PyGNOME.
- Lower KL means the normalized density fields are more similar over the ocean cells.
- The ranking is relative within each comparison track inside the prototype_2016 support set only.
- The per-forecast figures under `figures/` are support visuals built from the stored benchmark rasters only, now shown with footprint-first rendering over canonical Mindoro land/shoreline context, with a provenance source-point star when available.
