"""Build stored-output-only B1 drifter provenance context assets for panel review."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.services.focused_phase1_segment_map import generate_focused_phase1_segment_map


PHASE = "panel_b1_drifter_context"
OUTPUT_DIR = Path("output") / "panel_drifter_context"
ACCEPTED_REGISTRY_PATH = Path("output") / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_accepted_segment_registry.csv"
RANKING_SUBSET_REGISTRY_PATH = Path("output") / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_ranking_subset_registry.csv"
PHASE1_MANIFEST_PATH = Path("output") / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_production_manifest.json"
B1_RUN_MANIFEST_PATH = (
    Path("output")
    / "Phase 3B March13-14 Final Output"
    / "summary"
    / "opendrift_primary"
    / "march13_14_reinit_run_manifest.json"
)


def _relative(repo_root: Path, path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except Exception:
        return str(path)


def _load_optional_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _direct_march13_14_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool)
    start_series = df.get("start_time_utc", pd.Series("", index=df.index)).fillna("").astype(str)
    end_series = df.get("end_time_utc", pd.Series("", index=df.index)).fillna("").astype(str)
    pattern = r"2023-03-13|2023-03-14"
    return start_series.str.contains(pattern, na=False) | end_series.str.contains(pattern, na=False)


def run_panel_b1_drifter_context(*, repo_root: str | Path = ".") -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    output_dir = (repo_root / OUTPUT_DIR).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    accepted_path = repo_root / ACCEPTED_REGISTRY_PATH
    subset_path = repo_root / RANKING_SUBSET_REGISTRY_PATH
    phase1_manifest_path = repo_root / PHASE1_MANIFEST_PATH
    b1_manifest_path = repo_root / B1_RUN_MANIFEST_PATH

    accepted = _load_optional_csv(accepted_path)
    subset = _load_optional_csv(subset_path)
    phase1_manifest = _load_optional_json(phase1_manifest_path)
    b1_manifest = _load_optional_json(b1_manifest_path)

    direct_accepted = accepted.loc[_direct_march13_14_mask(accepted)].copy() if not accepted.empty else pd.DataFrame()
    direct_subset = subset.loc[_direct_march13_14_mask(subset)].copy() if not subset.empty else pd.DataFrame()
    direct_segments_found = bool(not direct_accepted.empty or not direct_subset.empty)

    map_metadata: dict[str, Any] = {}
    map_error = ""
    map_generated = False
    map_output_path = output_dir / "b1_drifter_context_map.png"
    map_metadata_path = output_dir / "b1_drifter_context_map.json"
    if accepted_path.exists() and subset_path.exists():
        try:
            map_metadata = generate_focused_phase1_segment_map(
                repo_root=repo_root,
                output_dir=OUTPUT_DIR,
                output_stem="b1_drifter_context_map",
            )
            map_generated = map_output_path.exists()
        except Exception as exc:  # pragma: no cover - defensive read-only fallback
            map_error = str(exc)

    manifest = {
        "phase": PHASE,
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "repo_root": str(repo_root),
        "output_dir": OUTPUT_DIR.as_posix(),
        "source_files_used": [
            _relative(repo_root, accepted_path if accepted_path.exists() else None),
            _relative(repo_root, subset_path if subset_path.exists() else None),
            _relative(repo_root, phase1_manifest_path if phase1_manifest_path.exists() else None),
            _relative(repo_root, b1_manifest_path if b1_manifest_path.exists() else None),
        ],
        "accepted_segment_count": int(len(accepted)),
        "ranking_subset_count": int(len(subset)),
        "direct_march13_14_2023_accepted_segments_found": direct_segments_found,
        "direct_march13_14_2023_accepted_segment_count": int(len(direct_accepted)),
        "direct_march13_14_2023_subset_segment_count": int(len(direct_subset)),
        "official_b1_recipe": str(((b1_manifest.get("recipe") or {}).get("recipe")) or phase1_manifest.get("official_b1_recipe") or "").strip(),
        "winning_recipe": str(phase1_manifest.get("historical_four_recipe_winner") or phase1_manifest.get("winning_recipe") or "").strip(),
        "provenance_lane": "phase1_mindoro_focus_pre_spill_2016_2023",
        "claim_boundary": (
            "Drifter records support B1 recipe provenance only; they are not the direct March 13-14 "
            "public-observation truth mask."
        ),
        "output_role": "transport_provenance_context_only",
        "no_science_rerun": True,
        "map_generated": map_generated,
        "map_output_path": _relative(repo_root, map_output_path if map_output_path.exists() else None),
        "map_metadata_path": _relative(repo_root, map_metadata_path if map_metadata_path.exists() else None),
        "map_generation_error": map_error,
        "status": (
            "No direct March 13-14 2023 accepted drifter segment is stored for B1. "
            "The displayed drifter data is the historical focused Phase 1 provenance set used for recipe selection."
            if not direct_segments_found
            else "Directly dated March 13-14 drifter records were found in stored files, but they remain supplementary context only."
        ),
    }
    if map_metadata:
        manifest["map_metadata"] = map_metadata

    manifest_path = output_dir / "b1_drifter_context_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return {
        "output_dir": str(output_dir),
        "manifest_path": str(manifest_path),
        "map_output_path": str(map_output_path),
        "map_metadata_path": str(map_metadata_path),
        "accepted_segment_count": int(len(accepted)),
        "ranking_subset_count": int(len(subset)),
        "direct_march13_14_2023_accepted_segments_found": direct_segments_found,
        "output_role": manifest["output_role"],
        "no_science_rerun": True,
    }


__all__ = ["PHASE", "run_panel_b1_drifter_context"]
