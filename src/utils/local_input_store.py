"""Helpers for persistent local input store paths, staging, and inventories."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PERSISTENT_LOCAL_INPUT_STORE = "persistent_local_input_store"
TEMPORARY_OUTPUT_CACHE = "temporary_output_cache"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if np.isnan(value) else float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def persistent_local_input_dir(run_name: str, *parts: str) -> Path:
    """Return the canonical persistent local-input-store directory for a workflow lane."""
    root = Path("data") / "local_input_store" / str(run_name)
    for part in parts:
        root = root / str(part)
    return root


def write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def write_inventory(csv_path: Path, rows: list[dict[str, Any]], *, json_path: Path | None = None) -> dict[str, Path]:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    frame.to_csv(csv_path, index=False)
    resolved_json = json_path or csv_path.with_suffix(".json")
    write_json(resolved_json, frame.to_dict(orient="records"))
    return {"csv": csv_path, "json": resolved_json}


def stage_store_file(store_path: Path, stage_path: Path) -> Path:
    """Copy a persistent store file into an output-local staging path when needed."""
    stage_path.parent.mkdir(parents=True, exist_ok=True)
    if store_path.resolve() == stage_path.resolve():
        return stage_path
    shutil.copy2(store_path, stage_path)
    return stage_path


def classify_reuse_action(status: str | None) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {
        "reused_validated_cache",
        "cached",
        "reused_local_file",
        "reused_existing_real_historical_file",
    }:
        return "reused_valid_local_store"
    if normalized in {"staged_legacy_cache", "augmented_partial_cache"}:
        return "reused_local_store_after_staging"
    if normalized in {"downloaded"}:
        return "downloaded_new_file"
    if normalized in {"force_refresh_downloaded", "force_refreshed"}:
        return "force_refreshed_file"
    if normalized in {"copied_from_canonical_store"}:
        return "reused_valid_local_store"
    return normalized or "unknown"


def validation_status_from_record(record: dict[str, Any] | None) -> str:
    record = dict(record or {})
    validation = dict(record.get("validation") or {})
    if validation:
        if validation.get("valid"):
            return "validated"
        reason = str(validation.get("reason") or "").strip()
        return f"invalid:{reason}" if reason else "invalid"
    return str(record.get("validation_status") or "").strip() or "not_recorded"
