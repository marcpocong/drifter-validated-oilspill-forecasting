"""Data-source provenance page."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    from ui.bootstrap import ensure_repo_root_on_path
except ModuleNotFoundError:
    import sys

    _UI_DIR = Path(__file__).resolve().parents[1]
    _UI_DIR_TEXT = str(_UI_DIR)
    if _UI_DIR_TEXT not in sys.path:
        sys.path.insert(0, _UI_DIR_TEXT)
    from bootstrap import ensure_repo_root_on_path

ensure_repo_root_on_path(__file__)

import pandas as pd
import streamlit as st

from ui.pages.common import (
    render_badge_strip,
    render_feature_grid,
    render_key_takeaway,
    render_metric_row,
    render_modern_hero,
    render_section_header,
    render_status_callout,
    render_table,
)


CATEGORY_ORDER = [
    "observation_truth",
    "transport_validation",
    "ocean_current_forcing",
    "wind_forcing",
    "wave_forcing",
    "shoreline_geography",
    "oil_property",
    "model_tool",
    "support_reference",
]

CATEGORY_LABELS = {
    "observation_truth": "Observation Truth",
    "transport_validation": "Transport Validation",
    "ocean_current_forcing": "Ocean Current Forcing",
    "wind_forcing": "Wind Forcing",
    "wave_forcing": "Wave / Stokes Forcing",
    "shoreline_geography": "Shoreline Geography",
    "oil_property": "Oil Property",
    "model_tool": "Model / Tool Provenance",
    "support_reference": "Support Reference",
}

DISPLAY_COLUMNS = {
    "label": "Source",
    "provider": "Provider",
    "category": "Category",
    "product_or_layer_id": "Product / layer ID",
    "used_in_workflows": "Used for",
    "evidence_role": "Evidence role",
    "manuscript_role": "Workflow lane",
    "time_coverage_used": "Time period used",
    "manifest_or_inventory_paths": "Repo manifests / outputs",
    "official_or_item_link": "Official link or item link",
    "caveats": "Caveats / access notes",
    "status": "Status",
}


def _flatten(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join(_flatten(item) for item in value if _flatten(item))
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return str(value).strip()


def _registry_records(registry: dict[str, Any]) -> list[dict[str, str]]:
    source_map = registry.get("sources") if isinstance(registry.get("sources"), dict) else registry
    if not isinstance(source_map, dict):
        return []

    records: list[dict[str, str]] = []
    for key, payload in source_map.items():
        if not isinstance(payload, dict):
            continue
        official_url = _flatten(payload.get("official_url"))
        item_url = _flatten(payload.get("access_endpoint_or_item_url"))
        official_or_item_link = official_url or item_url
        if official_url and item_url and item_url != official_url:
            official_or_item_link = f"{official_url}; {item_url}"
        record = {
            "id": _flatten(payload.get("id")) or str(key),
            "label": _flatten(payload.get("label")) or str(key),
            "provider": _flatten(payload.get("provider")),
            "category": _flatten(payload.get("category")),
            "product_or_layer_id": _flatten(payload.get("product_or_layer_id")),
            "used_in_workflows": _flatten(payload.get("used_in_workflows")),
            "evidence_role": _flatten(payload.get("evidence_role")),
            "manuscript_role": _flatten(payload.get("manuscript_role")),
            "time_coverage_used": _flatten(payload.get("time_coverage_used")),
            "manifest_or_inventory_paths": _flatten(payload.get("manifest_or_inventory_paths")),
            "official_or_item_link": official_or_item_link,
            "caveats": _flatten(payload.get("caveats")),
            "status": _flatten(payload.get("status")),
        }
        records.append(record)
    return records


def _registry_frame(registry: dict[str, Any]) -> pd.DataFrame:
    records = _registry_records(registry)
    if not records:
        return pd.DataFrame(columns=list(DISPLAY_COLUMNS.values()))
    df = pd.DataFrame(records)
    df["category_sort"] = df["category"].map({category: index for index, category in enumerate(CATEGORY_ORDER)}).fillna(999)
    df = df.sort_values(["category_sort", "label"]).drop(columns=["category_sort"])
    return df.rename(columns=DISPLAY_COLUMNS)


def _filter_frame(df: pd.DataFrame, *, search_text: str, categories: list[str]) -> pd.DataFrame:
    filtered = df.copy()
    if categories and "Category" in filtered.columns:
        filtered = filtered[filtered["Category"].isin(categories)]
    search = search_text.strip().lower()
    if search:
        haystack = filtered.astype(str).agg(" ".join, axis=1).str.lower()
        filtered = filtered[haystack.str.contains(search, regex=False)]
    return filtered.reset_index(drop=True)


def _category_sequence(df: pd.DataFrame) -> list[str]:
    present = [category for category in CATEGORY_ORDER if category in set(df.get("Category", pd.Series(dtype=str)).astype(str))]
    extras = sorted(set(df.get("Category", pd.Series(dtype=str)).astype(str)) - set(present))
    return present + extras


def _category_count(category_counts: dict[str, int], *categories: str) -> int:
    return sum(int(category_counts.get(category, 0)) for category in categories)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    render_modern_hero(
        "Data Sources & Provenance",
        "Reference registry of external observation, drifter, forcing, shoreline, oil-property, and model/tool sources used by the stored workflow.",
        badge="Reference page / read-only provenance",
        eyebrow="Provenance reference",
        meta=["Drifters", "Forcing", "Public masks", "Manifests"],
        tone="advanced",
    )
    render_badge_strip(["panel-ready", "stored outputs", "provenance", "read-only"])

    render_key_takeaway(
        "Panel answer",
        "The workflow uses public drifter, satellite/ArcGIS observation, ocean-current, wind, wave/Stokes, shoreline, and oil-property sources. Exact source names, links, repo paths, and caveats are listed below.",
        tone="advanced",
        badge="Reference page / read-only provenance",
    )
    render_status_callout(
        "How to read this page",
        "Observation truth != model comparator. Drifter provenance != oil-footprint truth. Forcing data != validation target.",
        tone="info",
    )

    registry = state.get("data_source_registry", {})
    df = _registry_frame(registry if isinstance(registry, dict) else {})
    if df.empty:
        render_status_callout(
            "Data-source registry not loaded",
            "No data-source registry was loaded. Expected config/data_sources.yaml. The rest of the dashboard can still be reviewed from stored outputs.",
            "warning",
        )
        return

    category_counts = df["Category"].value_counts().to_dict() if "Category" in df.columns else {}
    manifest_count = int(
        df.astype(str)
        .agg(" ".join, axis=1)
        .str.contains("manifest|package|registry", case=False, regex=True, na=False)
        .sum()
    )
    render_metric_row(
        [
            ("Registered sources", str(len(df))),
            ("Observation entries", str(category_counts.get("observation_truth", 0))),
            ("Forcing entries", str(sum(category_counts.get(category, 0) for category in ["ocean_current_forcing", "wind_forcing", "wave_forcing"]))),
            ("Tool/support entries", str(sum(category_counts.get(category, 0) for category in ["shoreline_geography", "oil_property", "model_tool", "support_reference"]))),
        ],
        export_mode=export_mode,
    )
    render_section_header(
        "Source Families",
        "Cards group the same registry into the families panel reviewers usually ask about first.",
        badge="Reference page / read-only provenance",
    )
    render_feature_grid(
        [
            {
                "title": "Drifters",
                "body": "Transport-provenance inputs and drifter validation sources.",
                "badge": "Reference page",
                "note": f"{_category_count(category_counts, 'transport_validation')} registered source(s)",
                "tone": "advanced",
            },
            {
                "title": "Forcing",
                "body": "Ocean-current, wind, and wave/Stokes products used by stored model runs.",
                "badge": "Reference page",
                "note": f"{_category_count(category_counts, 'ocean_current_forcing', 'wind_forcing', 'wave_forcing')} registered source(s)",
                "tone": "advanced",
            },
            {
                "title": "Public masks",
                "body": "Observation-derived masks used as external validation context, not model comparators.",
                "badge": "Reference page",
                "note": f"{_category_count(category_counts, 'observation_truth')} registered source(s)",
                "tone": "advanced",
            },
            {
                "title": "Shoreline / grids",
                "body": "Shoreline, geography, grid, and support layers used by stored outputs.",
                "badge": "Reference page",
                "note": f"{_category_count(category_counts, 'shoreline_geography', 'support_reference')} registered source(s)",
                "tone": "advanced",
            },
            {
                "title": "Manifests / package roots",
                "body": "Repo manifests, registries, inventories, and package-root references used for audit.",
                "badge": "Reference page",
                "note": f"{manifest_count} source row(s) mention manifests, packages, or registries",
                "tone": "advanced",
            },
        ],
        columns_per_row=3,
        export_mode=export_mode,
    )

    search_text = ""
    selected_categories: list[str] = []
    if not export_mode:
        columns = st.columns([2, 1])
        with columns[0]:
            search_text = st.text_input(
                "Search sources",
                value="",
                placeholder="Search provider, product, workflow, caveat, or path",
                key="data_sources_search",
            )
        with columns[1]:
            category_options = _category_sequence(df)
            selected_categories = st.multiselect(
                "Category",
                options=category_options,
                format_func=lambda value: CATEGORY_LABELS.get(value, value.replace("_", " ").title()),
                key="data_sources_category_filter",
            )

    filtered = _filter_frame(df, search_text=search_text, categories=selected_categories)
    render_section_header(
        "Registry Details",
        "Use filters for review; grouped tables below preserve exact source names, links, caveats, and workflow roles.",
    )
    render_table(
        "All registered sources",
        filtered,
        download_name="data_sources_registry.csv",
        caption="Machine-readable source: config/data_sources.yaml. Rows with missing exact links are marked for verification rather than guessed.",
        height=360,
        max_rows=None,
        export_mode=export_mode,
    )

    for category in _category_sequence(filtered):
        category_df = filtered[filtered["Category"].astype(str).eq(category)].reset_index(drop=True)
        if category_df.empty:
            continue
        label = CATEGORY_LABELS.get(category, category.replace("_", " ").title())
        render_table(
            label,
            category_df,
            download_name=f"data_sources_{category}.csv",
            caption="Grouped provenance view. Evidence role and caveats carry the thesis claim boundary for this source family.",
            height=260,
            max_rows=None,
            export_mode=export_mode,
        )
