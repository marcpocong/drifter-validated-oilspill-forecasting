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

from ui.evidence_contract import ROLE_GOVERNANCE
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


ROLE_ORDER = [
    "observation validation mask",
    "transport validation",
    "forcing input",
    "shoreline support",
    "oil-property support",
    "model/tool provenance",
    "ui/review tool",
]

ROLE_LABELS = {
    "observation validation mask": "Observation Validation Mask",
    "transport validation": "Transport Validation",
    "forcing input": "Forcing Input",
    "shoreline support": "Shoreline Support",
    "oil-property support": "Oil-Property Support",
    "model/tool provenance": "Model / Tool Provenance",
    "ui/review tool": "UI / Review Tool",
}

DISPLAY_COLUMNS = {
    "source_id": "Source ID",
    "label": "Source",
    "provider": "Provider",
    "product_or_layer": "Product / layer",
    "role": "Role",
    "evidence_boundary": "Evidence boundary",
    "workflow_lanes": "Workflow lanes",
    "time_period_used": "Time period used",
    "repo_manifests": "Repo manifests",
    "official_link_if_known": "Official link if known",
    "access_caveats": "Access caveats",
    "secrets_required": "Secrets required",
    "stored_in_repo": "Stored in repo",
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
    if isinstance(registry.get("sources"), dict):
        source_items = list(registry["sources"].items())
    elif isinstance(registry.get("sources"), list):
        source_items = [
            (str(item.get("source_id") or index), item)
            for index, item in enumerate(registry["sources"])
            if isinstance(item, dict)
        ]
    elif isinstance(registry, dict):
        source_items = list(registry.items())
    else:
        return []

    records: list[dict[str, str]] = []
    for key, payload in source_items:
        if not isinstance(payload, dict):
            continue
        source_id = _flatten(payload.get("source_id")) or _flatten(payload.get("id")) or str(key)
        product_or_layer = _flatten(payload.get("product_or_layer")) or _flatten(payload.get("product_or_layer_id"))
        role = _flatten(payload.get("role")) or _flatten(payload.get("category"))
        evidence_boundary = _flatten(payload.get("evidence_boundary")) or _flatten(payload.get("evidence_role"))
        workflow_lanes = _flatten(payload.get("workflow_lanes")) or _flatten(payload.get("used_in_workflows"))
        time_period_used = _flatten(payload.get("time_period_used")) or _flatten(payload.get("time_coverage_used"))
        repo_manifests = _flatten(payload.get("repo_manifests")) or _flatten(payload.get("manifest_or_inventory_paths"))
        access_caveats = _flatten(payload.get("access_caveats")) or _flatten(payload.get("caveats"))
        official_link_if_known = _flatten(payload.get("official_link_if_known"))
        official_url = _flatten(payload.get("official_url"))
        item_url = _flatten(payload.get("access_endpoint_or_item_url"))
        official_or_item_link = official_link_if_known or official_url or item_url
        if official_url and item_url and item_url != official_url:
            official_or_item_link = f"{official_url}; {item_url}"
        record = {
            "source_id": source_id,
            "label": _flatten(payload.get("label")) or str(key),
            "provider": _flatten(payload.get("provider")),
            "product_or_layer": product_or_layer,
            "role": role,
            "evidence_boundary": evidence_boundary,
            "workflow_lanes": workflow_lanes,
            "time_period_used": time_period_used,
            "repo_manifests": repo_manifests,
            "official_link_if_known": official_or_item_link,
            "access_caveats": access_caveats,
            "secrets_required": _flatten(payload.get("secrets_required")),
            "stored_in_repo": _flatten(payload.get("stored_in_repo")),
        }
        records.append(record)
    return records


def _registry_frame(registry: dict[str, Any]) -> pd.DataFrame:
    records = _registry_records(registry)
    if not records:
        return pd.DataFrame(columns=list(DISPLAY_COLUMNS.values()))
    df = pd.DataFrame(records)
    df["role_sort"] = df["role"].map({role: index for index, role in enumerate(ROLE_ORDER)}).fillna(999)
    df = df.sort_values(["role_sort", "label"]).drop(columns=["role_sort"])
    return df.rename(columns=DISPLAY_COLUMNS)


def _filter_frame(df: pd.DataFrame, *, search_text: str, roles: list[str]) -> pd.DataFrame:
    filtered = df.copy()
    if roles and "Role" in filtered.columns:
        filtered = filtered[filtered["Role"].isin(roles)]
    search = search_text.strip().lower()
    if search:
        haystack = filtered.astype(str).agg(" ".join, axis=1).str.lower()
        filtered = filtered[haystack.str.contains(search, regex=False)]
    return filtered.reset_index(drop=True)


def _category_sequence(df: pd.DataFrame) -> list[str]:
    present = [role for role in ROLE_ORDER if role in set(df.get("Role", pd.Series(dtype=str)).astype(str))]
    extras = sorted(set(df.get("Role", pd.Series(dtype=str)).astype(str)) - set(present))
    return present + extras


def _category_count(role_counts: dict[str, int], *roles: str) -> int:
    return sum(int(role_counts.get(role, 0)) for role in roles)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    render_modern_hero(
        "Data Sources & Provenance",
        "Reference registry of external observation, drifter, forcing, shoreline, oil-property, and model/tool sources used by the stored workflow.",
        badge=ROLE_GOVERNANCE,
        eyebrow="Provenance reference",
        meta=["Drifters", "Forcing", "Public masks", "Manifests"],
        tone="readonly",
    )
    render_badge_strip([ROLE_GOVERNANCE, "stored outputs", "provenance", "read-only"])

    render_key_takeaway(
        "Panel answer",
        "The workflow uses public drifter, satellite/ArcGIS observation, ocean-current, wind, wave/Stokes, shoreline, and oil-property sources. Exact source names, links, repo paths, and caveats are listed below.",
        tone="readonly",
        badge=ROLE_GOVERNANCE,
    )
    render_status_callout(
        "How to read this page",
        "Observation reference != model comparator. Drifter provenance != oil-footprint validation. Forcing data != validation target.",
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

    role_counts = df["Role"].value_counts().to_dict() if "Role" in df.columns else {}
    manifest_count = int(
        df.astype(str)
        .agg(" ".join, axis=1)
        .str.contains("manifest|package|registry", case=False, regex=True, na=False)
        .sum()
    )
    render_metric_row(
        [
            ("Registered sources", str(len(df))),
            ("Observation entries", str(role_counts.get("observation validation mask", 0))),
            ("Forcing entries", str(role_counts.get("forcing input", 0))),
            ("Tool/support entries", str(sum(role_counts.get(role, 0) for role in ["shoreline support", "oil-property support", "model/tool provenance", "ui/review tool"]))),
        ],
        export_mode=export_mode,
    )
    render_section_header(
        "Source Families",
        "Cards group the same registry into the families panel reviewers usually ask about first.",
        badge=ROLE_GOVERNANCE,
    )
    render_feature_grid(
        [
            {
                "title": "Drifters",
                "body": "Transport-provenance inputs and drifter validation sources.",
                "badge": ROLE_GOVERNANCE,
                "note": f"{_category_count(role_counts, 'transport validation')} registered source(s)",
                "tone": "advanced",
            },
            {
                "title": "Forcing",
                "body": "Ocean-current, wind, and wave/Stokes products used by stored model runs.",
                "badge": ROLE_GOVERNANCE,
                "note": f"{_category_count(role_counts, 'forcing input')} registered source(s)",
                "tone": "advanced",
            },
            {
                "title": "Public masks",
                "body": "Public-observation masks used as validation references, not model comparators.",
                "badge": ROLE_GOVERNANCE,
                "note": f"{_category_count(role_counts, 'observation validation mask')} registered source(s)",
                "tone": "advanced",
            },
            {
                "title": "Shoreline / grids",
                "body": "Shoreline, geography, grid, and support layers used by stored outputs.",
                "badge": ROLE_GOVERNANCE,
                "note": f"{_category_count(role_counts, 'shoreline support')} registered source(s)",
                "tone": "advanced",
            },
            {
                "title": "Manifests / package roots",
                "body": "Repo manifests, registries, inventories, and package-root references used for audit.",
                "badge": ROLE_GOVERNANCE,
                "note": f"{manifest_count} source row(s) mention manifests, packages, or registries",
                "tone": "advanced",
            },
        ],
        columns_per_row=3,
        export_mode=export_mode,
    )

    search_text = ""
    selected_roles: list[str] = []
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
            role_options = _category_sequence(df)
            selected_roles = st.multiselect(
                "Role",
                options=role_options,
                format_func=lambda value: ROLE_LABELS.get(value, value.replace("_", " ").title()),
                key="data_sources_category_filter",
            )

    filtered = _filter_frame(df, search_text=search_text, roles=selected_roles)
    render_section_header(
        "Registry Details",
        "Use filters for review; grouped tables below preserve exact source names, links, caveats, and workflow roles.",
    )
    render_table(
        "All registered sources",
        filtered,
        download_name="data_sources_registry.csv",
        caption="Machine-readable source: config/data_sources.yaml. Missing exact links are stated rather than guessed.",
        height=360,
        max_rows=None,
        export_mode=export_mode,
    )

    for category in _category_sequence(filtered):
        category_df = filtered[filtered["Role"].astype(str).eq(category)].reset_index(drop=True)
        if category_df.empty:
            continue
        label = ROLE_LABELS.get(category, category.replace("_", " ").title())
        render_table(
            label,
            category_df,
            download_name=f"data_sources_{category}.csv",
            caption="Grouped provenance view. Evidence boundary and caveats carry the thesis claim boundary for this source family.",
            height=260,
            max_rows=None,
            export_mode=export_mode,
        )
