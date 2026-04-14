"""Mindoro cross-model comparator page."""

from __future__ import annotations

from pathlib import Path

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

import streamlit as st

from ui.pages.common import (
    render_export_note,
    render_figure_gallery,
    render_markdown_block,
    render_package_cards,
    render_page_intro,
    render_status_callout,
    render_table,
)


def _filter_table(df, *, blocked_values: set[str]) -> object:
    if df is None or df.empty or "track_id" not in df.columns:
        return df
    payload = df.copy()
    return payload.loc[~payload["track_id"].fillna("").astype(str).isin(sorted(blocked_values))].reset_index(drop=True)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    registry = state["mindoro_final_registry"]
    figures = registry.loc[
        registry.get("artifact_group", "").astype(str).eq("publication/comparator_pygnome")
        & registry.get("status_key", "").astype(str).eq("mindoro_crossmodel_comparator")
    ].reset_index(drop=True)
    comparator_ranking = _filter_table(state["mindoro_comparator_ranking"], blocked_values={"R0_reinit_p50"})
    comparator_summary = _filter_table(state["mindoro_comparator_summary"], blocked_values={"R0_reinit_p50"})
    archive_package = next(
        (package for package in state.get("curated_package_roots", []) if package.get("package_id") == "mindoro_validation_archive"),
        None,
    )

    render_page_intro(
        "Mindoro Cross-Model Comparator",
        "This page is the dedicated home for the thesis-facing Mindoro Track A support comparison. It stays comparator-only, uses the same March 14 target as the March 13 -> March 14 R1 primary validation row, and never lets PyGNOME read like truth or a co-primary validation row.",
        badge="Mindoro A | comparator-only",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode keeps the comparator page concise so the PDF reads as cross-model context rather than a second validation claim.",
                "The figures and tables below remain comparator-only and should be read alongside the main B1 page, not instead of it.",
            ]
        )

    render_status_callout(
        "Support comparison only",
        "This page stays comparator-only. It helps compare model behavior on the same target, but it does not replace the main OpenDrift-versus-observation claim.",
        "warning",
    )
    render_status_callout(
        "Relationship to B1",
        "Track A is attached to the B1 package as supporting cross-model context on the same March 14 target. Archived March 13 -> March 14 R0 comparator outputs were moved to the Mindoro Validation Archive page.",
        "info",
    )
    render_status_callout(
        "Phase 4 honesty note",
        "This page stays in Phase 3 spatial-comparator territory only. No matched Mindoro Phase 4 PyGNOME package is shown here; the current Mindoro Phase 4 layer remains OpenDrift/OpenOil scenario context only.",
        "warning",
    )

    render_figure_gallery(
        figures,
        title="Comparator publication figures",
        caption="These figures come from the curated comparator subgroup in the March13-14 final package. Click any figure to enlarge it and read the fuller context there.",
        limit=2 if export_mode else (None if ui_state["advanced"] else 4),
        columns_per_row=1 if export_mode else 2,
        export_mode=export_mode,
        overlay_label="Click to enlarge",
    )

    if export_mode:
        render_table(
            "Comparator ranking",
            comparator_ranking,
            download_name="march13_14_reinit_crossmodel_model_ranking.csv",
            caption="Curated thesis-facing ranking table for the same-case March 14 comparator lane after removing the archived R0 row.",
            height=260,
            export_mode=export_mode,
        )
        render_table(
            "Comparator summary",
            comparator_summary,
            download_name="march13_14_reinit_crossmodel_summary.csv",
            caption="Curated thesis-facing summary table for the Mindoro comparator subgroup after removing the archived R0 row.",
            height=260,
            export_mode=export_mode,
        )
    else:
        left, right = st.columns(2)
        with left:
            render_table(
                "Comparator ranking",
                comparator_ranking,
                download_name="march13_14_reinit_crossmodel_model_ranking.csv",
                caption="Curated thesis-facing ranking table for the same-case March 14 comparator lane after removing the archived R0 row.",
                height=260,
            )
        with right:
            render_table(
                "Comparator summary",
                comparator_summary,
                download_name="march13_14_reinit_crossmodel_summary.csv",
                caption="Curated thesis-facing summary table for the Mindoro comparator subgroup after removing the archived R0 row.",
                height=260,
            )

    if archive_package:
        render_status_callout(
            "Archive note",
            "For the March 13 -> March 14 R0 archived baseline and other R0-including March13-14 comparator outputs, use the Mindoro Validation Archive page.",
            "warning",
        )
        render_package_cards(
            [{**archive_package, "button_label": "Open Mindoro Validation Archive"}],
            columns_per_row=1,
            export_mode=export_mode,
        )

    if ui_state["advanced"] or export_mode:
        render_markdown_block("Mindoro final-package note", state["mindoro_final_readme"], collapsed=True, export_mode=export_mode)
