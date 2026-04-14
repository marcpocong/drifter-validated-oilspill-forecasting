"""Home / Overview page."""

from __future__ import annotations

import html
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
    render_study_structure_cards,
)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    render_page_intro(
        "Home / Overview",
        "This is a polished read-only launch surface over the current curated outputs. Start with Phase 1 for recipe provenance, move to Mindoro B1 for the March 13 -> March 14 R1 primary validation row, then use the comparator, archive, DWH, Phase 4, and legacy pages as clearly labeled support or provenance lanes.",
        badge="Read-only thesis dashboard | curated packages first",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode converts the dashboard into a print-friendly snapshot of the current curated packages.",
                "This view hides the sidebar and interactive controls, keeps only the publication-first layer, and limits the page to a small set of featured figures for PDF export.",
            ]
        )

    recommended = state.get("home_featured_publication_figures", state["curated_recommended_figures"])
    guide_cards = [
        {
            "title": "Start with provenance",
            "body": "Phase 1 shows the focused drifter-based recipe check and explains how Mindoro B1 inherits that recipe without directly ingesting drifters inside Phase 3B.",
        },
        {
            "title": "Treat B1 as the main result",
            "body": "Mindoro B1 is the only main Mindoro validation claim. Track A stays comparator-only, while the March 13 -> March 14 R0 archived baseline and the preserved March-family rows now live on the Mindoro Validation Archive page.",
        },
        {
            "title": "Keep the lanes separate",
            "body": "DWH is a separate Phase 3C transfer-validation lane, Mindoro Phase 4 is context only, the archive page is provenance-only, and the Legacy 2016 package remains support-only.",
        },
    ]

    st.subheader("How to read this dashboard")
    st.caption("Use the pages in this order for the shortest defense-ready story.")
    guide_columns_per_row = 1 if export_mode else 3
    for start in range(0, len(guide_cards), guide_columns_per_row):
        visible_columns = min(guide_columns_per_row, len(guide_cards) - start)
        columns = st.columns(visible_columns)
        for column, card in zip(columns, guide_cards[start : start + guide_columns_per_row]):
            with column:
                with st.container(border=not export_mode):
                    st.markdown(
                        (
                            "<div class='home-guide-card'>"
                            f"<div class='home-guide-card__title'>{html.escape(card['title'])}</div>"
                            f"<div class='home-guide-card__body'>{html.escape(card['body'])}</div>"
                            "</div>"
                        ),
                        unsafe_allow_html=True,
                    )

    st.subheader("Study structure")
    st.caption("Each card introduces one thesis lane in plain language and links straight to the page that presents the current packaged evidence.")
    render_study_structure_cards(
        [
            {
                "title": "Phase 1 Recipe Selection",
                "classification": "Study setup",
                "body": "Historical drifter segments were used to compare forcing recipes and select the Mindoro recipe before the main validation row was discussed.",
                "note": "Main provenance layer for the B1 recipe choice.",
                "page_label": "Phase 1 Recipe Selection",
            },
            {
                "title": "Mindoro Primary Validation",
                "classification": "Main thesis result",
                "body": "The March 13 -> March 14 R1 primary validation row is the only thesis-facing Mindoro observation-based validation claim.",
                "note": "Main paper uses this Mindoro row only.",
                "page_label": "Mindoro B1 Primary Validation",
            },
            {
                "title": "Mindoro Comparator",
                "classification": "Support comparison",
                "body": "Track A compares OpenDrift and PyGNOME on the same March 14 target, but it remains comparator-only and excludes archived R0-only outputs from the thesis-facing view.",
                "note": "PyGNOME is never shown as truth here.",
                "page_label": "Mindoro Cross-Model Comparator",
            },
            {
                "title": "Mindoro Validation Archive",
                "classification": "Archive / provenance",
                "body": "Archived March13-14 R0 baseline, older R0-including March13-14 outputs, and preserved March-family legacy rows are centralized here for provenance, audit, and reproducibility.",
                "note": "Not thesis-facing evidence; main paper uses March 13 -> March 14 R1 only.",
                "page_label": "Mindoro Validation Archive",
            },
            {
                "title": "DWH Transfer Validation",
                "classification": "Main discussion result",
                "body": "DWH is a separate transfer-validation lane with C1 deterministic, C2 ensemble extension, and C3 comparator-only semantics.",
                "note": "No drifter baseline is used for DWH.",
                "page_label": "DWH Phase 3C Transfer Validation",
            },
            {
                "title": "Phase 4 Oil-Type and Shoreline",
                "classification": "Technical context",
                "body": "Mindoro Phase 4 provides the current OpenDrift/OpenOil scenario context for oil-type and shoreline interpretation.",
                "note": "No matched Mindoro Phase 4 PyGNOME package is currently shown.",
                "page_label": "Phase 4 Oil-Type and Shoreline Context",
            },
            {
                "title": "Legacy 2016 Support",
                "classification": "Legacy support",
                "body": "The 2016 package preserves the legacy support flow and now includes a budget-only deterministic PyGNOME Phase 4 pilot.",
                "note": "This lane remains support-only and is not main validation evidence.",
                "page_label": "Legacy 2016 Support Package",
            },
        ],
        columns_per_row=2 if export_mode else 3,
        export_mode=export_mode,
    )

    quick_link_ids = {
        "mindoro_b1_final",
        "mindoro_comparator",
        "mindoro_validation_archive",
        "dwh_phase3c_final",
        "phase4_context_status",
        "legacy_2016_final",
    }
    quick_links = []
    for package in state.get("curated_package_roots", []):
        if package.get("package_id") not in quick_link_ids:
            continue
        quick_links.append({**package, "button_label": "Open page"})

    st.subheader("Quick links")
    st.caption("These shortcuts point to the main curated packages and pages rather than raw case folders.")
    render_package_cards(
        quick_links,
        columns_per_row=1 if export_mode else 3,
        export_mode=export_mode,
    )
    st.caption("For manifests, logs, and synced registries, use the Artifacts / Logs / Registries page.")

    render_figure_gallery(
        recommended,
        title="Featured publication figures",
        caption="Home keeps the requested top-down overview sequence visible in live browsing: Legacy 2016 support triptychs first, then the Mindoro March 13-14 final-validation figures, then the DWH horizon-run comparison boards. Click any figure to open a larger preview; export mode stays smaller and static for cleaner PDF snapshots.",
        limit=2 if export_mode else None,
        columns_per_row=1 if export_mode else 2,
        export_mode=export_mode,
        overlay_label="Click to enlarge",
    )

    if ui_state["advanced"] and not export_mode:
        st.subheader("Advanced notes")
        st.caption("Panel mode stops here. Advanced mode keeps the lower-level reproducibility notes available in read-only form.")
        render_markdown_block("Final reproducibility summary", state["final_reproducibility_summary"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Publication talking points", state["publication_talking_points"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Publication captions", state["publication_captions"], collapsed=True, export_mode=export_mode)
