"""Artifacts and logs page."""

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

from ui.evidence_contract import ROLE_ADVANCED
from ui.pages.common import (
    preview_artifact,
    render_export_note,
    render_feature_grid,
    render_key_takeaway,
    render_markdown_block,
    render_metric_row,
    render_modern_hero,
    render_section_header,
    render_section_stack,
    render_table,
)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    render_modern_hero(
        "Reproducibility / Governance / Audit",
        "Technical audit page for synced reproducibility indexes, panel-result verification outputs, registries, manifests, and logs.",
        badge=ROLE_ADVANCED,
        eyebrow="Read-only technical audit",
        meta=["Registries", "Manifests", "Logs", "No writes"],
        tone="readonly",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode keeps this page high-level and sequential so the PDF captures the main registries without interactive previews.",
                "Artifact previews and log inspection stay available only in the live read-only dashboard.",
            ]
        )

    render_key_takeaway(
        "This page is for audit, not thesis storytelling.",
        "Panel-friendly mode keeps raw artifact, manifest, and log previews out of the main story. Panel-friendly mode keeps registries compact; Advanced mode exposes raw catalogs, manifests, logs, and read-only artifact previews.",
        tone="advanced",
        badge=ROLE_ADVANCED,
    )

    catalog = state["final_output_catalog"]
    case_registry = state["final_case_registry"]
    manifests = state["final_manifest_index"]
    logs = state["final_log_index"]
    panel_review_check = state["panel_review_check_table"]
    launcher_entries = state.get("launcher_matrix", {}).get("entries", [])
    archive_items = state.get("archive_registry", {}).get("archive_items", [])
    paper_output_entries = state.get("paper_to_output_registry", {}).get("entries", [])

    def _package_overview() -> None:
        render_metric_row(
            [
                ("Case registry rows", str(len(case_registry))),
                ("Output catalog rows", str(len(catalog))),
                ("Manifest index rows", str(len(manifests))),
                ("Log index rows", str(len(logs))),
                ("Launcher entries", str(len(launcher_entries))),
                ("Archive registry items", str(len(archive_items))),
                ("Paper-output registry rows", str(len(paper_output_entries))),
            ],
            export_mode=export_mode,
        )
        render_feature_grid(
            [
                {
                    "title": "Panel review checks",
                    "body": "Paper-to-output and panel review verification artifacts stay readable without overwhelming the main story.",
                    "badge": ROLE_ADVANCED,
                    "tone": "advanced",
                },
                {
                    "title": "Raw registries",
                    "body": "Output catalogs, manifest indexes, and log indexes are tucked into Advanced expanders for direct inspection.",
                    "badge": ROLE_ADVANCED,
                    "tone": "advanced",
                },
                {
                    "title": "Config-backed routing",
                    "body": "Launcher, archive, and paper-to-output registries are loaded from config files and exposed here as governance context.",
                    "badge": ROLE_ADVANCED,
                    "tone": "readonly",
                },
                {
                    "title": "Read-only previews",
                    "body": "Artifact previews never rerun workflows or write back to stored outputs.",
                    "badge": ROLE_ADVANCED,
                    "tone": "readonly",
                },
            ],
            columns_per_row=4,
            export_mode=export_mode,
        )

    def _panel_review() -> None:
        render_table(
            "Panel review result check",
            panel_review_check,
            download_name="panel_results_match_check.csv",
            caption="When present, this table shows the latest read-only manuscript-to-output verification written to output/panel_review_check/.",
            height=280,
            max_rows=None if ui_state["advanced"] else 25,
            export_mode=export_mode,
        )
        render_markdown_block(
            "Paper-to-output registry",
            state["paper_output_registry_markdown"],
            collapsed=not ui_state["advanced"],
            export_mode=export_mode,
        )
        render_markdown_block(
            "Panel review report",
            state["panel_review_check_markdown"],
            collapsed=True,
            export_mode=export_mode,
        )
        if ui_state["advanced"] and not export_mode:
            preview_options = [
                "docs/PAPER_OUTPUT_REGISTRY.md",
                "output/panel_review_check/panel_results_match_check.md",
                "output/panel_review_check/panel_review_manifest.json",
            ]
            selected = st.selectbox("Preview panel artifact", preview_options, key="panel_artifact_preview")
            preview_artifact(selected)

    def _case_registry() -> None:
        render_table(
            "Final case registry",
            case_registry,
            download_name="final_case_registry.csv",
            caption="This registry exposes the main workflow lanes and their authoritative curated package roots.",
            height=240,
            max_rows=None if ui_state["advanced"] else 18,
            export_mode=export_mode,
        )
        if ui_state["advanced"] and not export_mode and not case_registry.empty:
            selected = st.selectbox(
                "Preview primary output root",
                case_registry["primary_output_root"].astype(str).tolist(),
                key="case_root_preview",
            )
            preview_artifact(selected)

    def _output_catalog() -> None:
        if export_mode:
            render_table(
                "Final output catalog",
                catalog,
                download_name="final_output_catalog.csv",
                caption="Compact export preview of the Phase 5 synced output catalog.",
                height=240,
                max_rows=25,
                export_mode=export_mode,
            )
        else:
            with st.expander("Open raw final output catalog", expanded=False):
                render_table(
                    "Final output catalog",
                    catalog,
                    download_name="final_output_catalog.csv",
                    caption="This catalog is regenerated by the Phase 5 sync and is the safest way to see what artifact groups exist right now.",
                    height=260,
                    max_rows=None if ui_state["advanced"] else 40,
                    export_mode=export_mode,
                )
        if ui_state["advanced"] and not export_mode and not catalog.empty:
            selected = st.selectbox("Preview output artifact", catalog["relative_path"].astype(str).tolist(), key="catalog_preview")
            preview_artifact(selected)

    def _manifest_index() -> None:
        if export_mode:
            render_table(
                "Final manifest index",
                manifests,
                download_name="final_manifest_index.csv",
                caption="Compact export preview of synced manifest inventory.",
                height=240,
                max_rows=25,
                export_mode=export_mode,
            )
        else:
            with st.expander("Open raw final manifest index", expanded=False):
                render_table(
                    "Final manifest index",
                    manifests,
                    download_name="final_manifest_index.csv",
                    caption="Manifest inventory pulled from the synced final reproducibility package.",
                    height=260,
                    max_rows=None if ui_state["advanced"] else 40,
                    export_mode=export_mode,
                )
        if ui_state["advanced"] and not export_mode and not manifests.empty:
            selected = st.selectbox("Preview manifest", manifests["relative_path"].astype(str).tolist(), key="manifest_preview")
            preview_artifact(selected)

    def _log_index() -> None:
        if export_mode:
            render_table(
                "Final log index",
                logs,
                download_name="final_log_index.csv",
                caption="Compact export preview of synced logs.",
                height=220,
                max_rows=20,
                export_mode=export_mode,
            )
        else:
            with st.expander("Open raw final log index", expanded=False):
                render_table(
                    "Final log index",
                    logs,
                    download_name="final_log_index.csv",
                    caption="Log preview stays read-only. Missing optional logs are tolerated.",
                    height=240,
                    max_rows=None if ui_state["advanced"] else 30,
                    export_mode=export_mode,
                )
        if ui_state["advanced"] and not export_mode and not logs.empty:
            selected = st.selectbox("Preview log", logs["relative_path"].astype(str).tolist(), key="log_preview")
            preview_artifact(selected)

    def _package_notes() -> None:
        render_markdown_block("Final reproducibility summary", state["final_reproducibility_summary"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Publication captions", state["publication_captions"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Publication talking points", state["publication_talking_points"], collapsed=True, export_mode=export_mode)

    sections = [
        ("Package overview", _package_overview),
        ("Panel review", _panel_review),
        ("Case registry", _case_registry),
        ("Package notes", _package_notes),
    ]
    if ui_state["advanced"] and not export_mode:
        sections[3:3] = [
            ("Output catalog", _output_catalog),
            ("Manifest index", _manifest_index),
            ("Log index", _log_index),
        ]

    render_section_header(
        "Audit Sections",
        "Primary review checks stay visible first; raw registries and logs remain compact, secondary inspection layers.",
        badge=ROLE_ADVANCED,
    )
    render_section_stack(
        sections,
        export_mode=export_mode,
        use_tabs=ui_state["advanced"],
    )
