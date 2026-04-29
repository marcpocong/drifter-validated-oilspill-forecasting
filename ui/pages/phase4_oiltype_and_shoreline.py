"""Phase 4 oil-type and shoreline page."""

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

from src.core.artifact_status import get_artifact_status
from ui.data_access import figure_subset
from ui.evidence_contract import ROLE_CONTEXT, assert_no_archive_leak, filter_for_page
from ui.pages.common import render_export_note, render_figure_gallery, render_page_intro, render_section_stack, render_status_callout, render_table
from ui.plots import phase4_budget_summary_figure


def _draft22_scenario_registry():
    import pandas as pd

    return pd.DataFrame(
        [
            {"Scenario ID": "lighter_oil", "Oil label": "Light oil", "Category": "light_distillate", "Density": "850 kg/m3", "Methodological role": "Light-end representative scenario"},
            {"Scenario ID": "fixed_base_medium_heavy_proxy", "Oil label": "Fixed base medium-heavy proxy", "Category": "medium_heavy_proxy", "Density": "930 kg/m3", "Methodological role": "Fixed base marine-fuel proxy / comparison anchor"},
            {"Scenario ID": "heavier_oil", "Oil label": "Heavier oil", "Category": "heavy_residual", "Density": "990 kg/m3", "Methodological role": "Heavier persistent residual-fuel scenario"},
        ]
    )


def _draft22_budget_summary():
    import pandas as pd

    return pd.DataFrame(
        [
            {"Scenario": "Light oil", "Evap. %": "0.86%", "Disp. %": "99.12%", "Beached %": "0.02%", "Total beached kg from 50-tonne basis": "10.000 kg", "First arrival": "4 h", "Impacted segments": "11", "QC": "Pass"},
            {"Scenario": "Fixed-base medium-heavy proxy", "Evap. %": "0.31%", "Disp. %": "99.08%", "Beached %": "0.61%", "Total beached kg from 50-tonne basis": "305.000 kg", "First arrival": "4 h", "Impacted segments": "10", "QC": "Flagged"},
            {"Scenario": "Heavier oil", "Evap. %": "0.23%", "Disp. %": "99.15%", "Beached %": "0.63%", "Total beached kg from 50-tonne basis": "315.000 kg", "First arrival": "4 h", "Impacted segments": "11", "QC": "Pass"},
        ]
    )


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    oil_status = get_artifact_status("mindoro_phase4_oil_budget")
    shoreline_status = get_artifact_status("mindoro_phase4_shoreline")

    render_page_intro(
        "Mindoro Oil-Type and Shoreline Context",
        "Mindoro oil-type and shoreline outputs are downstream support/context for operational consequences. They are not a primary validation phase.",
        badge=ROLE_CONTEXT,
    )

    if export_mode:
        render_export_note(
            [
                "Export mode keeps the Mindoro Phase 4 story on one print-friendly page.",
                "The current package remains OpenDrift/OpenOil-only context, and no matched PyGNOME Phase 4 comparator is shown here.",
            ]
        )

    render_status_callout(
        "Support/context only; not a primary validation phase.",
        "These outputs show consequence sensitivity under the retained transport setup, but they do not add a second Mindoro validation claim and do not imply a matched Mindoro OpenDrift–PyGNOME fate-and-shoreline comparison.",
        "warning",
    )
    render_status_callout(
        "Comparator availability",
        "No matched Mindoro Phase 4 PyGNOME fate-and-shoreline comparison is packaged yet.",
        "warning",
    )

    render_status_callout(
        "Follow-up note",
        "The fixed base medium-heavy proxy remains flagged for follow-up because of the recorded mass-balance tolerance exceedance.",
        "warning",
    )
    render_status_callout(
        "Scenario score summary",
        "Beached fractions are 0.02%, 0.61%, and 0.63%; first shoreline arrival is 4 h for all scenarios; impacted segments are 11 / 10 / 11; QC is Pass / Flagged / Pass.",
        "info",
    )
    render_status_callout(
        "Deferred-comparison note",
        "Any PyGNOME fate material in this repo belongs to archive or legacy support. It does not create matched Mindoro fate-and-shoreline evidence.",
        "info",
    )

    figures = filter_for_page(
        figure_subset(
            ui_state["visual_layer"],
            case_id="CASE_MINDORO_RETRO_2023",
            status_keys=[oil_status.key, shoreline_status.key],
        ),
        "phase4_oiltype_and_shoreline",
        advanced=bool(ui_state.get("advanced")),
    )
    assert_no_archive_leak(figures, "phase4_oiltype_and_shoreline", advanced=bool(ui_state.get("advanced")))

    st.pyplot(phase4_budget_summary_figure(state["phase4_budget_summary"]), width="stretch")
    st.caption("Stored budget summary chart from the packaged Mindoro Phase 4 bundle.")

    def _publication_figures() -> None:
        render_figure_gallery(
            figures,
            title="Mindoro Phase 4 figures",
            caption="Panel-friendly mode now shows the oil-budget and shoreline boards directly as a gallery. Click any figure to enlarge it and read the fuller context there.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 5),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _budget_tables() -> None:
        render_table(
            "Draft 22 scenario registry",
            _draft22_scenario_registry(),
            download_name="draft22_mindoro_oiltype_scenarios.csv",
            caption="Scenario roles and densities used for Draft 22 wording.",
            height=190,
            export_mode=export_mode,
        )
        render_table(
            "Draft 22 oil-budget summary",
            _draft22_budget_summary(),
            download_name="draft22_mindoro_oil_budget_summary.csv",
            caption="Support/context consequence summary; not primary validation.",
            height=190,
            export_mode=export_mode,
        )
        if ui_state["advanced"]:
            render_table(
                "Stored Phase 4 oil-budget registry",
                state["phase4_budget_summary"],
                download_name="phase4_oil_budget_summary.csv",
                caption="Advanced read-only package table. Draft 22 summary values above are the panel-facing wording authority.",
                height=250,
                export_mode=export_mode,
            )
            render_table(
                "Stored Phase 4 oil-type comparison",
                state["phase4_oiltype_comparison"],
                download_name="phase4_oiltype_comparison.csv",
                caption="Advanced read-only delta-versus-anchor table from the stored bundle.",
                height=220,
                export_mode=export_mode,
            )

    def _shoreline_tables() -> None:
        render_table(
            "Phase 4 shoreline arrival summary",
            state["phase4_shoreline_arrival"],
            download_name="phase4_shoreline_arrival.csv",
            caption="Stored first-arrival summary per scenario.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "Phase 4 shoreline segment impacts",
            state["phase4_shoreline_segments"],
            download_name="phase4_shoreline_segments.csv",
            caption="Stored shoreline segment impact table; advanced mode can be used to inspect the full per-segment rows.",
            height=300,
            max_rows=None if ui_state["advanced"] else 25,
            export_mode=export_mode,
        )

    render_section_stack(
        [
            ("Publication figures", _publication_figures),
            ("Budget tables", _budget_tables),
            ("Shoreline tables", _shoreline_tables),
        ],
        export_mode=export_mode,
        use_tabs=ui_state["advanced"],
    )
