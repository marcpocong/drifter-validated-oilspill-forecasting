"""Read-only local dashboard for the thesis workflow outputs."""

from __future__ import annotations

import base64
from pathlib import Path

try:
    from ui.bootstrap import discover_branding_assets, ensure_repo_root_on_path
except ModuleNotFoundError:
    import sys

    _UI_DIR = Path(__file__).resolve().parent
    _UI_DIR_TEXT = str(_UI_DIR)
    if _UI_DIR_TEXT not in sys.path:
        sys.path.insert(0, _UI_DIR_TEXT)
    from bootstrap import discover_branding_assets, ensure_repo_root_on_path

ensure_repo_root_on_path(__file__)

import streamlit as st
from streamlit import config as st_config
from PIL import Image

from ui.data_access import build_dashboard_state
from ui.pages import PageDefinition, visible_page_definitions


APP_TITLE = "Drifter-Validated Oil Spill Forecasting Dashboard"
APP_SUBTITLE = "Read-only thesis dashboard over the curated final packages, publication figures, and synced registries."
SIDEBAR_SUBTITLE = "Read-only thesis dashboard"


LAYER_LABELS = {
    "publication": "Publication package",
    "panel": "Panel gallery",
    "raw": "Raw technical gallery",
}


@st.cache_data(show_spinner=False)
def _load_dashboard_state() -> dict:
    return build_dashboard_state()


def _branding_payload() -> tuple[dict, Image.Image | None]:
    branding = discover_branding_assets(__file__)
    page_icon = None
    page_icon_path = branding.get("page_icon_path")
    if page_icon_path:
        try:
            page_icon = Image.open(page_icon_path)
        except OSError:
            page_icon = None
    return branding, page_icon


def _load_css() -> None:
    css_path = Path(__file__).resolve().parent / "assets" / "style.css"
    if css_path.exists():
        font_css = _inline_font_face_css()
        st.markdown(f"<style>{font_css}\n{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


def _asset_mime_type(path: Path) -> str:
    suffix = path.suffix.lower()
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".svg": "image/svg+xml",
        ".woff2": "font/woff2",
        ".woff": "font/woff",
        ".ttf": "font/ttf",
        ".otf": "font/otf",
    }.get(suffix, "application/octet-stream")


def _asset_data_uri(path: str | Path) -> str:
    asset_path = Path(path).resolve()
    encoded = base64.b64encode(asset_path.read_bytes()).decode("ascii")
    return f"data:{_asset_mime_type(asset_path)};base64,{encoded}"


def _inline_font_face_css() -> str:
    fonts_dir = Path(__file__).resolve().parent / "assets" / "fonts"
    font_specs = [
        ("Ideal Sans", "IdealSans-Regular", 400, "normal"),
        ("Ideal Sans", "IdealSans-Bold", 700, "normal"),
    ]
    extension_map = {
        ".woff2": "woff2",
        ".woff": "woff",
        ".ttf": "truetype",
        ".otf": "opentype",
    }
    blocks: list[str] = []
    for family_name, filename_stem, weight, style in font_specs:
        for extension, format_name in extension_map.items():
            candidate = fonts_dir / f"{filename_stem}{extension}"
            if not candidate.exists():
                continue
            blocks.append(
                (
                    "@font-face {"
                    f"font-family: '{family_name}';"
                    f"src: url('{_asset_data_uri(candidate)}') format('{format_name}');"
                    f"font-weight: {weight};"
                    f"font-style: {style};"
                    "font-display: swap;"
                    "}"
                )
            )
            break
    return "\n".join(blocks)


def _render_sidebar_branding(branding: dict) -> None:
    if branding.get("has_logo") and branding.get("logo_path"):
        logo_uri = _asset_data_uri(str(branding["logo_path"]))
        st.markdown(
            (
                "<div class='sidebar-brand'>"
                f"<img class='sidebar-brand__logo' src='{logo_uri}' alt='Drifter Validated Oil Spill Tracker' />"
                f"<div class='sidebar-brand__subtitle'>{SIDEBAR_SUBTITLE}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            (
                "<div class='sidebar-brand sidebar-brand--fallback'>"
                "<div class='sidebar-brand__title'>Drifter-Validated Dashboard</div>"
                f"<div class='sidebar-brand__subtitle'>{SIDEBAR_SUBTITLE}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )


def _truthy_query_param(value: object) -> bool:
    if isinstance(value, list):
        return any(_truthy_query_param(item) for item in value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "print", "pdf"}


def _export_mode_from_query_params(query_params: object) -> bool:
    try:
        value = query_params.get("export")
    except Exception:
        return False
    return _truthy_query_param(value)


def _load_export_css() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"],
        [data-testid="stSidebarNav"],
        [data-testid="stToolbar"],
        [data-testid="stStatusWidget"],
        button[kind],
        [data-testid="stDownloadButton"],
        [data-testid="stExpanderToggleIcon"] {
          display: none !important;
        }
        .main .block-container {
          max-width: 1180px !important;
          padding-top: 1rem !important;
          padding-left: 1.25rem !important;
          padding-right: 1.25rem !important;
        }
        .hero-card,
        .page-hero,
        .export-note {
          break-inside: avoid;
          page-break-inside: avoid;
        }
        .stTabs [data-baseweb="tab-list"] {
          display: none !important;
        }
        .stCode pre {
          white-space: pre-wrap !important;
          word-break: break-word !important;
        }
        @media print {
          [data-testid="stSidebar"],
          [data-testid="stSidebarNav"],
          [data-testid="stToolbar"],
          [data-testid="stStatusWidget"],
          button[kind],
          [data-testid="stDownloadButton"] {
            display: none !important;
          }
          .main .block-container {
            max-width: 100% !important;
            padding: 0.5rem 0.75rem !important;
          }
          h1, h2, h3, h4, img, .hero-card, .page-hero, .export-note {
            break-inside: avoid;
            page-break-inside: avoid;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_sidebar_controls(state: dict, branding: dict) -> dict:
    with st.sidebar:
        _render_sidebar_branding(branding)
        st.markdown("---")

        mode_label = st.radio(
            "Viewing mode",
            options=["Panel-friendly", "Advanced"],
            index=0,
            key="view_mode_selector",
        )
        advanced = mode_label == "Advanced"
        layer_options = ["publication"] if not advanced else ["publication", "panel", "raw"]
        visual_layer = st.selectbox(
            "Visual layer",
            options=layer_options,
            format_func=lambda value: LAYER_LABELS[value],
            index=0,
            key="visual_layer_selector",
        )

        st.markdown("---")
        st.caption("Read-only scope")
        st.markdown(
            "\n".join(
                [
                    "- Read-only only; no scientific rerun controls are exposed here",
                    "- Curated final packages are the primary browse surfaces",
                    "- Publication figures stay the default layer",
                    "- Raw CASE_* folders remain advanced-only fallback context",
                    "- Support and comparator lanes stay labeled as support and comparator lanes",
                ]
            )
        )

        curated_packages = state.get("curated_package_roots", [])
        st.metric("Curated package roots", len(curated_packages))
        st.metric("Publication figures indexed", len(state["publication_registry"]))
        st.metric("Focused Phase 1 recipes tested", len(state["phase1_focused_recipe_summary"]))

        with st.expander("Read paths", expanded=False):
            read_paths = [
                "output/phase1_mindoro_focus_pre_spill_2016_2023/",
                "output/Phase 3B March13-14 Final Output/",
                "output/Phase 3C DWH Final Output/",
                "output/2016 Legacy Runs FINAL Figures/",
                "output/final_validation_package/",
                "output/final_reproducibility_package/",
                "output/figure_package_publication/",
                "output/phase4/CASE_MINDORO_RETRO_2023/",
                "output/phase4_crossmodel_comparability_audit/",
            ]
            if advanced:
                read_paths.extend(
                    [
                        "output/trajectory_gallery_panel/",
                        "output/trajectory_gallery/",
                        "output/CASE_MINDORO_RETRO_2023/",
                        "output/CASE_DWH_RETRO_2010_72H/",
                    ]
                )
            st.code("\n".join(read_paths), language="text")

    return {
        "advanced": advanced,
        "mode_label": mode_label,
        "visual_layer": visual_layer,
        "export_mode": False,
    }


def _render_sidebar_navigation(ui_state: dict) -> None:
    if ui_state["export_mode"]:
        return
    page_sections = st.session_state.get("_ui_nav_pages_by_section", {})
    if not page_sections:
        return
    with st.sidebar:
        st.markdown("---")
        st.caption("Pages")
        for section, page_entries in page_sections.items():
            st.markdown(f"**{section}**")
            for entry in page_entries:
                st.page_link(
                    entry["page"],
                    label=entry["label"],
                    help=entry.get("page_id"),
                    use_container_width=True,
                )


def _render_page_wrapper(page_definition: PageDefinition, state: dict, ui_state: dict):
    def _run() -> None:
        try:
            page_definition.renderer(state, ui_state)
        except Exception as exc:
            if ui_state["advanced"]:
                raise
            st.warning(
                "This page could not load one of its optional packaged artifacts. The dashboard is staying in read-only mode and the other pages remain available."
            )
            st.caption(f"Panel-mode detail: {exc}")

    return _run


def _build_navigation(state: dict, ui_state: dict):
    page_definitions = visible_page_definitions(state, advanced=ui_state["advanced"])
    sections: dict[str, list] = {}
    page_objects_by_label: dict[str, object] = {}
    page_objects_by_section: dict[str, list[dict[str, object]]] = {}
    for index, definition in enumerate(page_definitions):
        page_object = st.Page(
            _render_page_wrapper(definition, state, ui_state),
            title=definition.label,
            url_path=definition.url_path or definition.page_id,
            default=index == 0,
        )
        sections.setdefault(definition.navigation_section, []).append(page_object)
        page_objects_by_label[definition.label] = page_object
        page_objects_by_section.setdefault(definition.navigation_section, []).append(
            {
                "label": definition.label,
                "page": page_object,
                "page_id": definition.page_id,
            }
        )
    st.session_state["_ui_nav_pages_by_label"] = page_objects_by_label
    st.session_state["_ui_nav_pages_by_section"] = page_objects_by_section
    return st.navigation(sections, position="hidden", expanded=True)


def main() -> None:
    Image.MAX_IMAGE_PIXELS = None
    st_config.set_option("client.showSidebarNavigation", False)
    branding, page_icon = _branding_payload()
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _load_css()
    export_mode = _export_mode_from_query_params(st.query_params)
    if export_mode:
        _load_export_css()
    state = _load_dashboard_state()
    ui_state = _render_sidebar_controls(state, branding) if not export_mode else {
        "advanced": False,
        "mode_label": "Export",
        "visual_layer": "publication",
        "export_mode": True,
    }
    navigation = _build_navigation(state, ui_state)
    _render_sidebar_navigation(ui_state)

    st.markdown(
        """
        <div class="hero-card">
          <div class="hero-kicker">Read-only thesis dashboard</div>
          <div class="hero-title">Study structure first, curated packages first, science reruns never</div>
          <div class="hero-text">The dashboard leads with Phase 1 recipe selection, Mindoro B1 primary validation, the Mindoro comparator package, the frozen DWH Phase 3C package, the Mindoro Phase 4 context layer, and the curated legacy 2016 support package. Advanced mode opens registries, manifests, and lower-level figure layers without changing stored outputs.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if export_mode:
        st.info(
            "Print / export mode is active. Navigation chrome, sidebar controls, and interactive-only elements are hidden so this page can be saved cleanly as a PDF."
        )

    navigation.run()


if __name__ == "__main__":
    main()
