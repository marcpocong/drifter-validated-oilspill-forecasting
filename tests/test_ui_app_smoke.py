import importlib
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path

try:
    from streamlit.testing.v1 import AppTest
except ImportError:  # pragma: no cover - host environments may not have streamlit installed
    AppTest = None

from ui.data_access import build_dashboard_state


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = REPO_ROOT / "ui" / "app.py"
PAGES_DIR = REPO_ROOT / "ui" / "pages"


def _phase1_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import phase1_recipe_selection

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    phase1_recipe_selection.render(state, panel_state)


def _phase1_wrapper_missing_focused_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import phase1_recipe_selection

    state = build_dashboard_state()
    state["phase1_focused_manifest"] = {}
    state["phase1_focused_recipe_ranking"] = state["phase1_focused_recipe_ranking"].iloc[0:0].copy()
    state["phase1_focused_recipe_summary"] = state["phase1_focused_recipe_summary"].iloc[0:0].copy()
    state["phase1_focused_accepted_segments"] = state["phase1_focused_accepted_segments"].iloc[0:0].copy()
    state["phase1_focused_ranking_subset"] = state["phase1_focused_ranking_subset"].iloc[0:0].copy()
    state["phase1_focused_loading_audit"] = state["phase1_focused_loading_audit"].iloc[0:0].copy()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    phase1_recipe_selection.render(state, panel_state)


def _phase4_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import phase4_oiltype_and_shoreline

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    phase4_oiltype_and_shoreline.render(state, panel_state)


def _home_panel_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import home

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    home.render(state, panel_state)


def _home_advanced_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import home

    state = build_dashboard_state()
    advanced_state = {"advanced": True, "mode_label": "Advanced", "visual_layer": "publication", "export_mode": False}
    home.render(state, advanced_state)


def _home_export_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import home

    state = build_dashboard_state()
    export_state = {"advanced": False, "mode_label": "Export", "visual_layer": "publication", "export_mode": True}
    home.render(state, export_state)


def _phase1_export_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import phase1_recipe_selection

    state = build_dashboard_state()
    export_state = {"advanced": False, "mode_label": "Export", "visual_layer": "publication", "export_mode": True}
    phase1_recipe_selection.render(state, export_state)


def _probe_script_style_import(path: Path) -> subprocess.CompletedProcess[str]:
    code = textwrap.dedent(
        f"""
        import importlib.util
        import pathlib
        import sys

        repo_root = pathlib.Path(r"{REPO_ROOT}").resolve()
        target = pathlib.Path(r"{path}").resolve()
        script_dir = target.parent.resolve()
        sys.path = [str(script_dir)] + [
            entry
            for entry in sys.path
            if entry not in ("", str(repo_root), str(script_dir))
        ]
        spec = importlib.util.spec_from_file_location("streamlit_script_probe", str(target))
        module = importlib.util.module_from_spec(spec)
        if spec.loader is None:
            raise RuntimeError("Unable to load module spec")
        spec.loader.exec_module(module)
        print("IMPORT_OK")
        """
    )
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(REPO_ROOT),
        text=True,
        capture_output=True,
        check=False,
    )


class UiImportBootstrapTests(unittest.TestCase):
    def test_package_imports_work(self):
        importlib.import_module("ui.app")
        importlib.import_module("ui.data_access")

    def test_page_modules_import_through_package(self):
        for page_path in sorted(PAGES_DIR.glob("*.py")):
            if page_path.name in {"__init__.py", "common.py"}:
                continue
            with self.subTest(page=page_path.name):
                importlib.import_module(f"ui.pages.{page_path.stem}")

    def test_app_bootstrap_supports_streamlit_script_style_import(self):
        result = _probe_script_style_import(APP_PATH)
        self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)
        self.assertIn("IMPORT_OK", result.stdout)

    def test_page_bootstrap_supports_streamlit_script_style_import(self):
        for page_path in sorted(PAGES_DIR.glob("*.py")):
            if page_path.name in {"__init__.py", "common.py"}:
                continue
            with self.subTest(page=page_path.name):
                result = _probe_script_style_import(page_path)
                self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)
                self.assertIn("IMPORT_OK", result.stdout)


@unittest.skipIf(AppTest is None, "streamlit.testing is not available")
class UiAppSmokeTests(unittest.TestCase):
    def test_app_home_renders_without_exceptions(self):
        at = AppTest.from_file(str(APP_PATH), default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        titles = [element.value for element in at.title]
        self.assertIn("Home / Overview", titles)
        self.assertEqual(len(at.sidebar.selectbox), 1)
        self.assertEqual(at.sidebar.selectbox[0].label, "Visual layer")

    def test_pages_render_via_wrapper_functions(self):
        for wrapper, expected_title in (
            (_home_panel_wrapper_for_test, "Home / Overview"),
            (_home_advanced_wrapper_for_test, "Home / Overview"),
            (_phase1_wrapper_for_test, "Phase 1 Recipe Selection"),
            (_phase4_wrapper_for_test, "Phase 4 Oil-Type and Shoreline Context"),
            (_home_export_wrapper_for_test, "Home / Overview"),
            (_phase1_export_wrapper_for_test, "Phase 1 Recipe Selection"),
        ):
            at = AppTest.from_function(wrapper, default_timeout=60)
            at.run()
            self.assertFalse(at.exception, msg=f"Wrapper page raised for {expected_title}")
            titles = [element.value for element in at.title]
            self.assertIn(expected_title, titles)

    def test_home_panel_gallery_uses_hover_preview_without_dropdown(self):
        at = AppTest.from_function(_home_panel_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        selectbox_labels = [element.label for element in at.selectbox]
        self.assertNotIn("Featured figure", selectbox_labels)
        button_labels = [element.label for element in at.button]
        self.assertNotIn("Enlarge figure", button_labels)
        expected_count = len(build_dashboard_state(REPO_ROOT)["curated_recommended_figures"])
        download_buttons = [element for element in at.download_button if element.label == "Download PNG"]
        self.assertEqual(len(download_buttons), expected_count)
        text_blocks = " ".join(element.value for element in at.markdown)
        self.assertNotIn("keyboard_double", text_blocks)

    def test_home_advanced_gallery_matches_panel_gallery_count(self):
        at = AppTest.from_function(_home_advanced_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        selectbox_labels = [element.label for element in at.selectbox]
        self.assertNotIn("Featured figure", selectbox_labels)
        button_labels = [element.label for element in at.button]
        self.assertNotIn("Enlarge figure", button_labels)
        expected_count = len(build_dashboard_state(REPO_ROOT)["curated_recommended_figures"])
        download_buttons = [element for element in at.download_button if element.label == "Download PNG"]
        self.assertEqual(len(download_buttons), expected_count)

    def test_export_mode_renders_note_cards_without_sidebar_controls(self):
        at = AppTest.from_function(_home_export_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        text_blocks = " ".join(element.value for element in at.markdown)
        self.assertIn("Export mode converts the dashboard into a print-friendly snapshot", text_blocks)
        button_labels = [element.label for element in at.button]
        self.assertNotIn("Enlarge figure", button_labels)

    def test_phase1_page_degrades_gracefully_when_focused_artifacts_are_missing(self):
        at = AppTest.from_function(_phase1_wrapper_missing_focused_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        info_text = " ".join(element.value for element in at.warning)
        self.assertIn("falls back to the broader regional reference artifacts", info_text)


if __name__ == "__main__":
    unittest.main()
