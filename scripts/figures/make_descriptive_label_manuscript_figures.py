"""Build publication-facing figures with descriptive labels only.

The source scorecards, manifests, and internal run identifiers are intentionally
left untouched. This script reads stored rasters/tables and writes a separate
publication package under output/figure_package_publication/descriptive_labels.
"""

from __future__ import annotations

import json
import textwrap
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from matplotlib.cm import ScalarMappable
from matplotlib.colors import ListedColormap, LinearSegmentedColormap, Normalize
from matplotlib.patches import FancyBboxPatch, Patch, Rectangle
from rasterio.windows import Window, bounds as window_bounds

plt.rcParams.update(
    {
        "font.family": "Arial",
        "font.sans-serif": ["Arial"],
    }
)


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = Path("output") / "figure_package_publication" / "descriptive_labels"
MANIFEST_PATH = OUTPUT_DIR / "descriptive_label_figure_manifest.json"

SEED_MASK = (
    Path("output")
    / "CASE_MINDORO_RETRO_2023"
    / "phase3b_extended_public_scored_march13_14_reinit"
    / "march13_seed_mask_on_grid.tif"
)
TARGET_MASK = (
    Path("output")
    / "CASE_MINDORO_RETRO_2023"
    / "phase3b_extended_public"
    / "accepted_obs_masks"
    / "10b37c42a9754363a5f7b14199b077e6.tif"
)
OPENDRIFT_P50 = (
    Path("output")
    / "CASE_MINDORO_RETRO_2023"
    / "phase3b_extended_public_scored_march13_14_reinit"
    / "R1_previous"
    / "forecast_datecomposites"
    / "mask_p50_2023-03-14_localdate.tif"
)
PYGNOME_DETERMINISTIC = (
    Path("output")
    / "CASE_MINDORO_RETRO_2023"
    / "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison"
    / "tracks"
    / "pygnome_reinit_deterministic"
    / "pygnome_footprint_mask_2023-03-14_localdate.tif"
)
LAND_MASK = Path("data_processed") / "grids" / "land_mask.tif"

PRODUCT_FAMILY_JSON = Path("output") / "chapter5_generated" / "mindoro_product_family_board_march13_14_r1_previous.json"
PRIMARY_BOARD = (
    Path("output")
    / "figure_package_publication"
    / "case_mindoro_retro_2023__phase3b_reinit_primary__opendrift__comparison_board__2023_03_13_to_2023_03_14__board__slide__mindoro_primary_validation_board.png"
)
COMPARATOR_BOARD = (
    Path("output")
    / "figure_package_publication"
    / "case_mindoro_retro_2023__phase3a_reinit_crossmodel__opendrift_vs_pygnome__comparison_board__2023_03_14__board__slide__mindoro_crossmodel_board.png"
)
CROSSMODEL_SUMMARY = (
    Path("output")
    / "Phase 3B March13-14 Final Output"
    / "summary"
    / "comparator_pygnome"
    / "march13_14_reinit_crossmodel_model_ranking.csv"
)
PRIMARY_SUMMARY = (
    Path("output")
    / "Phase 3B March13-14 Final Output"
    / "summary"
    / "opendrift_primary"
    / "march13_14_reinit_summary.csv"
)

INTERNAL_LABELS = {
    "workflow": [
        ("phase1_mindoro_focus_pre_spill_2016_2023", "Focused Mindoro transport-provenance check"),
        ("phase2_official", "Standardized forecast generation"),
        ("B1", "Primary Mindoro March 13-14 public-observation validation"),
        ("Track A", "Same-case OpenDrift-PyGNOME comparator support"),
        ("DWH", "Deepwater Horizon external transfer validation"),
        ("phase4_oiltype_and_shoreline", "Mindoro oil-type and shoreline support/context"),
        ("archive-only / R0 / sensitivity", "Repository-only non-promoted runs retained for audit"),
    ],
    "ensemble": [
        ("branch R1_previous", "Adopted primary Mindoro baseline setup"),
        ("prob_presence", "Ensemble probability surface"),
        ("mask_p50", "p50 footprint"),
        ("mask_p90", "p90 footprint"),
    ],
    "validation": [
        ("March 13 seed mask on grid", "March 13 public seed observation"),
        ("March 14 target mask on grid", "March 14 public target observation"),
        ("Promoted R1 previous reinit overlay", "OpenDrift promoted p50 footprint"),
        ("OpenDrift p50 footprint (= p90 here)", "OpenDrift promoted p50 footprint"),
    ],
    "comparator": [
        ("OpenDrift R1 previous reinit p50", "OpenDrift promoted p50 footprint"),
        ("PyGNOME deterministic March 13 reinit comparator", "PyGNOME deterministic comparator"),
        ("R0_reinit_p50", "Excluded from publication comparator board"),
    ],
    "mean_fss": [
        ("OpenDrift R1 previous reinit p50", "OpenDrift promoted p50 branch"),
        ("PyGNOME deterministic March 13 reinit comparator", "PyGNOME deterministic comparator"),
        ("R0_reinit_p50", "Excluded from publication chart"),
    ],
}

FORBIDDEN_FIGURE_TEXT = (
    "R1_previous",
    "R1 previous",
    "R0",
    "B1",
    "Track A",
    "phase1_mindoro_focus_pre_spill_2016_2023",
    "manuscript",
    "Manuscript",
)


@dataclass
class RasterLayer:
    path: Path
    data: np.ndarray
    transform: Any
    bounds: tuple[float, float, float, float]
    crs: str


def _repo_path(path: Path) -> Path:
    return REPO_ROOT / path


def _relative(path: Path) -> str:
    absolute = path if path.is_absolute() else _repo_path(path)
    try:
        return absolute.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return absolute.resolve().as_posix()


def _load_raster(path: Path) -> RasterLayer:
    absolute = _repo_path(path)
    try:
        with rasterio.open(absolute) as dataset:
            data = dataset.read(1)
            return RasterLayer(
                path=path,
                data=np.asarray(data),
                transform=dataset.transform,
                bounds=(dataset.bounds.left, dataset.bounds.bottom, dataset.bounds.right, dataset.bounds.top),
                crs=str(dataset.crs),
            )
    except Exception as exc:
        raise FileNotFoundError(f"Missing or unreadable required raster: {_relative(path)}") from exc


def _load_optional_raster(path: Path) -> RasterLayer | None:
    try:
        return _load_raster(path)
    except FileNotFoundError:
        return None


def _nonzero_bounds(layer: RasterLayer) -> tuple[float, float, float, float] | None:
    rows, cols = np.where(np.isfinite(layer.data) & (layer.data > 0))
    if len(rows) == 0:
        return None
    row_min = int(rows.min())
    row_max = int(rows.max())
    col_min = int(cols.min())
    col_max = int(cols.max())
    window = Window.from_slices((row_min, row_max + 1), (col_min, col_max + 1))
    return tuple(float(value) for value in window_bounds(window, layer.transform))


def _union_bounds(layers: list[RasterLayer], pad_fraction: float = 0.20, minimum_span: float = 16_000.0) -> tuple[float, float, float, float]:
    available = [bounds for layer in layers if (bounds := _nonzero_bounds(layer)) is not None]
    if not available:
        return layers[0].bounds
    left = min(item[0] for item in available)
    bottom = min(item[1] for item in available)
    right = max(item[2] for item in available)
    top = max(item[3] for item in available)
    width = max(right - left, minimum_span)
    height = max(top - bottom, minimum_span)
    center_x = (left + right) / 2.0
    center_y = (bottom + top) / 2.0
    width *= 1.0 + pad_fraction
    height *= 1.0 + pad_fraction
    return (
        center_x - width / 2.0,
        center_y - height / 2.0,
        center_x + width / 2.0,
        center_y + height / 2.0,
    )


def _masked_positive(data: np.ndarray) -> np.ma.MaskedArray:
    return np.ma.masked_where(~np.isfinite(data) | (data <= 0), data)


def _cell_bounds(layer: RasterLayer, row: int, col: int) -> tuple[float, float, float, float]:
    x0, y0 = layer.transform * (col, row)
    x1, y1 = layer.transform * (col + 1, row + 1)
    return min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)


def _show_layer(ax: plt.Axes, layer: RasterLayer, color: str, alpha: float, label: str | None = None) -> None:
    rows, cols = np.where(np.isfinite(layer.data) & (layer.data > 0))
    for row, col in zip(rows, cols):
        left, bottom, right, top = _cell_bounds(layer, int(row), int(col))
        ax.add_patch(
            Rectangle(
                (left, bottom),
                right - left,
                top - bottom,
                facecolor=color,
                edgecolor="#ffffff",
                linewidth=0.45,
                alpha=alpha,
                zorder=5,
            )
        )
    if label:
        ax.plot([], [], color=color, linewidth=8, alpha=alpha, label=label)


def _show_continuous(ax: plt.Axes, layer: RasterLayer, cmap: Any, alpha: float = 0.90) -> ScalarMappable:
    rows, cols = np.where(np.isfinite(layer.data) & (layer.data > 0))
    values = layer.data[rows, cols].astype(float)
    if values.size == 0:
        norm = Normalize(vmin=0.0, vmax=1.0)
        return ScalarMappable(norm=norm, cmap=cmap)
    vmin = float(np.nanmin(values))
    vmax = float(np.nanmax(values))
    if np.isclose(vmin, vmax):
        vmin = max(0.0, vmin - 0.05)
        vmax = min(1.0, vmax + 0.05) if vmax <= 1.0 else vmax + 0.05
    norm = Normalize(vmin=vmin, vmax=vmax)
    for row, col, value in zip(rows, cols, values):
        left, bottom, right, top = _cell_bounds(layer, int(row), int(col))
        ax.add_patch(
            Rectangle(
                (left, bottom),
                right - left,
                top - bottom,
                facecolor=cmap(norm(float(value))),
                edgecolor="#ffffff",
                linewidth=0.45,
                alpha=alpha,
                zorder=5,
            )
        )
    return ScalarMappable(norm=norm, cmap=cmap)


def _format_axes(
    ax: plt.Axes,
    bounds: tuple[float, float, float, float],
    title: str,
    subtitle: str | None = None,
    *,
    show_ticks: bool = False,
    show_grid: bool = False,
) -> None:
    ax.set_xlim(bounds[0], bounds[2])
    ax.set_ylim(bounds[1], bounds[3])
    ax.set_facecolor("#f4fbff")
    ax.set_aspect("equal", adjustable="box")
    if show_grid:
        ax.grid(True, color="#dbe7f0", linewidth=0.7)
    else:
        ax.grid(False)
    display_title = "\n".join(textwrap.wrap(title, width=28)) if len(title) > 28 else title
    ax.set_title(display_title, loc="left", fontsize=11.4, fontweight="bold", pad=9)
    if subtitle:
        ax.text(0.0, 1.01, subtitle, transform=ax.transAxes, fontsize=9.2, color="#475569", va="bottom")
    if show_ticks:
        ax.tick_params(labelsize=8)
        ax.set_xlabel("Easting (m)", fontsize=9)
        ax.set_ylabel("Northing (m)", fontsize=9)
    else:
        ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
        ax.set_xlabel("")
        ax.set_ylabel("")
    for spine in ax.spines.values():
        spine.set_color("#cbd5e1")
        spine.set_linewidth(0.8)


def _show_land(ax: plt.Axes, land: RasterLayer | None) -> None:
    if land is None:
        return
    ax.imshow(
        _masked_positive(land.data),
        extent=land.bounds,
        origin="upper",
        interpolation="nearest",
        cmap=ListedColormap(["#dfd7c8"]),
        alpha=0.55,
        zorder=1,
    )


def _add_footer(fig: plt.Figure, text: str) -> None:
    fig.text(0.02, 0.022, text, ha="left", va="bottom", fontsize=8.2, color="#475569")


def _save(fig: plt.Figure, path: Path) -> None:
    absolute = _repo_path(path)
    absolute.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(absolute, dpi=240, facecolor="white")
    plt.close(fig)


def _validate_public_text(*texts: str) -> None:
    joined = "\n".join(texts)
    found = [token for token in FORBIDDEN_FIGURE_TEXT if token in joined]
    if found:
        raise ValueError(f"Internal label leaked into figure text: {found}")


def _score_rows() -> dict[str, pd.Series]:
    summary = pd.read_csv(_repo_path(CROSSMODEL_SUMMARY))
    by_track = {str(row["track_id"]): row for _, row in summary.iterrows()}
    required = ("R1_previous_reinit_p50", "pygnome_reinit_deterministic")
    missing = [track for track in required if track not in by_track]
    if missing:
        raise ValueError(f"Missing required score rows: {missing}")
    return by_track


def _mean_fss(row: pd.Series) -> float:
    value = pd.to_numeric(row.get("mean_fss"), errors="coerce")
    if pd.notna(value):
        return float(value)
    values = [pd.to_numeric(row.get(f"fss_{window}km"), errors="coerce") for window in (1, 3, 5, 10)]
    return float(np.nanmean(values))


def build_workflow_figure() -> dict[str, Any]:
    output_path = OUTPUT_DIR / "workflow_descriptive_labels.png"
    labels = [new for _, new in INTERNAL_LABELS["workflow"]]
    _validate_public_text(*labels, "Public-facing evidence workflow")

    fig = plt.figure(figsize=(11.2, 8.4), constrained_layout=False)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_axis_off()
    fig.text(0.065, 0.935, "Public-facing evidence workflow", fontsize=20, fontweight="bold", ha="left")
    fig.text(
        0.065,
        0.897,
        "Public figures use descriptive evidence roles; internal identifiers remain only in repository audit material.",
        fontsize=10.4,
        color="#475569",
        ha="left",
    )

    colors = ["#e8f4ff", "#eef8ef", "#fff7e6", "#f4edff", "#eaf7f6", "#fff0ee", "#f3f4f6"]
    edge_colors = ["#2b6cb0", "#2f855a", "#b7791f", "#805ad5", "#0f766e", "#c2410c", "#64748b"]
    x = 0.10
    w = 0.80
    h = 0.075
    gap = 0.030
    y_top = 0.785
    centers: list[tuple[float, float]] = []

    for index, (label, fill, edge) in enumerate(zip(labels, colors, edge_colors), start=1):
        y = y_top - (index - 1) * (h + gap)
        centers.append((x + w / 2.0, y + h / 2.0))
        patch = FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle="round,pad=0.012,rounding_size=0.014",
            facecolor=fill,
            edgecolor=edge,
            linewidth=1.8,
        )
        ax.add_patch(patch)
        ax.text(
            x + 0.038,
            y + h / 2.0,
            f"{index}",
            fontsize=11.2,
            fontweight="bold",
            color="white",
            va="center",
            ha="center",
            bbox=dict(boxstyle="circle,pad=0.28", facecolor=edge, edgecolor=edge),
        )
        ax.text(
            x + 0.086,
            y + h / 2.0,
            textwrap.fill(label, width=64),
            fontsize=11.2,
            fontweight="bold",
            color="#0f172a",
            va="center",
            ha="left",
            linespacing=1.12,
        )

    arrow_kw = dict(arrowstyle="-|>", color="#64748b", linewidth=2.0, mutation_scale=16)
    for (_, y1), (_, y2) in zip(centers[:-1], centers[1:]):
        ax.annotate("", xy=(0.50, y2 + h / 2.0 + 0.005), xytext=(0.50, y1 - h / 2.0 - 0.005), arrowprops=arrow_kw)

    fig.text(
        0.065,
        0.075,
        "Publication sequence: provenance, forecast generation, primary validation, comparator support, external transfer validation, support/context, and audit retention.",
        fontsize=9.3,
        color="#334155",
        ha="left",
    )
    _save(fig, output_path)
    return {
        "figure_key": "workflow",
        "old_figure_path": "analysis_exports/chapter5_repo_wide_redo/revised_chapter5_figure_inventory.csv",
        "old_figure_note": "Figure 5.1 was listed as generated from governance assets; no prior PNG was stored.",
        "new_figure_path": _relative(output_path),
        "label_replacements": [{"old_label": old, "new_label": new} for old, new in INTERNAL_LABELS["workflow"]],
    }


def build_ensemble_design_figure() -> dict[str, Any]:
    product_manifest = json.loads(_repo_path(PRODUCT_FAMILY_JSON).read_text(encoding="utf-8"))
    products = product_manifest["selected_products"]
    deterministic = _load_raster(Path(products["control_footprint_mask"]["relative_path"]))
    probability = _load_raster(Path(products["prob_presence"]["relative_path"]))
    p50 = _load_raster(Path(products["mask_p50"]["relative_path"]))
    p90 = _load_raster(Path(products["mask_p90"]["relative_path"]))
    land = _load_optional_raster(LAND_MASK)
    bounds = _union_bounds([deterministic, probability, p50, p90], pad_fraction=0.34, minimum_span=7_500.0)

    title = "Mindoro ensemble-design products"
    subtitle = "March 14 public-validation product family with descriptive labels"
    panel_titles = [
        "Adopted primary Mindoro baseline setup",
        "Ensemble probability surface",
        "p50 footprint",
        "p90 footprint",
    ]
    _validate_public_text(title, subtitle, *panel_titles)

    fig, axes = plt.subplots(2, 2, figsize=(11.8, 8.9), constrained_layout=False)
    axes = axes.flatten()
    fig.subplots_adjust(left=0.065, right=0.97, top=0.82, bottom=0.12, wspace=0.16, hspace=0.30)
    fig.text(0.065, 0.93, title, fontsize=20, fontweight="bold", ha="left")
    fig.text(0.065, 0.888, subtitle, fontsize=10.8, color="#475569", ha="left")

    green_cmap = LinearSegmentedColormap.from_list("prob", ["#d8f3e7", "#47c09a", "#0f766e"])

    layers = [
        (deterministic, "#1d5fa7", 0.88, None),
        (probability, None, 0.92, green_cmap),
        (p50, "#23824f", 0.86, None),
        (p90, "#5fa8f4", 0.86, None),
    ]
    for ax, panel_title, (layer, color, alpha, cmap) in zip(axes, panel_titles, layers):
        _show_land(ax, land)
        if cmap is None:
            _show_layer(ax, layer, str(color), alpha)
        else:
            image = _show_continuous(ax, layer, cmap, alpha=alpha)
            cbar = fig.colorbar(image, ax=ax, orientation="horizontal", fraction=0.045, pad=0.035)
            cbar.ax.tick_params(labelsize=7, length=2)
        _format_axes(ax, bounds, panel_title)
    _add_footer(fig, "p50 and p90 are probability thresholds; source rasters were read only, and no scientific scorecard or internal identifier was modified.")
    output_path = OUTPUT_DIR / "ensemble_design_descriptive_labels.png"
    _save(fig, output_path)
    return {
        "figure_key": "ensemble_design",
        "old_figure_path": _relative(Path("output") / "chapter5_generated" / "mindoro_product_family_board_march13_14_r1_previous.png"),
        "new_figure_path": _relative(output_path),
        "label_replacements": [{"old_label": old, "new_label": new} for old, new in INTERNAL_LABELS["ensemble"]],
    }


def build_validation_board() -> dict[str, Any]:
    seed = _load_raster(SEED_MASK)
    target = _load_raster(TARGET_MASK)
    p50 = _load_raster(OPENDRIFT_P50)
    land = _load_optional_raster(LAND_MASK)
    bounds = _union_bounds([seed, target, p50], pad_fraction=0.20, minimum_span=12_500.0)
    rows = _score_rows()
    mean = _mean_fss(rows["R1_previous_reinit_p50"])

    title = "Mindoro March 13-14 public-observation validation"
    panel_titles = [
        "March 13 public seed observation",
        "March 14 public target observation",
        "OpenDrift promoted p50 footprint",
    ]
    _validate_public_text(title, *panel_titles)

    fig, axes = plt.subplots(1, 3, figsize=(13.8, 5.8), constrained_layout=False)
    fig.subplots_adjust(left=0.055, right=0.985, top=0.74, bottom=0.20, wspace=0.16)
    fig.text(0.055, 0.92, title, fontsize=18.5, fontweight="bold", ha="left")
    fig.text(
        0.055,
        0.875,
        "Descriptive labels only; March 13 and March 14 are independent public day-specific observation products.",
        fontsize=10.4,
        color="#475569",
        ha="left",
    )

    for ax, panel_title in zip(axes, panel_titles):
        _show_land(ax, land)
        _format_axes(ax, bounds, panel_title)

    _show_layer(axes[0], seed, "#d97706", 0.68)
    _show_layer(axes[1], target, "#4b5563", 0.56)
    _show_layer(axes[2], target, "#4b5563", 0.32)
    _show_layer(axes[2], seed, "#d97706", 0.34)
    _show_layer(axes[2], p50, "#23824f", 0.86)

    legend_items = [
        Patch(facecolor="#d97706", edgecolor="#b45309", alpha=0.68, label="March 13 public seed observation"),
        Patch(facecolor="#4b5563", edgecolor="#374151", alpha=0.56, label="March 14 public target observation"),
        Patch(facecolor="#23824f", edgecolor="#166534", alpha=0.86, label="OpenDrift promoted p50 footprint"),
    ]
    fig.legend(handles=legend_items, loc="lower center", ncol=3, frameon=True, fontsize=10, bbox_to_anchor=(0.5, 0.095))
    _add_footer(fig, f"Stored mean FSS for OpenDrift promoted p50 footprint: {mean:.3f}. Internal branch identifiers remain only in manifests and source tables.")
    output_path = OUTPUT_DIR / "mindoro_validation_board_descriptive_labels.png"
    _save(fig, output_path)
    return {
        "figure_key": "mindoro_validation_board",
        "old_figure_path": _relative(PRIMARY_BOARD),
        "new_figure_path": _relative(output_path),
        "label_replacements": [{"old_label": old, "new_label": new} for old, new in INTERNAL_LABELS["validation"]],
    }


def build_comparator_board() -> dict[str, Any]:
    target = _load_raster(TARGET_MASK)
    p50 = _load_raster(OPENDRIFT_P50)
    pygnome = _load_raster(PYGNOME_DETERMINISTIC)
    seed = _load_raster(SEED_MASK)
    land = _load_optional_raster(LAND_MASK)
    bounds = _union_bounds([target, p50, pygnome, seed], pad_fraction=0.16, minimum_span=13_000.0)
    rows = _score_rows()
    opendrift_mean = _mean_fss(rows["R1_previous_reinit_p50"])
    pygnome_mean = _mean_fss(rows["pygnome_reinit_deterministic"])

    title = "Mindoro same-case model-comparator board"
    panel_titles = [
        "March 14 public target observation",
        "OpenDrift promoted p50 footprint",
        "PyGNOME deterministic comparator",
    ]
    _validate_public_text(title, *panel_titles)

    fig, axes = plt.subplots(1, 3, figsize=(13.8, 5.8), constrained_layout=False)
    fig.subplots_adjust(left=0.055, right=0.985, top=0.74, bottom=0.20, wspace=0.16)
    fig.text(0.055, 0.92, title, fontsize=18.5, fontweight="bold", ha="left")
    fig.text(
        0.055,
        0.875,
        "The public target observation remains the scoring reference; PyGNOME is shown only as comparator support.",
        fontsize=10.4,
        color="#475569",
        ha="left",
    )

    for ax, panel_title in zip(axes, panel_titles):
        _show_land(ax, land)
        _format_axes(ax, bounds, panel_title)

    _show_layer(axes[0], target, "#4b5563", 0.56)
    _show_layer(axes[1], target, "#4b5563", 0.28)
    _show_layer(axes[1], seed, "#d97706", 0.30)
    _show_layer(axes[1], p50, "#23824f", 0.86)
    _show_layer(axes[2], target, "#4b5563", 0.28)
    _show_layer(axes[2], seed, "#d97706", 0.30)
    _show_layer(axes[2], pygnome, "#9b4dca", 0.86)

    legend_items = [
        Patch(facecolor="#4b5563", edgecolor="#374151", alpha=0.56, label="March 14 public target observation"),
        Patch(facecolor="#23824f", edgecolor="#166534", alpha=0.86, label="OpenDrift promoted p50 footprint"),
        Patch(facecolor="#9b4dca", edgecolor="#7e22ce", alpha=0.86, label="PyGNOME deterministic comparator"),
    ]
    fig.legend(handles=legend_items, loc="lower center", ncol=3, frameon=True, fontsize=10, bbox_to_anchor=(0.5, 0.095))
    _add_footer(
        fig,
        f"Stored mean FSS: OpenDrift promoted p50 {opendrift_mean:.3f}; PyGNOME deterministic comparator {pygnome_mean:.3f}. Non-promoted branches are omitted from this publication board.",
    )
    output_path = OUTPUT_DIR / "mindoro_comparator_board_descriptive_labels.png"
    _save(fig, output_path)
    return {
        "figure_key": "mindoro_comparator_board",
        "old_figure_path": _relative(COMPARATOR_BOARD),
        "new_figure_path": _relative(output_path),
        "label_replacements": [{"old_label": old, "new_label": new} for old, new in INTERNAL_LABELS["comparator"]],
    }


def build_mean_fss_chart() -> dict[str, Any]:
    rows = _score_rows()
    chart_rows = [
        ("OpenDrift promoted p50 branch", _mean_fss(rows["R1_previous_reinit_p50"]), "#23824f"),
        ("PyGNOME deterministic comparator", _mean_fss(rows["pygnome_reinit_deterministic"]), "#9b4dca"),
    ]
    _validate_public_text("Mean FSS by public comparator", *(row[0] for row in chart_rows))

    labels = [row[0] for row in chart_rows]
    values = [row[1] for row in chart_rows]
    colors = [row[2] for row in chart_rows]
    fig, ax = plt.subplots(figsize=(8.6, 5.2), constrained_layout=False)
    fig.subplots_adjust(left=0.13, right=0.96, top=0.78, bottom=0.25)
    fig.text(0.13, 0.92, "Mean FSS by public comparator", ha="left", fontsize=17, fontweight="bold")
    positions = np.arange(len(labels))
    bars = ax.bar(positions, values, color=colors, alpha=0.88, width=0.56)
    ax.set_xticks(
        positions,
        labels=["OpenDrift promoted\np50 branch", "PyGNOME deterministic\ncomparator"],
        fontsize=10.4,
    )
    ax.set_ylabel("Mean FSS across 1, 3, 5, and 10 km windows", fontsize=10.4)
    ax.set_ylim(0, max(values) * 1.28 if values else 0.12)
    ax.grid(axis="y", color="#dbe7f0")
    ax.set_axisbelow(True)
    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2.0, value + 0.003, f"{value:.3f}", ha="center", va="bottom", fontsize=10.5)
    fig.text(
        0.13,
        0.09,
        textwrap.fill(
            "Only the promoted OpenDrift p50 branch and the deterministic PyGNOME comparator are included; non-promoted internal branches are excluded.",
            width=96,
        ),
        fontsize=9.2,
        color="#475569",
        va="bottom",
    )
    output_path = OUTPUT_DIR / "mindoro_mean_fss_descriptive_labels.png"
    _save(fig, output_path)
    return {
        "figure_key": "mindoro_mean_fss_chart",
        "old_figure_path": _relative(CROSSMODEL_SUMMARY),
        "new_figure_path": _relative(output_path),
        "label_replacements": [{"old_label": old, "new_label": new} for old, new in INTERNAL_LABELS["mean_fss"]],
        "included_source_track_ids": ["R1_previous_reinit_p50", "pygnome_reinit_deterministic"],
        "excluded_source_track_ids": ["R0_reinit_p50"],
    }


def build_manifest(figures: list[dict[str, Any]]) -> None:
    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "output_dir": _relative(OUTPUT_DIR),
        "publication_package_built_from_existing_outputs_only": True,
        "expensive_scientific_reruns_triggered": False,
        "underlying_scientific_scorecards_modified": False,
        "internal_ids_policy": "Internal run and track identifiers are retained only in manifests, file names, logs, audit pages, and source paths; they are not displayed in the generated publication PNGs.",
        "source_scorecards_read_only": [
            _relative(PRIMARY_SUMMARY),
            _relative(CROSSMODEL_SUMMARY),
        ],
        "figures": figures,
    }
    _repo_path(MANIFEST_PATH).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    figures = [
        build_workflow_figure(),
        build_ensemble_design_figure(),
        build_validation_board(),
        build_comparator_board(),
        build_mean_fss_chart(),
    ]
    build_manifest(figures)
    print(f"Wrote {len(figures)} descriptive-label figures to {_relative(OUTPUT_DIR)}")
    print(f"Manifest: {_relative(MANIFEST_PATH)}")


if __name__ == "__main__":
    main()
