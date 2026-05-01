from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Iterable

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.patches import Rectangle
import numpy as np
import rasterio
from rasterio import features
from shapely.geometry import MultiPolygon, Polygon, box, shape
from shapely.ops import unary_union


_SCRIPT_PATH = Path(globals().get("__file__", "")).resolve()
if _SCRIPT_PATH.name == "<stdin>" or not (_SCRIPT_PATH.parents[2] / "output").exists():
    REPO_ROOT = Path.cwd()
else:
    REPO_ROOT = _SCRIPT_PATH.parents[2]
PRIMARY_DIR = REPO_ROOT / "output" / "CASE_MINDORO_RETRO_2023" / "phase3b_extended_public_scored_march13_14_reinit"
PRECHECK_PATH = PRIMARY_DIR / "precheck" / "march14_reinit_branch_R1_previous.json"
SEED_MASK_PATH = PRIMARY_DIR / "march13_seed_mask_on_grid.tif"
SUMMARY_CSV = (
    REPO_ROOT
    / "output"
    / "Phase 3B March13-14 Final Output"
    / "summary"
    / "opendrift_primary"
    / "march13_14_reinit_summary.csv"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "presentation_assets" / "cover"

OBSERVED_FACE = "#9EA6AD"
OBSERVED_EDGE = "#2E3A45"
SEED_EDGE = "#E07B00"
P50_FACE = "#5FAE7D"
P50_EDGE = "#247A52"
WATER = "#F4FAFD"
LAND = "#EAE5D9"
INK = "#111827"
MUTED = "#4B5563"
PANEL_EDGE = "#CBD5E1"


def _repo_relative(path: Path) -> str:
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _resolve_repo_path(raw_path: str | Path) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def _load_font(font_path: str | None) -> tuple[str, font_manager.FontProperties]:
    if font_path:
        path = Path(font_path)
        if not path.exists():
            raise FileNotFoundError(f"Arial font path does not exist: {path}")
        font_manager.fontManager.addfont(str(path))
        prop = font_manager.FontProperties(fname=str(path))
        family = prop.get_name()
    else:
        family = "Arial"
        prop = font_manager.FontProperties(family=family)

    plt.rcParams.update(
        {
            "font.family": family,
            "font.sans-serif": [family],
            "axes.unicode_minus": False,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )
    return family, prop


def _read_mask(path: Path) -> tuple[np.ndarray, rasterio.Affine, rasterio.crs.CRS]:
    with rasterio.open(path) as src:
        array = src.read(1)
        transform = src.transform
        crs = src.crs
    return array, transform, crs


def _raster_extent(transform: rasterio.Affine, width: int, height: int) -> tuple[float, float, float, float]:
    left = transform.c
    top = transform.f
    right = left + transform.a * width
    bottom = top + transform.e * height
    return left, right, bottom, top


def _mask_bounds(mask: np.ndarray, transform: rasterio.Affine) -> tuple[float, float, float, float]:
    rows, cols = np.where(mask)
    if len(rows) == 0:
        raise ValueError("Cannot compute bounds for an empty mask")
    x0 = transform.c + cols.min() * transform.a
    x1 = transform.c + (cols.max() + 1) * transform.a
    y1 = transform.f + rows.min() * transform.e
    y0 = transform.f + (rows.max() + 1) * transform.e
    return min(x0, x1), max(x0, x1), min(y0, y1), max(y0, y1)


def _polygonize(mask: np.ndarray, transform: rasterio.Affine) -> list[Polygon | MultiPolygon]:
    binary = mask.astype("uint8")
    geometries = [
        shape(geom)
        for geom, value in features.shapes(binary, mask=binary.astype(bool), transform=transform)
        if int(value) == 1
    ]
    if not geometries:
        return []
    unioned = unary_union(geometries)
    if isinstance(unioned, (Polygon, MultiPolygon)):
        return [unioned]
    return [geom for geom in getattr(unioned, "geoms", []) if isinstance(geom, (Polygon, MultiPolygon))]


def _draw_polygon(ax: plt.Axes, geom: Polygon | MultiPolygon, **kwargs) -> None:
    if isinstance(geom, MultiPolygon):
        for part in geom.geoms:
            _draw_polygon(ax, part, **kwargs)
        return
    exterior = np.asarray(geom.exterior.coords)
    patch = MplPolygon(exterior, closed=True, **kwargs)
    ax.add_patch(patch)


def _draw_mask_polygons(
    ax: plt.Axes,
    mask: np.ndarray,
    transform: rasterio.Affine,
    *,
    facecolor: str,
    edgecolor: str,
    alpha: float,
    linewidth: float,
    linestyle: str = "-",
    zorder: int = 3,
) -> None:
    for geom in _polygonize(mask, transform):
        _draw_polygon(
            ax,
            geom,
            facecolor=facecolor,
            edgecolor=edgecolor,
            linewidth=linewidth,
            linestyle=linestyle,
            alpha=alpha,
            zorder=zorder,
            joinstyle="round",
            capstyle="round",
        )


def _load_primary_paths() -> dict[str, Path]:
    precheck = json.loads(PRECHECK_PATH.read_text(encoding="utf-8"))
    return {
        "forecast": _resolve_repo_path(precheck["forecast"]["path"]),
        "target": _resolve_repo_path(precheck["target"]["path"]),
        "sea_mask": _resolve_repo_path(precheck["sea_mask_path"]),
        "seed": SEED_MASK_PATH,
    }


def _load_score() -> dict[str, str]:
    with SUMMARY_CSV.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("branch_id") == "R1_previous":
                mean_fss = float(row["mean_fss"])
                nearest_km = float(row["nearest_distance_to_obs_m"]) / 1000.0
                forecast_cells = int(float(row["forecast_nonzero_cells"]))
                observed_cells = int(float(row["obs_nonzero_cells"]))
                return {
                    "mean_fss": f"{mean_fss:.3f}",
                    "nearest_km": f"{nearest_km:.2f} km",
                    "cells": f"{forecast_cells} modeled / {observed_cells} observed cells",
                }
    raise ValueError(f"Could not find R1_previous score row in {SUMMARY_CSV}")


def _style_panel(ax: plt.Axes) -> None:
    ax.set_facecolor("white")
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color(PANEL_EDGE)
        spine.set_linewidth(1.2)
    ax.set_xticks([])
    ax.set_yticks([])


def _build_main_map(ax: plt.Axes, font_prop: font_manager.FontProperties, sources: dict[str, Path]) -> dict[str, float]:
    forecast, transform, _ = _read_mask(sources["forecast"])
    target, target_transform, _ = _read_mask(sources["target"])
    seed, seed_transform, _ = _read_mask(sources["seed"])
    sea, sea_transform, _ = _read_mask(sources["sea_mask"])

    forecast_mask = forecast > 0.5
    target_mask = target > 0.5
    seed_mask = seed > 0.5
    land_mask = sea == 0

    height, width = forecast.shape
    left, right, bottom, top = _raster_extent(transform, width, height)

    # Cover-slide crop: tighter than the full analysis board, while preserving
    # the coastline cells, north arrow, and 5 km scale context.
    xlim = (max(left, 322000.0), min(right, 346000.0))
    ylim = (max(bottom, 1456500.0), min(top, 1474500.0))

    ax.set_facecolor(WATER)
    ax.imshow(
        np.where(land_mask, 1.0, np.nan),
        extent=(left, right, bottom, top),
        origin="upper",
        cmap=matplotlib.colors.ListedColormap([LAND]),
        interpolation="nearest",
        alpha=0.95,
        zorder=1,
    )

    _draw_mask_polygons(
        ax,
        target_mask,
        target_transform,
        facecolor=OBSERVED_FACE,
        edgecolor=OBSERVED_EDGE,
        alpha=0.78,
        linewidth=2.2,
        zorder=3,
    )
    _draw_mask_polygons(
        ax,
        forecast_mask,
        transform,
        facecolor=P50_FACE,
        edgecolor=P50_EDGE,
        alpha=0.42,
        linewidth=2.6,
        zorder=4,
    )
    _draw_mask_polygons(
        ax,
        seed_mask,
        seed_transform,
        facecolor="none",
        edgecolor=SEED_EDGE,
        alpha=1.0,
        linewidth=2.8,
        linestyle="--",
        zorder=5,
    )

    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.set_aspect("equal", adjustable="box")
    _style_panel(ax)

    # North arrow.
    arrow_x = xlim[1] - 0.055 * (xlim[1] - xlim[0])
    arrow_y0 = ylim[1] - 0.245 * (ylim[1] - ylim[0])
    arrow_y1 = ylim[1] - 0.105 * (ylim[1] - ylim[0])
    ax.annotate(
        "",
        xy=(arrow_x, arrow_y1),
        xytext=(arrow_x, arrow_y0),
        arrowprops=dict(arrowstyle="-|>", color=INK, lw=2.4, shrinkA=0, shrinkB=0),
        zorder=10,
    )
    ax.text(
        arrow_x,
        arrow_y0 - 0.018 * (ylim[1] - ylim[0]),
        "N",
        ha="center",
        va="top",
        color=INK,
        fontproperties=font_prop,
        fontsize=16,
        fontweight="bold",
        zorder=10,
    )

    # Scale bar.
    bar_m = 5000.0
    bar_x0 = xlim[0] + 0.065 * (xlim[1] - xlim[0])
    bar_y = ylim[0] + 0.085 * (ylim[1] - ylim[0])
    ax.plot([bar_x0, bar_x0 + bar_m], [bar_y, bar_y], color=INK, lw=4.0, solid_capstyle="butt", zorder=10)
    ax.text(
        bar_x0 + bar_m / 2,
        bar_y + 0.022 * (ylim[1] - ylim[0]),
        "5 km",
        ha="center",
        va="bottom",
        color=INK,
        fontproperties=font_prop,
        fontsize=13,
        zorder=10,
    )

    return {
        "x_min": float(xlim[0]),
        "x_max": float(xlim[1]),
        "y_min": float(ylim[0]),
        "y_max": float(ylim[1]),
    }


def _draw_shapely_fill(ax: plt.Axes, geom: Polygon | MultiPolygon, facecolor: str, edgecolor: str, linewidth: float) -> None:
    if isinstance(geom, MultiPolygon):
        for part in geom.geoms:
            _draw_shapely_fill(ax, part, facecolor, edgecolor, linewidth)
        return
    xy = np.asarray(geom.exterior.coords)
    ax.add_patch(
        MplPolygon(
            xy,
            closed=True,
            facecolor=facecolor,
            edgecolor=edgecolor,
            linewidth=linewidth,
            joinstyle="round",
        )
    )


def _build_locator(ax: plt.Axes, font_prop: font_manager.FontProperties) -> None:
    ax.set_facecolor("#F7FBFD")
    _style_panel(ax)

    try:
        import cartopy.io.shapereader as shpreader

        countries = shpreader.Reader(
            shpreader.natural_earth(resolution="50m", category="cultural", name="admin_0_countries")
        )
        philippines = None
        for record in countries.records():
            if record.attributes.get("ADMIN") == "Philippines":
                philippines = record.geometry
                break
        if philippines is not None:
            clip = box(116.0, 4.0, 127.5, 21.5)
            _draw_shapely_fill(ax, philippines.intersection(clip), "#9A9187", "#9A9187", 0.9)
    except Exception:
        # Minimal fallback that keeps the locator informative if Natural Earth is unavailable.
        islands = [
            [(120.0, 18.6), (121.6, 17.4), (122.2, 15.7), (121.0, 13.8), (120.2, 15.8)],
            [(121.0, 13.6), (122.2, 13.1), (122.0, 12.0), (120.9, 12.2)],
            [(123.0, 13.0), (124.5, 11.0), (124.2, 8.5), (122.8, 10.4)],
        ]
        for coords in islands:
            ax.add_patch(MplPolygon(coords, closed=True, facecolor="#9A9187", edgecolor="#9A9187"))

    focus_box = Rectangle(
        (120.5, 12.15),
        1.95,
        1.65,
        facecolor="none",
        edgecolor="#C9332A",
        linewidth=2.0,
        linestyle="-",
    )
    ax.add_patch(focus_box)
    ax.plot(121.35, 12.75, marker="o", color=INK, markersize=5)
    ax.text(
        121.42,
        12.72,
        "Mindoro",
        ha="left",
        va="center",
        color=INK,
        fontproperties=font_prop,
        fontsize=11,
        fontweight="bold",
    )
    ax.text(
        116.55,
        20.85,
        "Philippines",
        ha="left",
        va="top",
        color=MUTED,
        fontproperties=font_prop,
        fontsize=11,
    )
    ax.set_xlim(116.0, 127.5)
    ax.set_ylim(4.0, 21.5)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])


def _draw_scorecard(ax: plt.Axes, font_prop: font_manager.FontProperties, score: dict[str, str]) -> None:
    _style_panel(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    ax.text(
        0.06,
        0.86,
        "Primary validation case",
        ha="left",
        va="center",
        color=INK,
        fontproperties=font_prop,
        fontsize=17,
        fontweight="bold",
    )
    ax.text(
        0.06,
        0.75,
        "March 13 seed -> March 14 target",
        ha="left",
        va="center",
        color=MUTED,
        fontproperties=font_prop,
        fontsize=12.5,
    )

    rows = [
        ("Observed target", OBSERVED_FACE, OBSERVED_EDGE, "-"),
        ("March 13 seed", "none", SEED_EDGE, "--"),
        ("OpenDrift p50 footprint", P50_FACE, P50_EDGE, "-"),
    ]
    y = 0.59
    for label, face, edge, linestyle in rows:
        rect = Rectangle(
            (0.07, y - 0.035),
            0.095,
            0.06,
            facecolor=face if face != "none" else "white",
            edgecolor=edge,
            linewidth=2.0,
            linestyle=linestyle,
            alpha=0.82 if face != "none" else 1.0,
        )
        ax.add_patch(rect)
        ax.text(
            0.20,
            y,
            label,
            ha="left",
            va="center",
            color=INK,
            fontproperties=font_prop,
            fontsize=13.5,
        )
        y -= 0.14

    ax.plot([0.06, 0.94], [0.21, 0.21], color=PANEL_EDGE, lw=1.0)
    ax.text(
        0.06,
        0.125,
        f"FSS = {score['mean_fss']}",
        ha="left",
        va="center",
        color=INK,
        fontproperties=font_prop,
        fontsize=18,
        fontweight="bold",
    )
    ax.text(
        0.94,
        0.125,
        f"Nearest {score['nearest_km']}",
        ha="right",
        va="center",
        color=MUTED,
        fontproperties=font_prop,
        fontsize=12.5,
    )


def _make_figure(
    *,
    output_path: Path,
    dpi: int,
    font_prop: font_manager.FontProperties,
    sources: dict[str, Path],
    score: dict[str, str],
) -> dict[str, object]:
    fig = plt.figure(figsize=(10, 10), dpi=dpi, facecolor="#FBFCFD")

    fig.text(
        0.055,
        0.952,
        "March 13-14 Mindoro validation",
        ha="left",
        va="top",
        color=INK,
        fontproperties=font_prop,
        fontsize=27,
        fontweight="bold",
    )
    fig.text(
        0.055,
        0.908,
        "Observed target vs modeled p50 footprint",
        ha="left",
        va="top",
        color=MUTED,
        fontproperties=font_prop,
        fontsize=15,
    )

    ax_map = fig.add_axes([0.055, 0.305, 0.89, 0.56])
    map_extent = _build_main_map(ax_map, font_prop, sources)

    ax_locator = fig.add_axes([0.055, 0.06, 0.38, 0.19])
    _build_locator(ax_locator, font_prop)

    ax_score = fig.add_axes([0.49, 0.06, 0.455, 0.19])
    _draw_scorecard(ax_score, font_prop, score)

    fig.text(
        0.055,
        0.274,
        "Focused B1 public-observation validation",
        ha="left",
        va="center",
        color=MUTED,
        fontproperties=font_prop,
        fontsize=11.5,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=dpi, facecolor=fig.get_facecolor(), bbox_inches=None, pad_inches=0)
    plt.close(fig)

    return {
        "output_path": _repo_relative(output_path),
        "pixel_size": [int(10 * dpi), int(10 * dpi)],
        "map_extent_epsg32651_m": map_extent,
    }


def build_cover_assets(output_dir: Path, font_path: str | None) -> None:
    _, font_prop = _load_font(font_path)
    sources = _load_primary_paths()
    score = _load_score()

    outputs = [
        (output_dir / "mindoro_primary_validation_cover_square.png", 200),
        (output_dir / "mindoro_primary_validation_cover_square_hires.png", 400),
    ]
    rendered = [
        _make_figure(output_path=path, dpi=dpi, font_prop=font_prop, sources=sources, score=score)
        for path, dpi in outputs
    ]

    manifest = {
        "asset": "Mindoro primary validation cover square",
        "generated_from": {
            "precheck_json": _repo_relative(PRECHECK_PATH),
            "summary_csv": _repo_relative(SUMMARY_CSV),
            "seed_mask": _repo_relative(sources["seed"]),
            "target_mask": _repo_relative(sources["target"]),
            "forecast_mask_p50": _repo_relative(sources["forecast"]),
            "sea_mask": _repo_relative(sources["sea_mask"]),
        },
        "scorecard": score,
        "outputs": rendered,
        "font": "Arial",
        "claim_boundary": "Cover-slide visual summary only; not a replacement for the full analysis board.",
    }
    (output_dir / "mindoro_primary_validation_cover_square_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build square Mindoro B1 cover-slide validation assets.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory for presentation assets.")
    parser.add_argument("--font-path", default=None, help="Path to Arial TTF/OTF. Recommended for exact Arial output.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    build_cover_assets(Path(args.output_dir), args.font_path)
    print(f"Wrote Mindoro cover assets to {Path(args.output_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
