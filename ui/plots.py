"""Small matplotlib plots for the read-only dashboard."""

from __future__ import annotations

import matplotlib
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle

matplotlib.use("Agg")


def phase_status_overview_figure(phase_status: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8.5, 3.8))
    if phase_status.empty:
        ax.text(0.5, 0.5, "Phase-status registry not available.", ha="center", va="center")
        ax.axis("off")
        return fig
    grouped = (
        phase_status.assign(
            reportable_now=phase_status["reportable_now"].astype(bool),
            inherited_provisional=phase_status["inherited_provisional"].astype(bool),
        )
        .groupby("phase_id", dropna=False)[["reportable_now", "inherited_provisional"]]
        .sum()
        .reset_index()
    )
    x = range(len(grouped))
    ax.bar(x, grouped["reportable_now"], label="Reportable now", color="#165ba8")
    ax.bar(x, grouped["inherited_provisional"], label="Inherited-provisional", color="#f28c28", alpha=0.8)
    ax.set_xticks(list(x), grouped["phase_id"].astype(str).tolist(), rotation=0)
    ax.set_ylabel("Track count")
    ax.set_title("Current reportable and inherited-provisional tracks")
    ax.legend(frameon=False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    return fig


def phase4_budget_summary_figure(summary_df: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8.5, 4.0))
    if summary_df.empty:
        ax.text(0.5, 0.5, "Phase 4 oil-budget summary not available.", ha="center", va="center")
        ax.axis("off")
        return fig
    required_columns = {"oil_label", "final_evaporated_pct", "final_dispersed_pct", "final_beached_pct"}
    if not required_columns.issubset(summary_df.columns):
        ax.text(0.5, 0.5, "Phase 4 oil-budget summary is incomplete.", ha="center", va="center")
        ax.axis("off")
        return fig
    df = summary_df.copy()
    x = range(len(df))
    ax.bar(x, df["final_evaporated_pct"], label="Evaporated %", color="#f28c28")
    ax.bar(x, df["final_dispersed_pct"], bottom=df["final_evaporated_pct"], label="Dispersed %", color="#1f7a4d")
    ax.bar(
        x,
        df["final_beached_pct"],
        bottom=df["final_evaporated_pct"] + df["final_dispersed_pct"],
        label="Beached %",
        color="#8c564b",
    )
    ax.set_xticks(list(x), df["oil_label"].astype(str).tolist(), rotation=15, ha="right")
    ax.set_ylabel("Percent of initial mass")
    ax.set_title("Mindoro Phase 4 final oil-budget compartments")
    ax.legend(frameon=False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    return fig


def comparability_status_figure(matrix_df: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(7.2, 3.8))
    if matrix_df.empty or "classification" not in matrix_df.columns:
        ax.text(0.5, 0.5, "Phase 4 cross-model matrix not available.", ha="center", va="center")
        ax.axis("off")
        return fig
    display_map = {
        "directly_comparable_now": "Comparable now",
        "comparable_with_small_adapter": "Small adapter needed",
        "no_matched_phase4_pygnome_package_yet": "No matched\nPyGNOME package yet",
    }
    counts = (
        matrix_df["classification"]
        .astype(str)
        .map(lambda value: display_map.get(value, value.replace("_", " ")))
        .value_counts()
        .sort_index()
    )
    ax.bar(counts.index.tolist(), counts.values.tolist(), color="#9b4dca")
    ax.set_ylabel("Quantity count")
    ax.set_title("Phase 4 cross-model comparability status")
    ax.tick_params(axis="x", rotation=0)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    return fig


def _month_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="Int64")
    month_from_key = pd.to_numeric(df.get("month_key", pd.Series("", index=df.index)).astype(str).str[4:6], errors="coerce")
    month_from_time = pd.to_datetime(df.get("start_time_utc", pd.Series("", index=df.index)), errors="coerce", utc=True).dt.month
    return month_from_key.fillna(month_from_time).astype("Int64")


def b1_drifter_context_map_figure(
    accepted_df: pd.DataFrame,
    subset_df: pd.DataFrame,
    *,
    phase1_validation_box: list[float] | tuple[float, float, float, float] | None = None,
    mindoro_case_domain: list[float] | tuple[float, float, float, float] | None = None,
    source_point: dict | None = None,
) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(9.4, 6.0))

    if accepted_df.empty and subset_df.empty:
        ax.text(0.5, 0.5, "No stored accepted or ranking-subset drifter segments were found.", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        return fig

    accepted = accepted_df.copy()
    subset = subset_df.copy()
    accepted["month_number"] = _month_series(accepted)
    subset["month_number"] = _month_series(subset)

    for frame in (accepted, subset):
        for column in ("start_lon", "end_lon", "start_lat", "end_lat"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

    month_styles = {
        2: {"label": "February subset", "color": "#d97706"},
        3: {"label": "March subset", "color": "#0f766e"},
        4: {"label": "April subset", "color": "#7c3aed"},
    }

    for row in accepted.itertuples(index=False):
        if pd.isna(row.start_lon) or pd.isna(row.end_lon) or pd.isna(row.start_lat) or pd.isna(row.end_lat):
            continue
        ax.plot(
            [float(row.start_lon), float(row.end_lon)],
            [float(row.start_lat), float(row.end_lat)],
            color="#94a3b8",
            linewidth=1.0,
            alpha=0.35,
            zorder=2,
        )

    for month_number, style in month_styles.items():
        month_subset = subset.loc[subset["month_number"] == month_number].copy()
        if month_subset.empty:
            continue
        for row in month_subset.itertuples(index=False):
            if pd.isna(row.start_lon) or pd.isna(row.end_lon) or pd.isna(row.start_lat) or pd.isna(row.end_lat):
                continue
            ax.annotate(
                "",
                xy=(float(row.end_lon), float(row.end_lat)),
                xytext=(float(row.start_lon), float(row.start_lat)),
                arrowprops={
                    "arrowstyle": "-|>",
                    "color": style["color"],
                    "linewidth": 2.1,
                    "alpha": 0.94,
                    "shrinkA": 0.0,
                    "shrinkB": 0.0,
                    "mutation_scale": 10.0,
                },
                zorder=5,
            )
        ax.scatter(
            month_subset["start_lon"],
            month_subset["start_lat"],
            s=18,
            facecolor="#ffffff",
            edgecolor=style["color"],
            linewidth=0.8,
            zorder=6,
        )

    if mindoro_case_domain and len(mindoro_case_domain) == 4:
        min_lon, max_lon, min_lat, max_lat = [float(value) for value in mindoro_case_domain]
        ax.add_patch(
            Rectangle(
                (min_lon, min_lat),
                max_lon - min_lon,
                max_lat - min_lat,
                linewidth=1.3,
                edgecolor="#0f766e",
                facecolor=matplotlib.colors.to_rgba("#0f766e", alpha=0.05),
                linestyle="--",
                zorder=1,
            )
        )
        ax.text(min_lon + 0.06, max_lat - 0.08, "Mindoro case domain", color="#0f766e", fontsize=8.0, va="top")

    if phase1_validation_box and len(phase1_validation_box) == 4:
        min_lon, max_lon, min_lat, max_lat = [float(value) for value in phase1_validation_box]
        ax.add_patch(
            Rectangle(
                (min_lon, min_lat),
                max_lon - min_lon,
                max_lat - min_lat,
                linewidth=1.8,
                edgecolor="#ea580c",
                facecolor=matplotlib.colors.to_rgba("#ea580c", alpha=0.07),
                zorder=3,
            )
        )
        ax.text(min_lon + 0.06, max_lat - 0.08, "Focused Phase 1 box", color="#9a3412", fontsize=8.0, va="top")

    source_lon = pd.to_numeric(pd.Series([(source_point or {}).get("lon")]), errors="coerce").iloc[0]
    source_lat = pd.to_numeric(pd.Series([(source_point or {}).get("lat")]), errors="coerce").iloc[0]
    if pd.notna(source_lon) and pd.notna(source_lat):
        ax.scatter(
            [float(source_lon)],
            [float(source_lat)],
            marker="*",
            s=165,
            facecolor="#b42318",
            edgecolor="#ffffff",
            linewidth=0.8,
            zorder=7,
        )
        ax.text(float(source_lon) + 0.08, float(source_lat) + 0.05, "Source-point context", color="#b42318", fontsize=8.0)

    lon_values = pd.concat(
        [
            accepted.get("start_lon", pd.Series(dtype=float)),
            accepted.get("end_lon", pd.Series(dtype=float)),
            subset.get("start_lon", pd.Series(dtype=float)),
            subset.get("end_lon", pd.Series(dtype=float)),
        ],
        ignore_index=True,
    ).dropna()
    lat_values = pd.concat(
        [
            accepted.get("start_lat", pd.Series(dtype=float)),
            accepted.get("end_lat", pd.Series(dtype=float)),
            subset.get("start_lat", pd.Series(dtype=float)),
            subset.get("end_lat", pd.Series(dtype=float)),
        ],
        ignore_index=True,
    ).dropna()
    if not lon_values.empty and not lat_values.empty:
        min_lon = float(lon_values.min())
        max_lon = float(lon_values.max())
        min_lat = float(lat_values.min())
        max_lat = float(lat_values.max())
        lon_pad = max(0.45, (max_lon - min_lon) * 0.08)
        lat_pad = max(0.35, (max_lat - min_lat) * 0.08)
        ax.set_xlim(min_lon - lon_pad, max_lon + lon_pad)
        ax.set_ylim(min_lat - lat_pad, max_lat + lat_pad)

    legend_handles = [
        Line2D([0], [0], color="#94a3b8", linewidth=1.8, alpha=0.7, label=f"Accepted historical segments (n={len(accepted)})"),
        Line2D([0], [0], color=month_styles[2]["color"], linewidth=2.2, label="February subset"),
        Line2D([0], [0], color=month_styles[3]["color"], linewidth=2.2, label="March subset"),
        Line2D([0], [0], color=month_styles[4]["color"], linewidth=2.2, label="April subset"),
    ]
    ax.legend(handles=legend_handles, loc="upper right", frameon=True, facecolor="#ffffff", edgecolor="#cbd5e1", fontsize=8.4)
    ax.set_title("Historical accepted drifter segments used to select the transport recipe inherited by B1.", loc="left")
    ax.set_xlabel("Longitude (degrees east)")
    ax.set_ylabel("Latitude (degrees north)")
    ax.grid(True, linestyle="--", alpha=0.28)
    ax.set_aspect("equal", adjustable="datalim")
    fig.tight_layout()
    return fig
