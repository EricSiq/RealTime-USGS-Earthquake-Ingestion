"""
High-Quality Scientific Visuals Generator (Light & Dark Mode Compatible)
Renders publication-grade, ultra-high-resolution (300 DPI) scientific figures
specifically optimized for academic reports, light-mode presentations, and posters.

Key Enhancements:
- Vector world continent and country outlines on global seismic map.
- Ample, dedicated title and subtitle spacing preventing collisions or clipping.
- Dual theme support: Light Mode (default) & Dark Mode.
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
import matplotlib.gridspec as gridspec

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_FILE = BASE_DIR / "data" / "hdfs" / "processed" / "earthquakes" / "earthquakes_processed.parquet"
ANALYTICS_DIR = BASE_DIR / "data" / "analytics"
BENCHMARK_FILE = BASE_DIR / "outputs" / "benchmark_results.json"
WORLD_GEOJSON = BASE_DIR / "data" / "world_boundaries.geojson"
DEFAULT_OUTPUT_DIR = BASE_DIR / "outputs" / "visuals"

# Theme Palettes
THEMES = {
    "light": {
        "bg": "#ffffff",
        "surface": "#ffffff",
        "border": "#cbd5e1",
        "grid": "#e2e8f0",
        "text_primary": "#0f172a",
        "text_muted": "#475569",
        "text_dim": "#94a3b8",
        "accent_primary": "#0284c7",    # Sky blue
        "accent_secondary": "#d97706",  # Deep amber
        "hive_color": "#d97706",
        "hbase_color": "#0284c7",
        "point_edge": "#334155",
        "land_fill": "#f1f5f9",
        "land_edge": "#94a3b8",
        "ocean_fill": "#ffffff",
        "callout_bg": "#eff6ff",
        "callout_border": "#3b82f6",
        "callout_text": "#1e3a8a",
        "band_0": "#f8fafc",
        "band_1": "#f1f5f9",
        "band_2": "#fee2e2",
        "band_3": "#fecaca"
    },
    "dark": {
        "bg": "#080c14",
        "surface": "#111827",
        "border": "#1f293d",
        "grid": "#334155",
        "text_primary": "#f8fafc",
        "text_muted": "#94a3b8",
        "text_dim": "#64748b",
        "accent_primary": "#00f0ff",
        "accent_secondary": "#f59e0b",
        "hive_color": "#f59e0b",
        "hbase_color": "#06b6d4",
        "point_edge": "#080c14",
        "land_fill": "#111827",
        "land_edge": "#334155",
        "ocean_fill": "#080c14",
        "callout_bg": "#1e1b4b",
        "callout_border": "#818cf8",
        "callout_text": "#ffffff",
        "band_0": "#0f172a",
        "band_1": "#1e293b",
        "band_2": "#3b151b",
        "band_3": "#450a0a"
    }
}

plt.rcParams["font.sans-serif"] = ["Segoe UI", "DejaVu Sans", "Helvetica", "Arial"]

def get_theme(theme_name: str):
    return THEMES.get(theme_name.lower(), THEMES["light"])

def load_data():
    df = pd.read_parquet(PROCESSED_FILE)
    df["dt_utc"] = pd.to_datetime(df["epoch_millis"], unit="ms", utc=True)
    return df

def get_world_patches():
    if not WORLD_GEOJSON.exists():
        return None
    try:
        with open(WORLD_GEOJSON, "r", encoding="utf-8") as f:
            geojson = json.load(f)
        patches_list = []
        for feat in geojson.get("features", []):
            geom = feat.get("geometry", {})
            gtype = geom.get("type")
            coords = geom.get("coordinates", [])
            if gtype == "Polygon":
                for poly in coords:
                    patches_list.append(Polygon(poly, closed=True))
            elif gtype == "MultiPolygon":
                for mpoly in coords:
                    for poly in mpoly:
                        patches_list.append(Polygon(poly, closed=True))
        return patches_list
    except Exception as e:
        print(f"[WARN] Failed to load world geojson: {e}")
        return None

# -----------------------------------------------------------------------------
# VISUAL 1: Global Seismic Map (With World Continental Outlines & Clean Spacing)
# -----------------------------------------------------------------------------
def generate_global_seismic_map(df: pd.DataFrame, out_dir: Path, theme: dict):
    fig = plt.figure(figsize=(18, 10.5), dpi=300, facecolor=theme["bg"])
    ax = fig.add_subplot(111, facecolor=theme["ocean_fill"])

    # Dedicated header zone with generous spacing
    fig.text(0.05, 0.955, "REAL-TIME GLOBAL SEISMIC TELEMETRY & TECTONIC EPICENTERS", 
             color=theme["text_primary"], fontsize=18, fontweight="bold")
    fig.text(0.05, 0.925, f"Total Events Ingested: {len(df):,}  •  Max Magnitude: M{df['magnitude'].max():.1f}  •  USGS Live Feed Ingested via HDFS Data Lake", 
             color=theme["text_muted"], fontsize=11)

    # World continent & coastline outlines
    patches_list = get_world_patches()
    if patches_list:
        pcol = PatchCollection(patches_list, facecolor=theme["land_fill"], edgecolor=theme["land_edge"], linewidth=0.65, zorder=2)
        ax.add_collection(pcol)

    # Coordinate Gridlines
    ax.axhline(0, color=theme["grid"], linestyle="--", linewidth=1.1, alpha=0.7, zorder=1)
    ax.axvline(0, color=theme["grid"], linestyle="--", linewidth=1.1, alpha=0.7, zorder=1)
    for lat in range(-60, 90, 30):
        ax.axhline(lat, color=theme["grid"], linestyle=":", linewidth=0.6, alpha=0.5, zorder=1)
    for lon in range(-180, 190, 60):
        ax.axvline(lon, color=theme["grid"], linestyle=":", linewidth=0.6, alpha=0.5, zorder=1)

    ax.set_xlim(-180, 180)
    ax.set_ylim(-70, 85)

    sizes = np.clip(14.0 * np.exp(0.55 * (df["magnitude"] - 2.0)), 12, 450)

    # Plot earthquakes
    scatter = ax.scatter(
        df["longitude"],
        df["latitude"],
        s=sizes,
        c=df["depth_km"],
        cmap="turbo_r",
        alpha=0.85,
        edgecolors=theme["point_edge"],
        linewidths=0.55,
        zorder=4
    )

    # Highlight major events (M >= 5.0)
    major_events = df[df["magnitude"] >= 5.0]
    for _, row in major_events.iterrows():
        ax.scatter(row["longitude"], row["latitude"], s=sizes.loc[_]*1.35, facecolors='none', edgecolors="#dc2626", linewidths=2.0, zorder=5)
        place_clean = row['place'].split(',')[-1].strip()
        ax.annotate(
            f"M{row['magnitude']:.1f} • {place_clean}",
            xy=(row["longitude"], row["latitude"]),
            xytext=(row["longitude"] + 3.5, row["latitude"] + 3.5),
            color=theme["text_primary"],
            fontsize=8.5,
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", facecolor=theme["surface"], edgecolor="#dc2626", alpha=0.95, lw=1.2),
            arrowprops=dict(arrowstyle="->", color="#dc2626", lw=1.3),
            zorder=6
        )

    # Horizontal colorbar
    cbar = plt.colorbar(scatter, ax=ax, orientation="horizontal", pad=0.07, fraction=0.035, shrink=0.45)
    cbar.ax.set_facecolor(theme["bg"])
    cbar.set_label("Hypocenter Focal Depth (km)", color=theme["text_primary"], fontsize=11, labelpad=8, fontweight="bold")
    cbar.ax.tick_params(colors=theme["text_muted"], labelsize=9)
    cbar.outline.set_edgecolor(theme["border"])

    ax.set_xlabel("Longitude (°)", color=theme["text_primary"], fontsize=11, labelpad=8)
    ax.set_ylabel("Latitude (°)", color=theme["text_primary"], fontsize=11, labelpad=8)
    ax.tick_params(colors=theme["text_muted"], labelsize=9)

    for spine in ax.spines.values():
        spine.set_color(theme["border"])
        spine.set_linewidth(1.2)

    out_path = out_dir / "01_global_seismic_map.png"
    plt.tight_layout(rect=[0, 0, 1, 0.91])
    plt.savefig(out_path, dpi=300, facecolor=theme["bg"])
    plt.close()
    print(f"  [OK] Saved: {out_path.name}")

# -----------------------------------------------------------------------------
# VISUAL 2: Seismic Drumbeat Strip Chart (Clean Dedicated Header & Breathing Room)
# -----------------------------------------------------------------------------
def generate_seismic_drumbeat_strip(df: pd.DataFrame, out_dir: Path, theme: dict):
    fig, ax = plt.subplots(figsize=(16, 7.5), dpi=300, facecolor=theme["bg"])
    ax.set_facecolor(theme["surface"])

    # Dedicated header zone with ample breathing room
    fig.text(0.06, 0.945, "TEMPORAL SEISMIC DRUMBEAT & MAINSHOCK-AFTERSHOCK CLUSTERING", 
             color=theme["text_primary"], fontsize=16, fontweight="bold")
    fig.text(0.06, 0.908, "Continuous timeline showing temporal event clustering, energy dissipation, and tectonic swarm patterns", 
             color=theme["text_muted"], fontsize=10.5)

    df_sorted = df.sort_values("dt_utc")
    
    scatter = ax.scatter(
        df_sorted["dt_utc"],
        df_sorted["magnitude"],
        c=df_sorted["depth_km"],
        cmap="coolwarm",
        s=np.clip(20.0 * (df_sorted["magnitude"]**1.35), 18, 400),
        alpha=0.82,
        edgecolors=theme["point_edge"],
        linewidths=0.6,
        zorder=3
    )

    # Shaded alert bands
    ax.axhspan(0, 2.5, color=theme["band_0"], alpha=0.5, zorder=1)
    ax.axhspan(2.5, 4.5, color=theme["band_1"], alpha=0.35, zorder=1)
    ax.axhspan(4.5, 6.0, color=theme["band_2"], alpha=0.45, zorder=1)
    ax.axhspan(6.0, 9.0, color=theme["band_3"], alpha=0.55, zorder=1)

    ax.axhline(4.5, color="#d97706", linestyle="--", linewidth=1.3, alpha=0.9, label="Moderate Alert (M 4.5)")
    ax.axhline(6.0, color="#dc2626", linestyle="--", linewidth=1.6, alpha=0.95, label="Major Alert (M 6.0)")

    cbar = plt.colorbar(scatter, ax=ax, pad=0.02, shrink=0.82)
    cbar.set_label("Hypocenter Depth (km)", color=theme["text_primary"], fontsize=10, fontweight="bold")
    cbar.ax.tick_params(colors=theme["text_muted"], labelsize=8.5)
    cbar.outline.set_edgecolor(theme["border"])

    ax.set_ylabel("Earthquake Magnitude (Mw)", color=theme["text_primary"], fontsize=11, labelpad=8)
    ax.set_xlabel("Time (UTC)", color=theme["text_primary"], fontsize=11, labelpad=8)
    ax.tick_params(colors=theme["text_muted"], labelsize=9)
    ax.grid(True, linestyle=":", alpha=0.4, color=theme["grid"])
    ax.legend(facecolor=theme["surface"], edgecolor=theme["border"], labelcolor=theme["text_primary"], loc="upper right")

    for spine in ax.spines.values():
        spine.set_color(theme["border"])
        spine.set_linewidth(1.2)

    out_path = out_dir / "02_seismic_drumbeat_strip.png"
    plt.tight_layout(rect=[0, 0, 1, 0.89])
    plt.savefig(out_path, dpi=300, facecolor=theme["bg"])
    plt.close()
    print(f"  [OK] Saved: {out_path.name}")

# -----------------------------------------------------------------------------
# VISUAL 3: Rolling Activity Frequency & Spike Alert
# -----------------------------------------------------------------------------
def generate_hourly_activity_spikes(out_dir: Path, theme: dict):
    hourly_file = ANALYTICS_DIR / "hourly_activity.csv"
    if not hourly_file.exists():
        return

    hdf = pd.read_csv(hourly_file)
    hdf["hour_dt"] = pd.to_datetime(hdf["hour_window"])
    hdf = hdf.sort_values("hour_dt")

    fig, ax1 = plt.subplots(figsize=(16, 7.5), dpi=300, facecolor=theme["bg"])
    ax1.set_facecolor(theme["surface"])

    fig.text(0.06, 0.945, "ROLLING HOURLY SEISMIC FREQUENCY & ENERGY SPIKE DETECTION", 
             color=theme["text_primary"], fontsize=16, fontweight="bold")
    fig.text(0.06, 0.908, "Empirical proof of continuous streaming ingestion throughput and automated anomaly spike detection", 
             color=theme["text_muted"], fontsize=10.5)

    line1 = ax1.plot(hdf["hour_dt"], hdf["total_events"], color=theme["accent_primary"], linewidth=2.4, label="Hourly Event Count", zorder=3)
    ax1.fill_between(hdf["hour_dt"], hdf["total_events"], color=theme["accent_primary"], alpha=0.18, zorder=2)

    mean_val = hdf["total_events"].mean()
    std_val = hdf["total_events"].std()
    spike_thresh = mean_val + 1.8 * std_val

    ax1.axhline(mean_val, color=theme["text_muted"], linestyle=":", linewidth=1.3, label=f"Mean Rate ({mean_val:.1f} quakes/hr)")
    ax1.axhline(spike_thresh, color="#dc2626", linestyle="--", linewidth=1.6, label=f"Seismic Spike Threshold (+1.8σ = {spike_thresh:.1f})")

    ax2 = ax1.twinx()
    ax2.set_facecolor("none")
    line2 = ax2.plot(hdf["hour_dt"], hdf["total_energy_joules"], color=theme["accent_secondary"], linewidth=1.8, linestyle="-.", alpha=0.88, label="Radiated Energy (Joules)", zorder=3)
    ax2.set_yscale("log")
    ax2.set_ylabel("Radiated Energy (Joules, Log Scale)", color=theme["accent_secondary"], fontsize=11, labelpad=8)
    ax2.tick_params(colors=theme["accent_secondary"], labelsize=9)
    ax2.spines["right"].set_color(theme["accent_secondary"])

    ax1.set_ylabel("Earthquakes Ingested per Hour", color=theme["accent_primary"], fontsize=11, labelpad=8)
    ax1.set_xlabel("Time (Hourly Partition Windows)", color=theme["text_primary"], fontsize=11, labelpad=8)
    ax1.tick_params(colors=theme["text_muted"], labelsize=9)
    ax1.grid(True, linestyle=":", alpha=0.4, color=theme["grid"])

    lines = line1 + line2 + [plt.Line2D([0], [0], color="#dc2626", linestyle="--", lw=1.6)]
    labels = ["Hourly Event Frequency", "Radiated Energy (Joules)", f"Spike Alert Threshold ({spike_thresh:.1f}/hr)"]
    ax1.legend(lines, labels, facecolor=theme["surface"], edgecolor=theme["border"], labelcolor=theme["text_primary"], loc="upper left")

    for spine in ax1.spines.values():
        spine.set_color(theme["border"])
        spine.set_linewidth(1.2)

    out_path = out_dir / "03_hourly_activity_spikes.png"
    plt.tight_layout(rect=[0, 0, 1, 0.89])
    plt.savefig(out_path, dpi=300, facecolor=theme["bg"])
    plt.close()
    print(f"  [OK] Saved: {out_path.name}")

# -----------------------------------------------------------------------------
# VISUAL 4: Depth vs Magnitude Scatter
# -----------------------------------------------------------------------------
def generate_depth_vs_magnitude_scatter(df: pd.DataFrame, out_dir: Path, theme: dict):
    fig = plt.figure(figsize=(15, 9.5), dpi=300, facecolor=theme["bg"])
    gs = gridspec.GridSpec(4, 4, figure=fig, wspace=0.15, hspace=0.15, top=0.88)

    fig.text(0.08, 0.955, "FOCAL DEPTH VS. MAGNITUDE CORRELATION & WADATI-BENIOFF PATTERNS", 
             color=theme["text_primary"], fontsize=16, fontweight="bold")
    fig.text(0.08, 0.925, "Explores seismic dissipation across shallow crustal fault lines and deep subduction zone mantle slabs", 
             color=theme["text_muted"], fontsize=10.5)

    ax_main = fig.add_subplot(gs[1:4, 0:3], facecolor=theme["surface"])
    ax_histx = fig.add_subplot(gs[0, 0:3], facecolor=theme["surface"], sharex=ax_main)
    ax_histy = fig.add_subplot(gs[1:4, 3], facecolor=theme["surface"], sharey=ax_main)

    ax_main.scatter(
        df["magnitude"],
        df["depth_km"],
        c=df["depth_km"],
        cmap="turbo_r",
        s=np.clip(18.0 * (df["magnitude"]**1.3), 14, 320),
        alpha=0.78,
        edgecolors=theme["point_edge"],
        linewidths=0.5
    )
    ax_main.set_ylim(df["depth_km"].max() * 1.05, -5)

    ax_main.axhline(35, color="#dc2626", linestyle="--", linewidth=1.3, alpha=0.8, label="Crust-Mantle Boundary (35 km)")
    ax_main.axhline(150, color="#0284c7", linestyle="--", linewidth=1.3, alpha=0.8, label="Transition Zone (150 km)")

    ax_histx.hist(df["magnitude"], bins=40, color="#0284c7", edgecolor=theme["surface"], alpha=0.85)
    ax_histy.hist(df["depth_km"], bins=40, orientation="horizontal", color="#dc2626", edgecolor=theme["surface"], alpha=0.85)

    plt.setp(ax_histx.get_xticklabels(), visible=False)
    plt.setp(ax_histy.get_yticklabels(), visible=False)
    ax_histx.tick_params(colors=theme["text_dim"])
    ax_histy.tick_params(colors=theme["text_dim"])

    ax_main.set_xlabel("Earthquake Magnitude (Mw)", color=theme["text_primary"], fontsize=11, labelpad=8)
    ax_main.set_ylabel("Hypocenter Focal Depth (km, Inverted)", color=theme["text_primary"], fontsize=11, labelpad=8)
    ax_main.tick_params(colors=theme["text_muted"], labelsize=9)
    ax_main.grid(True, linestyle=":", alpha=0.4, color=theme["grid"])
    ax_main.legend(facecolor=theme["surface"], edgecolor=theme["border"], labelcolor=theme["text_primary"], loc="lower right")

    for ax in [ax_main, ax_histx, ax_histy]:
        for spine in ax.spines.values():
            spine.set_color(theme["border"])
            spine.set_linewidth(1.1)

    out_path = out_dir / "04_depth_vs_magnitude_scatter.png"
    plt.savefig(out_path, dpi=300, facecolor=theme["bg"])
    plt.close()
    print(f"  [OK] Saved: {out_path.name}")

# -----------------------------------------------------------------------------
# VISUAL 5: Latency Benchmark
# -----------------------------------------------------------------------------
def generate_latency_benchmark_chart(out_dir: Path, theme: dict):
    if not BENCHMARK_FILE.exists():
        return

    with open(BENCHMARK_FILE, "r", encoding="utf-8") as f:
        bdata = json.load(f)

    hive_mean = bdata["hive"]["mean_ms"]
    hbase_mean = bdata["hbase"]["mean_ms"]
    speedup = bdata["speedup_factor"]

    fig, ax = plt.subplots(figsize=(14, 8.2), dpi=300, facecolor=theme["bg"])
    ax.set_facecolor(theme["surface"])

    fig.text(0.06, 0.945, "EMPIRICAL LATENCY BENCHMARK: APACHE HIVE VS. APACHE HBASE", 
             color=theme["text_primary"], fontsize=16, fontweight="bold")
    fig.text(0.06, 0.908, "Query: 'Retrieve 10 most recent earthquakes in California' across 50 benchmark iterations", 
             color=theme["text_muted"], fontsize=10.5)

    systems = ["Apache Hive (HDFS OLAP Scan)", "Apache HBase (Prefix Seek)"]
    latencies = [hive_mean, hbase_mean]
    colors = [theme["hive_color"], theme["hbase_color"]]

    bars = ax.barh(systems, latencies, color=colors, height=0.45, edgecolor=theme["border"], linewidth=1.2, zorder=3)
    ax.set_xscale("log")
    ax.set_xlim(0.05, 50000)

    ax.text(hive_mean * 1.25, 0, f"{hive_mean:,.1f} ms (~{hive_mean/1000:.1f} sec)\n[Distributed Block Scan + YARN]", 
            va="center", color=theme["hive_color"], fontsize=11, fontweight="bold")
    ax.text(hbase_mean * 1.35, 1, f"{hbase_mean:.2f} ms (Sub-Millisecond!)\n[Indexed LSM-Tree Row-Key Seek]", 
            va="center", color=theme["hbase_color"], fontsize=11, fontweight="bold")

    ax.text(0.5, 0.48, f"HBASE IS {speedup:,.0f}× FASTER\nFOR OPERATIONAL POINT LOOKUPS",
            transform=ax.transAxes, ha="center", va="center", fontsize=15, fontweight="bold",
            color=theme["callout_text"], bbox=dict(boxstyle="round,pad=0.6", facecolor=theme["callout_bg"], edgecolor=theme["callout_border"], lw=2.0))

    ax.set_xlabel("Query Response Latency (Milliseconds, Logarithmic Scale)", color=theme["text_primary"], fontsize=11, labelpad=10)
    ax.tick_params(colors=theme["text_primary"], labelsize=11)
    ax.grid(True, which="both", linestyle=":", alpha=0.4, color=theme["grid"])

    for spine in ax.spines.values():
        spine.set_color(theme["border"])
        spine.set_linewidth(1.2)

    out_path = out_dir / "05_hive_vs_hbase_latency.png"
    plt.tight_layout(rect=[0, 0, 1, 0.89])
    plt.savefig(out_path, dpi=300, facecolor=theme["bg"])
    plt.close()
    print(f"  [OK] Saved: {out_path.name}")

# -----------------------------------------------------------------------------
# VISUAL 6: Regional Seismic Risk Matrix
# -----------------------------------------------------------------------------
def generate_regional_risk_matrix(out_dir: Path, theme: dict):
    reg_file = ANALYTICS_DIR / "regional_summary.csv"
    if not reg_file.exists():
        return

    rdf = pd.read_csv(reg_file).head(8)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7.5), dpi=300, facecolor=theme["bg"])

    fig.text(0.06, 0.945, "REGIONAL SEISMIC ACTIVITY & HAZARD SCORECARD", 
             color=theme["text_primary"], fontsize=16, fontweight="bold")
    fig.text(0.06, 0.908, "Comparative evaluation of total event volume versus peak Richter magnitude across monitored zones", 
             color=theme["text_muted"], fontsize=10.5)

    # Panel 1
    bars1 = ax1.barh(rdf["region"], rdf["event_count"], color=theme["accent_primary"], edgecolor=theme["border"], height=0.55, zorder=3)
    ax1.set_facecolor(theme["surface"])
    ax1.invert_yaxis()
    ax1.set_title("Seismic Activity Count by Region", color=theme["text_primary"], fontsize=13, fontweight="bold", pad=12)
    ax1.set_xlabel("Event Count", color=theme["text_muted"], fontsize=10)
    ax1.tick_params(colors=theme["text_primary"], labelsize=9.5)
    ax1.grid(True, linestyle=":", alpha=0.4, color=theme["grid"])
    for bar in bars1:
        w = bar.get_width()
        ax1.text(w + 8, bar.get_y() + bar.get_height()/2, f"{int(w):,}", va="center", color=theme["text_primary"], fontsize=9, fontweight="bold")

    # Panel 2
    bars2 = ax2.barh(rdf["region"], rdf["max_magnitude"], color="#dc2626", edgecolor=theme["border"], height=0.55, zorder=3)
    ax2.set_facecolor(theme["surface"])
    ax2.invert_yaxis()
    ax2.set_title("Peak Magnitude Recorded (Mw)", color=theme["text_primary"], fontsize=13, fontweight="bold", pad=12)
    ax2.set_xlabel("Max Magnitude (Richter Scale)", color=theme["text_muted"], fontsize=10)
    ax2.tick_params(colors=theme["text_primary"], labelsize=9.5)
    ax2.grid(True, linestyle=":", alpha=0.4, color=theme["grid"])
    for bar in bars2:
        w = bar.get_width()
        ax2.text(w + 0.1, bar.get_y() + bar.get_height()/2, f"M {w:.1f}", va="center", color=theme["text_primary"], fontsize=9, fontweight="bold")

    for ax in [ax1, ax2]:
        for spine in ax.spines.values():
            spine.set_color(theme["border"])
            spine.set_linewidth(1.2)

    out_path = out_dir / "06_regional_risk_matrix.png"
    plt.tight_layout(rect=[0, 0, 1, 0.89])
    plt.savefig(out_path, dpi=300, facecolor=theme["bg"])
    plt.close()
    print(f"  [OK] Saved: {out_path.name}")

# -----------------------------------------------------------------------------
# VISUAL 7: Hadoop Architecture Infographic
# -----------------------------------------------------------------------------
def generate_architecture_infographic(out_dir: Path, theme: dict):
    fig, ax = plt.subplots(figsize=(16, 9.2), dpi=300, facecolor=theme["bg"])
    ax.set_facecolor(theme["bg"])
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    fig.text(0.05, 0.945, "HADOOP ECOSYSTEM BIG DATA ARCHITECTURE", color=theme["text_primary"], fontsize=20, fontweight="bold")
    fig.text(0.05, 0.91, "Real-Time Ingestion, Distributed Data Lake, Analytical OLAP & Low-Latency Serving Pipeline", color=theme["text_muted"], fontsize=11)

    def draw_box(x, y, w, h, title, subtitle, header_color, border_color):
        rect = patches.FancyBboxPatch((x, y), w, h, boxstyle="round,pad=1.2", facecolor=theme["surface"], edgecolor=border_color, linewidth=1.6)
        ax.add_patch(rect)
        head_rect = patches.FancyBboxPatch((x, y + h - 6), w, 6, boxstyle="round,pad=0.2", facecolor=header_color, edgecolor=border_color, linewidth=0.5)
        ax.add_patch(head_rect)
        ax.text(x + w/2, y + h - 3.2, title, ha="center", va="center", color="#ffffff", fontsize=10.5, fontweight="bold")
        ax.text(x + w/2, y + h/2 - 2, subtitle, ha="center", va="center", color=theme["text_primary"], fontsize=8.5, multialignment="center")

    draw_box(4, 55, 18, 26, "1. INGESTION", "USGS Live GeoJSON\nEvery 1-5 Mins Poller\nStateful Deduplication\nPartition Chunker", "#0284c7", "#38bdf8")
    draw_box(27, 45, 22, 39, "2. HDFS STORAGE", "/raw/ (Immutable JSON)\n/processed/ (ORC Snappy)\nReplication & Block Nodes\nYARN Resource Mgmt", "#9333ea", "#c084fc")
    draw_box(53, 58, 20, 26, "3. BATCH ANALYTICS", "Apache Hive (SQL OLAP)\nJsonSerDe Schema Projection\nApache Pig Procedural ETL\nSnappy Compression", "#d97706", "#fbbf24")
    draw_box(53, 18, 20, 26, "4. SERVING LAYER", "Apache HBase (NoSQL)\nRowKey: region#reverse_ts\nSub-Millisecond Seeks\nZooKeeper Quorum", "#0d9488", "#2dd4bf")
    draw_box(78, 38, 18, 33, "5. VISUAL ANALYTICS", "Interactive Web Dashboard\nPublication-Grade Figures\nRegional Hazard Matrix\nEmpirical Latency Benchmark", "#059669", "#34d399")

    def draw_arrow(x1, y1, x2, y2, label=""):
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                    arrowprops=dict(arrowstyle="->", color=theme["text_muted"], lw=2.2, mutation_scale=15))
        if label:
            ax.text((x1+x2)/2, (y1+y2)/2 + 2, label, color=theme["text_primary"], fontsize=8.5, ha="center", fontweight="bold")

    draw_arrow(23, 68, 26, 68, "Put raw")
    draw_arrow(50, 71, 52, 71, "Hive DDL")
    draw_arrow(50, 31, 52, 31, "HBase Load")
    draw_arrow(74, 71, 77, 60, "Export Aggs")
    draw_arrow(74, 31, 77, 48, "< 1ms Seeks")

    out_path = out_dir / "07_hadoop_architecture_infographic.png"
    plt.tight_layout(rect=[0, 0, 1, 0.90])
    plt.savefig(out_path, dpi=300, facecolor=theme["bg"])
    plt.close()
    print(f"  [OK] Saved: {out_path.name}")

def render_all(theme_name: str = "light", output_dir: Path = DEFAULT_OUTPUT_DIR):
    theme = get_theme(theme_name)
    output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 65)
    print(f"Rendering Visuals [Theme: {theme_name.upper()}] (300 DPI)")
    print(f"Output Directory: {output_dir}")
    print("=" * 65)

    df = load_data()
    generate_global_seismic_map(df, output_dir, theme)
    generate_seismic_drumbeat_strip(df, output_dir, theme)
    generate_hourly_activity_spikes(output_dir, theme)
    generate_depth_vs_magnitude_scatter(df, output_dir, theme)
    generate_latency_benchmark_chart(output_dir, theme)
    generate_regional_risk_matrix(output_dir, theme)
    generate_architecture_infographic(output_dir, theme)
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Render Scientific Visuals")
    parser.add_argument("--theme", choices=["light", "dark", "both"], default="light", help="Color theme (default: light)")
    parser.add_argument("--outdir", type=str, default=str(DEFAULT_OUTPUT_DIR), help="Output directory")
    args = parser.parse_args()

    if args.theme == "both":
        render_all("light", Path(args.outdir))
        render_all("dark", Path(args.outdir) / "dark")
    else:
        render_all(args.theme, Path(args.outdir))
