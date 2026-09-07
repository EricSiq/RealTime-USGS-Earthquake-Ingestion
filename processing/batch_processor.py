"""
Batch Processing Engine (Hadoop / Hive / Pig Dual-Track Simulator)
Reads raw partitioned GeoJSON from the HDFS landing zone, applies schema unnesting,
calculates Gutenberg-Richter radiated seismic energy, computes regional classifications,
and exports clean partitioned Parquet/CSV data and analytical aggregate tables.
"""

import os
import sys
import json
import math
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_HDFS_DIR = BASE_DIR / "data" / "hdfs" / "raw" / "earthquakes"
PROCESSED_HDFS_DIR = BASE_DIR / "data" / "hdfs" / "processed" / "earthquakes"
ANALYTICS_DIR = BASE_DIR / "data" / "analytics"

import re

def classify_region(place: str) -> str:
    if not place:
        return "GLOBAL_OTHER"
    p = place.upper()
    if "CALIFORNIA" in p or re.search(r"\bCA\b", p):
        return "CALIFORNIA"
    if "ALASKA" in p or re.search(r"\bAK\b", p):
        return "ALASKA"
    if "HAWAII" in p or re.search(r"\bHI\b", p):
        return "HAWAII"
    if "JAPAN" in p:
        return "JAPAN"
    if "INDONESIA" in p:
        return "INDONESIA"
    if "CHILE" in p:
        return "CHILE"
    if "PHILIPPINES" in p:
        return "PHILIPPINES"
    if "MEXICO" in p:
        return "MEXICO"
    if "NEW ZEALAND" in p or "KERMADEC" in p:
        return "NEW_ZEALAND"
    if "PUERTO RICO" in p or re.search(r"\bPR\b", p):
        return "PUERTO_RICO"
    if "TURKEY" in p or "TÜRKIYE" in p or "KAHRAMANMARAS" in p or "PAZARCIK" in p:
        return "TURKEY"
    if "FIJI" in p or "TONGA" in p or "VANUATU" in p or "LOYALTY" in p or "PAPUA" in p or "SOLOMON" in p:
        return "SOUTH_PACIFIC"
    if "TAIWAN" in p:
        return "TAIWAN"
    if "ICELAND" in p:
        return "ICELAND"
    if "GREECE" in p or "ITALY" in p:
        return "MEDITERRANEAN"
    if "SANDWICH" in p:
        return "SOUTH_ATLANTIC"
    if "JAMAICA" in p or "CARIBBEAN" in p or "HAITI" in p:
        return "CARIBBEAN"
    return "GLOBAL_OTHER"

def calculate_seismic_energy(mag: float) -> float:
    """Gutenberg-Richter empirical energy formula: E = 10^(4.8 + 1.5 * M) in Joules"""
    try:
        return 10.0 ** (4.8 + 1.5 * float(mag))
    except (ValueError, OverflowError):
        return 0.0

def process_raw_hdfs_batches():
    print("=" * 65)
    print("Executing Batch Processing on HDFS Raw Landing Zone")
    print(f"Source Directory: {RAW_HDFS_DIR}")
    print("=" * 65)

    raw_files = list(RAW_HDFS_DIR.glob("**/*.json"))
    if not raw_files:
        print("[ERROR] No raw GeoJSON batch files found in HDFS landing zone!")
        return None

    all_records = []
    seen_ids = set()

    for fpath in raw_files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                payload = json.load(f)
                features = payload.get("features", [])
                for feat in features:
                    fid = feat.get("id")
                    if not fid or fid in seen_ids:
                        continue
                    seen_ids.add(fid)

                    props = feat.get("properties", {})
                    geom = feat.get("geometry", {})
                    coords = geom.get("coordinates", [0.0, 0.0, 0.0])

                    mag = props.get("mag")
                    if mag is None:
                        continue
                    mag = float(mag)

                    epoch_ms = props.get("time", 0)
                    dt_obj = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
                    dt_str = dt_obj.strftime("%Y-%m-%d")

                    place = props.get("place", "Unknown")
                    lon = float(coords[0]) if len(coords) > 0 else 0.0
                    lat = float(coords[1]) if len(coords) > 1 else 0.0
                    depth = float(coords[2]) if len(coords) > 2 else 0.0

                    all_records.append({
                        "event_id": fid,
                        "magnitude": mag,
                        "place": place,
                        "region": classify_region(place),
                        "event_time": dt_obj.isoformat(),
                        "epoch_millis": epoch_ms,
                        "longitude": lon,
                        "latitude": lat,
                        "depth_km": depth,
                        "felt_reports": props.get("felt") or 0,
                        "alert_level": props.get("alert") or "none",
                        "tsunami_flag": props.get("tsunami") or 0,
                        "significance": props.get("sig") or 0,
                        "magnitude_type": props.get("magType") or "unknown",
                        "seismic_energy_joules": calculate_seismic_energy(mag),
                        "dt": dt_str
                    })
        except Exception as e:
            print(f"[WARN] Error parsing {fpath.name}: {e}")

    df = pd.DataFrame(all_records)
    print(f"Successfully processed and cleansed {len(df)} distinct seismic events.")

    # Save to processed HDFS partitioned columnar Parquet & CSV
    PROCESSED_HDFS_DIR.mkdir(parents=True, exist_ok=True)
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    parquet_file = PROCESSED_HDFS_DIR / "earthquakes_processed.parquet"
    csv_file = PROCESSED_HDFS_DIR / "earthquakes_processed.csv"
    df.to_parquet(parquet_file, index=False, engine="pyarrow", compression="snappy")
    df.to_csv(csv_file, index=False)

    print(f"Cleaned dataset written to:")
    print(f"  -> {parquet_file.relative_to(BASE_DIR)}")
    print(f"  -> {csv_file.relative_to(BASE_DIR)}")

    # -------------------------------------------------------------
    # Compute Hive-Equivalent Analytical Rollups
    # -------------------------------------------------------------
    # 1. Hourly, Daily & Monthly Activity Aggregation
    df["dt_utc"] = pd.to_datetime(df["epoch_millis"], unit="ms", utc=True)
    df["hour_window"] = df["dt_utc"].dt.floor("h")
    hourly_df = df.groupby("hour_window").agg(
        total_events=("event_id", "count"),
        avg_magnitude=("magnitude", "mean"),
        max_magnitude=("magnitude", "max"),
        total_energy_joules=("seismic_energy_joules", "sum")
    ).reset_index().sort_values("hour_window", ascending=True)
    hourly_file = ANALYTICS_DIR / "hourly_activity.csv"
    hourly_df.to_csv(hourly_file, index=False)

    df["day_window"] = df["dt_utc"].dt.floor("D")
    daily_df = df.groupby("day_window").agg(
        total_events=("event_id", "count"),
        avg_magnitude=("magnitude", "mean"),
        max_magnitude=("magnitude", "max"),
        total_energy_joules=("seismic_energy_joules", "sum")
    ).reset_index().sort_values("day_window", ascending=True)
    daily_file = ANALYTICS_DIR / "daily_activity.csv"
    daily_df.to_csv(daily_file, index=False)

    df["month_window"] = pd.to_datetime(df["dt_utc"].dt.strftime("%Y-%m-01"), utc=True)
    monthly_df = df.groupby("month_window").agg(
        total_events=("event_id", "count"),
        avg_magnitude=("magnitude", "mean"),
        max_magnitude=("magnitude", "max"),
        total_energy_joules=("seismic_energy_joules", "sum")
    ).reset_index().sort_values("month_window", ascending=True)
    monthly_file = ANALYTICS_DIR / "monthly_activity.csv"
    monthly_df.to_csv(monthly_file, index=False)

    # 2. Regional Risk Scorecard
    regional_df = df.groupby("region").agg(
        event_count=("event_id", "count"),
        avg_magnitude=("magnitude", "mean"),
        max_magnitude=("magnitude", "max"),
        avg_depth_km=("depth_km", "mean"),
        tsunami_warnings=("tsunami_flag", "sum"),
        total_felt_reports=("felt_reports", "sum"),
        cumulative_energy_joules=("seismic_energy_joules", "sum")
    ).reset_index().sort_values("event_count", ascending=False)
    regional_file = ANALYTICS_DIR / "regional_summary.csv"
    regional_df.to_csv(regional_file, index=False)

    # 3. Depth Layering Distribution
    def depth_cat(d):
        if d < 35.0:
            return "Shallow (0-35 km)"
        elif d <= 150.0:
            return "Intermediate (35-150 km)"
        else:
            return "Deep (150-700 km)"

    df["depth_category"] = df["depth_km"].apply(depth_cat)
    depth_df = df.groupby("depth_category").agg(
        event_count=("event_id", "count"),
        avg_magnitude=("magnitude", "mean"),
        max_magnitude=("magnitude", "max")
    ).reset_index()
    depth_file = ANALYTICS_DIR / "depth_summary.csv"
    depth_df.to_csv(depth_file, index=False)

    print("\nAnalytical Rollups generated:")
    print(f"  -> {hourly_file.relative_to(BASE_DIR)} ({len(hourly_df)} hours)")
    print(f"  -> {regional_file.relative_to(BASE_DIR)} ({len(regional_df)} regions)")
    print(f"  -> {depth_file.relative_to(BASE_DIR)} ({len(depth_df)} depth strata)")
    print("=" * 65)

    return df

if __name__ == "__main__":
    process_raw_hdfs_batches()
