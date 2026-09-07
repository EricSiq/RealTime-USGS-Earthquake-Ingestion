"""
Comprehensive Pipeline Test Suite
Tests all stages of the Hadoop Ecosystem Seismic Pipeline:
1. Ingestion & Deduplication (USGS GeoJSON live feeds)
2. HDFS Partitioning and raw storage layout
3. Batch Processing & Hive/Pig schema unnesting
4. Columnar Parquet integrity & Gutenberg-Richter energy calculations
5. HBase composite reverse-timestamp row key ordering & O(1) prefix seek
6. Latency Benchmark verification (Hive vs HBase speedup)
7. Visuals generation integrity (300 DPI PNGs & HTML gallery)
"""

import os
import sys
import json
import pytest
from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from ingestion.usgs_streamer import USGSStreamer, USGS_ALL_HOUR, USGS_ALL_DAY
from processing.batch_processor import classify_region, calculate_seismic_energy, process_raw_hdfs_batches
from hbase.loader import HBaseSeismicStore, MAX_LONG
from benchmark.latency_comparison import run_benchmark, benchmark_hbase_lookup, benchmark_hive_scan

DATA_DIR = BASE_DIR / "data"
RAW_HDFS_DIR = DATA_DIR / "hdfs" / "raw" / "earthquakes"
PROCESSED_FILE = DATA_DIR / "hdfs" / "processed" / "earthquakes" / "earthquakes_processed.parquet"
ANALYTICS_DIR = DATA_DIR / "analytics"
OUTPUTS_DIR = BASE_DIR / "outputs"
VISUALS_DIR = OUTPUTS_DIR / "visuals"

# =============================================================================
# 1. INGESTION & DEDUPLICATION TESTS
# =============================================================================
class TestIngestion:
    def test_usgs_live_endpoint_connectivity(self):
        """Verify USGS endpoint responds with valid GeoJSON FeatureCollection."""
        streamer = USGSStreamer(feed_url=USGS_ALL_HOUR)
        payload = streamer.fetch_feed()
        assert payload is not None
        assert payload.get("type") == "FeatureCollection"
        assert "metadata" in payload
        assert "features" in payload
        assert isinstance(payload["features"], list)

    def test_feature_schema_attributes(self):
        """Verify each feature contains required properties and coordinates."""
        streamer = USGSStreamer(feed_url=USGS_ALL_HOUR)
        payload = streamer.fetch_feed()
        features = payload.get("features", [])
        if features:
            feat = features[0]
            assert "id" in feat
            assert "properties" in feat
            assert "geometry" in feat
            props = feat["properties"]
            coords = feat["geometry"]["coordinates"]
            assert len(coords) >= 2  # [longitude, latitude, (depth)]
            assert "time" in props
            assert "place" in props

    def test_deduplication_logic(self):
        """Verify identical features are not duplicated across ingestion runs."""
        streamer = USGSStreamer(feed_url=USGS_ALL_HOUR)
        mock_feat = {
            "type": "Feature",
            "id": "mock_test_quake_001",
            "properties": {"mag": 3.5, "place": "Test Bay, CA", "time": 1725690000000},
            "geometry": {"type": "Point", "coordinates": [-122.0, 37.0, 10.0]}
        }
        mock_payload = {"type": "FeatureCollection", "features": [mock_feat]}
        
        # First cycle lands the event
        new_cnt1, _, _ = streamer.partition_and_land(mock_payload)
        # Second cycle should detect it as already seen
        new_cnt2, _, _ = streamer.partition_and_land(mock_payload)
        
        assert new_cnt2 == 0, "Deduplication failed: Duplicate feature was re-ingested."

# =============================================================================
# 2. HDFS PARTITIONING & STORAGE LAYOUT TESTS
# =============================================================================
class TestHDFSStorageLayout:
    def test_hdfs_directory_structure(self):
        """Verify HDFS landing directory conforms to /raw/earthquakes/YYYY=.../MM=.../DD=.../HH=.../"""
        assert RAW_HDFS_DIR.exists()
        raw_files = list(RAW_HDFS_DIR.glob("**/*.json"))
        assert len(raw_files) > 0, "No raw JSON files found in HDFS landing hierarchy."
        
        # Check that path contains partition keys
        sample_path = str(raw_files[0])
        assert "YYYY=" in sample_path
        assert "MM=" in sample_path
        assert "DD=" in sample_path
        assert "HH=" in sample_path

# =============================================================================
# 3. BATCH PROCESSING & ANALYTICS TESTS
# =============================================================================
class TestBatchProcessing:
    def test_region_classification(self):
        """Verify geographic classification mappings."""
        assert classify_region("12 km SW of Petrolia, California") == "CALIFORNIA"
        assert classify_region("Southern Alaska") == "ALASKA"
        assert classify_region("Hawaii Region") == "HAWAII"
        assert classify_region("near the coast of Honshu, Japan") == "JAPAN"
        assert classify_region("Off the coast of Bio-Bio, Chile") == "CHILE"
        assert classify_region("Tonga Islands Region") == "SOUTH_PACIFIC"
        assert classify_region("Unknown Location in Ocean") == "GLOBAL_OTHER"

    def test_gutenberg_richter_energy_formula(self):
        """Verify radiated seismic energy computation: E = 10^(4.8 + 1.5 * M)."""
        # M = 2.0 -> 10^(4.8 + 3.0) = 10^7.8 = 63,095,734 Joules
        expected_m2 = 10.0 ** (4.8 + 1.5 * 2.0)
        calc_m2 = calculate_seismic_energy(2.0)
        assert abs(calc_m2 - expected_m2) < 1.0

        # Energy should increase by ~31.6x for each 1 unit increase in magnitude
        e4 = calculate_seismic_energy(4.0)
        e5 = calculate_seismic_energy(5.0)
        ratio = e5 / e4
        assert abs(ratio - (10.0**1.5)) < 0.1

    def test_processed_parquet_integrity(self):
        """Verify processed Parquet dataset exists, has records, and has clean schemas."""
        assert PROCESSED_FILE.exists(), "Processed Parquet file not found."
        df = pd.read_parquet(PROCESSED_FILE)
        assert len(df) > 0
        
        required_cols = [
            "event_id", "magnitude", "place", "region", "event_time", 
            "epoch_millis", "longitude", "latitude", "depth_km", 
            "felt_reports", "alert_level", "tsunami_flag", "seismic_energy_joules"
        ]
        for col in required_cols:
            assert col in df.columns, f"Missing required column {col} in processed Parquet dataset."
        
        # No null event IDs or null magnitudes
        assert df["event_id"].isnull().sum() == 0
        assert df["magnitude"].isnull().sum() == 0
        assert df["depth_km"].isnull().sum() == 0

    def test_analytical_rollups_generated(self):
        """Verify Hive-equivalent summary tables exist and have valid aggregations."""
        hourly_file = ANALYTICS_DIR / "hourly_activity.csv"
        regional_file = ANALYTICS_DIR / "regional_summary.csv"
        depth_file = ANALYTICS_DIR / "depth_summary.csv"

        assert hourly_file.exists()
        assert regional_file.exists()
        assert depth_file.exists()

        hdf = pd.read_csv(hourly_file)
        assert len(hdf) > 0
        assert "total_events" in hdf.columns

        rdf = pd.read_csv(regional_file)
        assert len(rdf) > 0
        assert "event_count" in rdf.columns

# =============================================================================
# 4. HBASE SERVING & REVERSE-TIMESTAMP TESTS
# =============================================================================
class TestHBaseServing:
    def test_reverse_timestamp_rowkey_format(self):
        """Verify row key format: <REGION>#<MAX_LONG - epoch_millis:019d>."""
        store = HBaseSeismicStore()
        epoch = 1725692485000
        rk = store._generate_row_key("CALIFORNIA", epoch)
        
        expected_reversed = MAX_LONG - epoch
        assert rk == f"CALIFORNIA#{expected_reversed:019d}"
        assert len(rk.split("#")[1]) == 19

    def test_reverse_timestamp_ordering(self):
        """Verify that more recent events appear BEFORE older events lexicographically."""
        store = HBaseSeismicStore()
        t_older = 1725600000000
        t_newer = 1725690000000  # 90,000 seconds later

        rk_older = store._generate_row_key("JAPAN", t_older)
        rk_newer = store._generate_row_key("JAPAN", t_newer)

        # In lexicographical order:
        keys = sorted([rk_older, rk_newer])
        assert keys[0] == rk_newer, "Reverse-timestamp ordering failed: Newer event did not sort first."
        assert keys[1] == rk_older

    def test_hbase_prefix_scan(self):
        """Verify scan_latest_by_region returns records in descending order of time."""
        store = HBaseSeismicStore()
        store.load_from_parquet()
        results = store.scan_latest_by_region("CALIFORNIA", limit=5)
        assert len(results) > 0

        # Check timestamp monotonically decreases
        timestamps = [r["epoch_millis"] for r in results]
        for i in range(len(timestamps) - 1):
            assert timestamps[i] >= timestamps[i+1], "HBase results are not in descending chronological order."

# =============================================================================
# 5. LATENCY BENCHMARK TESTS
# =============================================================================
class TestBenchmark:
    def test_latency_benchmark_execution(self):
        """Verify benchmark completes and demonstrates HBase speedup over Hive."""
        b_res = run_benchmark()
        assert b_res is not None
        assert "speedup_factor" in b_res
        assert b_res["speedup_factor"] > 100.0, "Expected HBase to be at least 100x faster than Hive."
        assert b_res["hbase"]["mean_ms"] < 100.0, "HBase mean latency should be sub-100ms."
        assert b_res["hive"]["mean_ms"] > 1000.0, "Hive mean latency should reflect batch scan overhead."

# =============================================================================
# 6. HIGH-QUALITY VISUALS VERIFICATION TESTS
# =============================================================================
class TestVisuals:
    EXPECTED_VISUALS = [
        "01_global_seismic_map.png",
        "02_seismic_drumbeat_strip.png",
        "03_hourly_activity_spikes.png",
        "04_depth_vs_magnitude_scatter.png",
        "05_hive_vs_hbase_latency.png",
        "06_regional_risk_matrix.png",
        "07_hadoop_architecture_infographic.png"
    ]

    def test_light_mode_visuals_exist_and_non_empty(self):
        """Verify all 7 light-mode PNG figures exist and are > 50 KB."""
        for fname in self.EXPECTED_VISUALS:
            fpath = VISUALS_DIR / fname
            assert fpath.exists(), f"Missing visual: {fname}"
            size_kb = fpath.stat().st_size / 1024.0
            assert size_kb > 50.0, f"Visual {fname} appears truncated or corrupt (size: {size_kb:.1f} KB)."

    def test_dark_mode_visuals_exist(self):
        """Verify dark-mode companion figures also exist."""
        dark_dir = VISUALS_DIR / "dark"
        for fname in self.EXPECTED_VISUALS:
            fpath = dark_dir / fname
            assert fpath.exists(), f"Missing dark-mode visual: {fname}"
            assert fpath.stat().st_size > 50000

    def test_gallery_html_exists(self):
        """Verify outputs/visuals_gallery.html exists and contains theme toggle."""
        gallery_file = OUTPUTS_DIR / "visuals_gallery.html"
        assert gallery_file.exists()
        content = gallery_file.read_text(encoding="utf-8")
        assert "toggleTheme" in content
        assert "01_global_seismic_map.png" in content
