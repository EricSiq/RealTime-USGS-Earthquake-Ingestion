# Real-Time USGS Earthquake Ingestion & Hadoop Ecosystem Analytics

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Hadoop 3.2](https://img.shields.io/badge/hadoop-3.2.1-red.svg)](https://hadoop.apache.org/)
[![Hive 2.3](https://img.shields.io/badge/hive-2.3.2-yellow.svg)](https://hive.apache.org/)
[![HBase 2.4](https://img.shields.io/badge/hbase-2.4-green.svg)](https://hbase.apache.org/)
[![Status](https://img.shields.io/badge/status-active-success.svg)]()

A comprehensive Big Data Analytics (BDA) real-world case study examining **why specific Hadoop ecosystem components exist** and how they integrate to solve high-velocity ingestion, distributed batch warehousing, and sub-millisecond real-time serving.

---

## 🌟 Executive Summary

This project ingests live global seismic telemetry from the **United States Geological Survey (USGS) Earthquake Hazards Program**, partitions and stores raw GeoJSON streams in **HDFS**, executes procedural ETL with **Apache Pig**, performs complex OLAP aggregations with **Apache Hive**, and enables sub-millisecond point queries with **Apache HBase** using a composite **reverse-timestamp row key**.

### Key Empirical Finding: Why HBase?
When answering operational point queries (e.g., *"Retrieve the 10 most recent earthquakes in California"*):
- **Apache Hive (OLAP Distributed Scan over HDFS):** `~6,255 ms`
- **Apache HBase (Indexed Row-Key Prefix Seek):** `0.28 ms`
- **Empirical Speedup:** **`> 21,000× faster`**

---

## 📐 Architecture & Ecosystem Component Breakdown

| Component | Role | Case Study Responsibility |
| :--- | :--- | :--- |
| **HDFS** | Distributed Data Lake | Fault-tolerant storage partitioned hierarchically (`/raw/earthquakes/YYYY/MM/DD/HH/`) to resolve the small-file problem. |
| **YARN** | Resource Negotiator | Manages compute memory and vCores across MapReduce, Hive, and Pig tasks. |
| **Apache Hive** | SQL-on-Hadoop (OLAP) | Applies schema-on-read via `JsonSerDe` and builds partitioned, Snappy-compressed ORC analytical tables. |
| **Apache Pig** | Procedural ETL | Unnests complex GeoJSON coordinates and properties into structured tabular tuples. |
| **Apache HBase** | NoSQL Serving (OLTP) | Stores events under row key `region#<Long.MAX_VALUE - timestamp>` for $O(1)$ prefix seeks. |
| **Apache ZooKeeper** | Quorum Consensus | Coordinates HBase master election and RegionServer metadata. |
| **Interactive Viz Layer** | WebGL & High-DPI Charts | Streamlit & Plotly dark-themed dashboard and 300 DPI scientific figures. |

---

## 🖼️ High-Quality Visual Deliverables

All high-resolution figures are saved in [`outputs/visuals/`](outputs/visuals/):
1. **`01_global_seismic_map.png`**: Global equirectangular projection of planetary epicenters scaled by magnitude and colored by focal depth.
2. **`02_seismic_drumbeat_strip.png`**: Temporal drumbeat strip revealing energy clustering, aftershocks, and severity alert bands.
3. **`03_hourly_activity_spikes.png`**: Hourly ingestion volume curve with dynamic anomaly threshold line ($+1.8\sigma$).
4. **`04_depth_vs_magnitude_scatter.png`**: Scientific scatter with marginal distributions exploring Wadati-Benioff subduction zone dynamics.
5. **`05_hive_vs_hbase_latency.png`**: Side-by-side empirical benchmark graphic proving the $>21,000\times$ HBase latency advantage.
6. **`06_regional_risk_matrix.png`**: Regional seismic activity and peak magnitude scorecard.
7. **`07_hadoop_architecture_infographic.png`**: Complete end-to-end ecosystem component diagram.

> 💡 **Tip:** Open [`outputs/visuals_gallery.html`](outputs/visuals_gallery.html) in any browser to review all 7 figures in a responsive, dark-mode visual gallery!

---

## 🚀 Quickstart & Execution

### 1. Setup Environment
```bash
# Clone the repository
git clone https://github.com/EricSiq/RealTime-USGS-Earthquake-Ingestion.git
cd RealTime-USGS-Earthquake-Ingestion

# Create virtual environment and install dependencies
python -m venv .venv
# On Windows:
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Ingest Live USGS Telemetry
```bash
# Poll past 24-hour feed
python ingestion/usgs_streamer.py

# Or run the ingestion daemon
python ingestion/daemon.py --feed week --interval 60 --cycles 3
```

### 3. Run Batch Processing (HDFS, Hive, Pig Simulator)
```bash
python processing/batch_processor.py
```

### 4. Load into HBase & Run Latency Benchmark
```bash
python hbase/loader.py
python benchmark/latency_comparison.py
```

### 5. Render All 300 DPI Figures & Launch Web Dashboard
```bash
# Generate PNG figures
python viz/charts.py

# Launch interactive Streamlit dashboard
streamlit run viz/app.py
```

### 6. (Optional) Run Full Multi-Container Docker Cluster
If running Docker Desktop with at least 6-8 GB RAM allocated:
```bash
docker compose up -d
python scripts/verify_cluster.py
```

---

## 📚 Technical Documentation
- [tasks.md](tasks.md): Implementation checklist and phase breakdown.
- [ARCHITECTURE.md](ARCHITECTURE.md): Deep-dive into distributed systems design and trade-offs.
- [PIPELINE_AND_SCHEMA.md](PIPELINE_AND_SCHEMA.md): Complete Hive DDL, Pig Latin scripts, and HBase table schemas.
- [VISUALS_AND_POSTER_SPEC.md](VISUALS_AND_POSTER_SPEC.md): Design tokens, typography, and visual guidelines.