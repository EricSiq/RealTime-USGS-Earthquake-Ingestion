# Project Task List: BDA Hadoop Ecosystem Case Study

**Topic:** Real-Time Global Earthquake Streaming & Analytics using Hadoop Ecosystem  
**Goal:** Build an end-to-end Big Data Analytics case study demonstrating why specific Hadoop ecosystem components (HDFS, YARN, Hive, Pig, HBase, ZooKeeper) exist, backed by **non-negotiable high-quality visuals** and a conference/academic-grade BDA poster.

---

## PART 1: Tasks for You (User / Human Checklist)

These are the immediate steps and environment decisions you need to take on your machine:

- [ ] **Task U1: Choose Execution Environment (Docker vs Native/Hybrid)**
  - *Recommendation:* Install **Docker Desktop for Windows** (with WSL2 backend enabled).
  - *Alternative:* If your machine cannot run Docker Desktop (due to RAM/permissions), we can configure a hybrid setup: Python-based data ingestion & direct USGS pipeline + local Hadoop/Hive emulator + high-fidelity visualization suite.
  - *Decision needed:* Confirm whether you will install/run Docker Desktop, or if you prefer a hybrid/local simulation mode.

- [ ] **Task U2: Allocate Machine Resources (If Docker)**
  - Open Docker Desktop > Settings > Resources.
  - Allocate **at least 6 GB to 8 GB RAM** and 4 CPU cores (Hadoop daemons, Hive metastore, and HBase together require sufficient heap memory).

- [ ] **Task U3: Verify Local Python Environment**
  - Verify Python 3.11 is present: `python --version` (Detected: Python 3.11.15 ✅).
  - You will run `pip install -r requirements.txt` once created in Phase 1 (libraries: `requests`, `pandas`, `plotly`, `streamlit`, `dash`, `kaleido`, `pillow`, `matplotlib`).

- [ ] **Task U4: Network & USGS Feed Connectivity**
  - Ensure your system can reach `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson` without proxy or VPN blocking.

- [ ] **Task U5: Academic Rubric & Poster Specifications Review**
  - Clarify any specific poster dimensions required by your institution (e.g., A0, A1, 36x48 inches, or 4:3 / 16:9 digital display).
  - Confirm if any specific component is strictly mandated (e.g., Pig vs Hive, or both).

---

## PART 2: Methodical Tasks for Antigravity (AI Vibe-Coding Plan)

These tasks will be executed methodically in strict sequence once approved:

### Phase 1: Infrastructure & Environment Setup
- [x] **Task A1.1: Docker Compose Stack Definition (`docker-compose.yml`)** ✅
- [x] **Task A1.2: Cluster Healthcheck & Verification Script (`scripts/verify_cluster.py`)** ✅

### Phase 2: Live Ingestion & HDFS Partitioning Engine
- [x] **Task A2.1: USGS GeoJSON Streaming Ingestion Script (`ingestion/usgs_streamer.py`)** ✅
- [x] **Task A2.2: Automated Orchestration & Ingestion Daemon (`ingestion/daemon.py`)** ✅

### Phase 3: Hadoop Core & Batch Processing (HDFS, Hive, Pig)
- [x] **Task A3.1: HDFS Directory Hierarchy & Storage Layout** ✅
- [x] **Task A3.2: Hive External Table & JSON SerDe DDL (`hive/schema.sql`)** ✅
- [x] **Task A3.3: Analytical Hive / Pig Transformation Scripts (`hive/analytics.sql`, `pig/flatten_quakes.pig`, `processing/batch_processor.py`)** ✅

### Phase 4: Near-Real-Time Serving Store (HBase & ZooKeeper)
- [x] **Task A4.1: HBase Schema & Row-Key Architecture (`hbase/schema.hb`)** ✅
- [x] **Task A4.2: HBase Batch/Stream Loader (`hbase/loader.py`)** ✅
- [x] **Task A4.3: Latency Benchmark Module (`benchmark/latency_comparison.py`)** ✅

### Phase 5: High-Quality Visualizations & Interactive Dashboard (Non-Negotiable)
- [x] **Task A5.1: Interactive Dark-Mode Analytics Dashboard (`viz/app.py`)** ✅
- [x] **Task A5.2: Visual Component 1 — Live Global Geospatial Map (`outputs/visuals/01_global_seismic_map.png`)** ✅
- [x] **Task A5.3: Visual Component 2 — Seismic Strip & Alert Visualizer (`outputs/visuals/02_seismic_drumbeat_strip.png`)** ✅
- [x] **Task A5.4: Visual Component 3 — Rolling Hourly Activity Frequency (`outputs/visuals/03_hourly_activity_spikes.png`)** ✅
- [x] **Task A5.5: Visual Component 4 — Depth vs Magnitude Analytical Scatter (`outputs/visuals/04_depth_vs_magnitude_scatter.png`)** ✅
- [x] **Task A5.6: Visual Component 5 — Hive vs HBase Latency Benchmark Graphic (`outputs/visuals/05_hive_vs_hbase_latency.png`)** ✅
- [x] **Task A5.7: Visual Component 6 & 7 — Regional Matrix & Hadoop Architecture (`outputs/visuals/06_regional_risk_matrix.png`, `07_hadoop_architecture_infographic.png`)** ✅
- [x] **Task A5.8: High-DPI Visuals Gallery Viewer (`outputs/visuals_gallery.html`)** ✅

### Phase 6: BDA Poster Assets & Academic Report Production
- [ ] **Task A6.1: Architecture & Dataflow Diagram (Vector/High-Res)**
  - Create publication-ready architecture diagram displaying USGS API -> Ingestor -> HDFS -> Hive/Pig -> HBase/ZooKeeper -> Visualization Layer.
- [ ] **Task A6.2: High-Resolution Poster Layout (`poster/generate_poster.py` & HTML/CSS layout)**
  - Generate an academic BDA poster in standard 36x48 inch / high-DPI format with 6-section grid:
    1. Title & Team Banner
    2. Problem Statement & Real-World Motivation
    3. Hadoop Ecosystem Architecture & Data Flow
    4. Data Engineering Methodology (SerDe, Partitioning, Reverse Row-Key)
    5. High-Impact Visual Analytics Panels
    6. Performance Benchmark (Hive vs HBase) & Conclusion
- [ ] **Task A6.3: Standalone Export Package**
  - Automated export script to generate all figures as 300+ DPI PNG/SVG assets and a printable PDF poster.
