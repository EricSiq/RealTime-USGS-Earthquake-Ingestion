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
- [ ] **Task A1.1: Docker Compose Stack Definition (`docker-compose.yml`)**
  - Define multi-container Hadoop cluster: `namenode`, `datanode`, `resourcemanager`, `nodemanager`, `hive-server`, `hive-metastore`, `hbase-master`, `hbase-regionserver`, `zookeeper`.
  - Configure port mappings (Namenode UI `9870`, ResourceManager `8088`, Hive `10000`, HBase UI `16010`).
  - Provide a fallback lightweight standalone container / mock pipeline mode for testing without bringing down system memory.
- [ ] **Task A1.2: Cluster Healthcheck & Verification Script**
  - Create `scripts/verify_cluster.py` to ping HDFS, WebHDFS, YARN, and verify port readiness.

### Phase 2: Live Ingestion & HDFS Partitioning Engine
- [ ] **Task A2.1: USGS GeoJSON Streaming Ingestion Script (`ingestion/usgs_streamer.py`)**
  - Connect to USGS live API (e.g., "all_hour" polled every 60 seconds or "all_day" feed).
  - Implement deduplication using USGS unique `id` and timestamp tracking.
  - Partition raw JSON files by time window: `/raw/earthquakes/YYYY/MM/DD/HH/quakes_<timestamp>.json`.
  - Support dual sink: directly push to HDFS via WebHDFS/Docker exec, and preserve a local mirrored cache for zero-friction visualization.
- [ ] **Task A2.2: Automated Orchestration & Ingestion Daemon (`ingestion/daemon.py`)**
  - Implement a configurable daemon/cron simulator to run every N minutes and log incoming seismic event batches.

### Phase 3: Hadoop Core & Batch Processing (HDFS, Hive, Pig)
- [ ] **Task A3.1: HDFS Directory Hierarchy & Storage Layout**
  - Setup `/raw/earthquakes/...` (immutable raw zone) and `/processed/earthquakes/...` (clean analytical zone).
- [ ] **Task A3.2: Hive External Table & JSON SerDe DDL (`hive/schema.sql`)**
  - Write Hive DDL for `earthquakes_raw` using `org.apache.hive.hcatalog.data.JsonSerDe`.
  - Create partitioned ORC/Parquet analytical tables `earthquakes_processed` partitioned by `dt` and `alert_level`.
- [ ] **Task A3.3: Analytical Hive / Pig Transformation Scripts (`hive/analytics.sql`, `pig/flatten_quakes.pig`)**
  - Compute hourly seismic activity counts and rolling averages.
  - Calculate magnitude vs depth distribution metrics and regional seismic danger scores.
  - Pig script to demonstrate procedural schema extraction and JSON unnesting.

### Phase 4: Near-Real-Time Serving Store (HBase & ZooKeeper)
- [ ] **Task A4.1: HBase Schema & Row-Key Architecture (`hbase/schema.hb`)**
  - Design table `seismic_events` with column families: `event` (mag, depth, lat, lon, place, time) and `meta` (alert, tsunami, felt, status).
  - Implement composite reverse-timestamp row-key: `<region_code>#<Long.MAX_VALUE - timestamp>` for $O(1)$ latest-first point queries.
- [ ] **Task A4.2: HBase Batch/Stream Loader (`hbase/loader.py`)**
  - Ingest processed earthquake records into HBase.
- [ ] **Task A4.3: Latency Benchmark Module (`benchmark/latency_comparison.py`)**
  - Execute identical queries on Hive (batch scan over HDFS) vs HBase (indexed row-key lookup).
  - Generate quantifiable empirical proof showing HBase latency (<30ms) vs Hive latency (8–20s).

### Phase 5: High-Quality Visualizations & Interactive Dashboard (Non-Negotiable)
- [ ] **Task A5.1: Interactive Dark-Mode Analytics Dashboard (`viz/app.py`)**
  - Build a sleek, modern web dashboard (Streamlit / Plotly Dark Obsidian theme `#0b0f19`).
  - Implement responsive controls: time-window selector, minimum magnitude slider, regional filters.
- [ ] **Task A5.2: Visual Component 1 — Live Animated Global Geospatial Map**
  - 3D / Dark-basemap WebGL map plotting seismic coordinates.
  - Circle radius dynamically scaled by magnitude ($M_w$ Richter scale); color gradient by hypocenter depth (shallow coral-red to deep oceanic cyan).
  - Time-slider animation showing earthquake propagation over the past 24 hours.
- [ ] **Task A5.3: Visual Component 2 — Seismic Strip & Spike Alert Visualizer**
  - Strip plot of magnitude over continuous time showing seismic clustering/aftershocks.
  - Rolling hourly frequency area chart with dynamic threshold spike detection (proving live streaming ingestion).
- [ ] **Task A5.4: Visual Component 3 — Depth vs Magnitude Analytical Scatter**
  - Scientific scatter plot with marginal distribution histograms and regional color encoding.
- [ ] **Task A5.5: Visual Component 4 — Hive vs HBase Latency Benchmark Graphic**
  - Side-by-side benchmark visual comparing query response times to clearly justify the role of NoSQL HBase vs Batch Hive.

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
