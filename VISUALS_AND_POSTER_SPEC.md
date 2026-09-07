# Visualization & Poster Specification: Non-Negotiable High-Quality Visuals

**Project Title:** Real-Time Global Earthquake Analytics via Hadoop Ecosystem  
**Requirement:** High-Quality Visuals as a Non-Negotiable Standard  
**Target Medium:** Academic BDA Poster (300 DPI Print / 4K Digital) & Interactive Dark-Theme Analytics Dashboard  

---

## 1. Design System & Aesthetic Standard

To ensure academic rigor combined with modern visual elegance, all visual artifacts adhere to a cohesive, high-contrast design system.

### 1.1 Color Palette
- **Canvas / Background:** `#080c14` (Deep Space Obsidian)
- **Card / Surface Container:** `#111827` (Matte Slate with subtle 1px border `#1f293d`)
- **Seismic Magnitude Gradient:**
  - $M < 3.0$ (Minor): `#38bdf8` (Ice Blue)
  - $3.0 \le M < 4.5$ (Moderate): `#facc15` (Amber Gold)
  - $4.5 \le M < 6.0$ (Strong): `#fb923c` (Vibrant Tangerine)
  - $M \ge 6.0$ (Severe / Major): `#ff1744` (Crimson Pulse)
- **Depth Color Encoding (Hypocenter):**
  - Shallow ($0–35\text{ km}$): `#ff3366` (Coral Red — high surface hazard)
  - Intermediate ($35–150\text{ km}$): `#ffaa00` (Gold Warning)
  - Deep ($150–700\text{ km}$): `#00e5ff` (Oceanic Cyan — subduction slabs)
- **Typography:**
  - Headers: **Outfit** / **Inter** (Semi-bold / Extra-bold)
  - Metric Counters & Latency Timings: **JetBrains Mono** / **Fira Code**

---

## 2. Core Visual Artifacts Specification

```
+---------------------------------------------------------------------------------------------------+
| [POSTER HEADER] REAL-TIME GLOBAL SEISMIC BIG DATA PIPELINE & HADOOP ECOSYSTEM ANALYTICS           |
+------------------------------------+----------------------------------+---------------------------+
| PANEL 1: ARCHITECTURE & DATA FLOW  | PANEL 2: LIVE 3D/2D GLOBAL MAP   | PANEL 3: HOURLY ACTIVITY  |
| - USGS GeoJSON Ingestion           | - Plotly WebGL Dark Basemap      | - Rolling 24h Area Chart  |
| - HDFS / YARN / Hive / HBase       | - Magnitude Radius & Depth Color | - Anomaly Spike Highlight |
| - High-res Vector Diagram          | - 24-hour Time Scrubber Slider   | - Evidence of Streaming   |
+------------------------------------+----------------------------------+---------------------------+
| PANEL 4: SEISMIC DRUMBEAT STRIP    | PANEL 5: DEPTH VS MAG SCATTER    | PANEL 6: HBASE VS HIVE    |
| - Magnitude vs Continuous Time     | - Wadati-Benioff Subduction Zone | - Latency Benchmark Chart |
| - Mainshock/Aftershock Clustering  | - Statistical Trend & Marginals  | - <25ms vs 14s (580x)     |
+------------------------------------+----------------------------------+---------------------------+
| [FOOTER] Empirical Conclusions • Team Members • Department of Computer Science • BDA Case Study    |
+---------------------------------------------------------------------------------------------------+
```

### Visual 1: Live Interactive & Animated Global Geospatial Map
- **Engine:** Plotly `scatter_geo` / Mapbox Dark Basemap with WebGL acceleration.
- **Visual Encoding:**
  - **Coordinates:** Longitude & Latitude plotted with sub-millimeter precision.
  - **Bubble Size:** Scaled exponentially to radiated seismic energy: $\text{Size} \propto 1.8^{\text{Magnitude}}$.
  - **Bubble Color:** Gradient mapped to hypocenter depth ($0 \to 700\text{ km}$).
  - **Interactivity:** Hover tooltip displaying Event ID, Place, Richter Magnitude, Depth, Tsunami Alert, and Felt Reports.
  - **Animation Mode:** Continuous playback timeline showing earthquakes emerging chronologically over the past 24–48 hours.

### Visual 2: Seismic Strip / Drumbeat Distribution Chart
- **Engine:** Plotly Scatter Strip with jitter and opacity layering.
- **Axes:**
  - X-Axis: Continuous Timeline (UTC).
  - Y-Axis: Earthquake Magnitude ($M_w$).
- **Value:** Instantly reveals temporal seismic clustering (e.g., aftershock swarms following a major tectonic slip) without requiring tabular parsing.

### Visual 3: Rolling Activity Frequency & Spike Alert Panel
- **Engine:** Gradient Area Chart with dynamic anomaly threshold line.
- **Axes:** Hourly buckets over the rolling 24-48h ingestion window vs. Event Count.
- **Value:** Proves live streaming ingestion in demonstrations. When re-polled, the curve updates dynamically, highlighting statistical spikes (> 2 standard deviations above mean).

### Visual 4: Depth vs. Magnitude Analytical Correlation
- **Engine:** Scientific Scatter Plot with marginal Kernel Density Estimation (KDE) distributions.
- **Axes:** Hypocenter Depth (km) vs. Magnitude ($M_w$), grouped by tectonic boundary zones (Pacific Ring of Fire, Mid-Atlantic Ridge, Alpine-Himalayan belt).
- **Insight:** Demonstrates that the highest magnitude destructive events occur primarily in the shallow lithosphere ($< 35\text{ km}$), whereas deep mantle earthquakes ($> 300\text{ km}$) dissipate significant energy before reaching surface infrastructure.

### Visual 5: Empirical Latency Benchmark (The "Why HBase?" Proof)
- **Engine:** High-contrast Horizontal Bar Comparison with speedup multipliers.
- **Comparison Metric:** Response time for regional top-10 event query:
  - **Apache Hive on HDFS (OLAP Scan):** $\approx 12,400\text{ ms} - 18,200\text{ ms}$ (Query planning, YARN container allocation, map-reduce table scan).
  - **Apache HBase (Indexed Row-Key Seek):** $\approx 18\text{ ms} - 28\text{ ms}$ (Direct seek on `REGION#<reversed_ts>` in MemStore/HFile BlockCache).
- **Speedup Ratio:** $\mathbf{> 500\times\text{ Faster}}$.
- **Significance:** Provides the definitive justification for why HBase is an indispensable component alongside Hive in the Hadoop ecosystem.

---

## 3. Academic BDA Poster Specifications

### 3.1 Poster Dimensions & Target Resolution
- **Standard Dimensions:** 48 inches (width) $\times$ 36 inches (height) [Landscape 4:3] or 36" $\times$ 48" [Portrait].
- **Digital Target:** 3840 $\times$ 2160 (4K UHD) at **300 DPI** export for physical printing.
- **Format Deliverables:**
  1. High-resolution standalone PNG & vector SVG charts in `assets/visuals/`.
  2. Standalone printable HTML/CSS web poster renderer (`poster/poster.html`) with print-to-PDF styles (`@media print { size: 48in 36in; }`).
  3. Interactive Web Dashboard application (`viz/app.py`).

### 3.2 Poster Content Hierarchy
1. **Header Banner:** Project Title, Institutional Affiliation, Course (Big Data Analytics), Author Names, and Date.
2. **Abstract & Problem Statement:** High-velocity streaming ingestion challenges and dual analytical/operational workloads.
3. **Architecture Diagram:** Clean SVG/Vector flow showing USGS Feed $\to$ Ingestion Daemon $\to$ HDFS $\to$ YARN $\to$ Hive/Pig $\to$ HBase $\to$ ZooKeeper $\to$ Dashboard.
4. **Data Engineering Methodology:** Schema on read (JsonSerDe), partition pruning, reverse-timestamp composite row keys.
5. **Seismic Analytics Findings:** Embedded high-DPI charts (Global Map, Drumbeat Strip, Rolling Activity, Depth vs Magnitude).
6. **Benchmark & Ecosystem Comparison:** Hive vs HBase query latency comparison graph and table.
7. **Conclusions & Lessons Learned:** Tradeoffs between batch storage compaction and real-time point-lookup serving.
