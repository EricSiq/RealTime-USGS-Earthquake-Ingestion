# Pipeline & Schema Specifications: Seismic Big Data Ingestion

**Case Study:** USGS Earthquake Ingestion & Multi-Tier Hadoop Analytics  
**Document:** Technical Schemas, DDLs, and Ingestion Contracts  

![Real-Time USGS Earthquake Analytics Using the Hadoop Ecosystem](docs/hadoop_ecosystem_pipeline_workflow.png)

---

## 1. USGS GeoJSON Input Specification

The United States Geological Survey exposes public endpoints providing near-real-time seismic telemetry:
- **Live Rapid Polling Endpoint:** `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson` (updated every 60 seconds)
- **Comprehensive Daily Endpoint:** `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson` (updated every 5 minutes)

### GeoJSON Payload Sample Structure
```json
{
  "type": "FeatureCollection",
  "metadata": {
    "generated": 1725693000000,
    "url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
    "title": "USGS All Earthquakes, Past Hour",
    "status": 200,
    "count": 14
  },
  "features": [
    {
      "type": "Feature",
      "id": "us7000m9xz",
      "properties": {
        "mag": 4.8,
        "place": "12 km SSW of Volcano, Hawaii",
        "time": 1725692485000,
        "updated": 1725692800000,
        "tz": null,
        "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000m9xz",
        "detail": "https://earthquake.usgs.gov/...",
        "felt": 128,
        "cdi": 4.1,
        "mmi": 5.2,
        "alert": "green",
        "status": "reviewed",
        "tsunami": 0,
        "sig": 354,
        "net": "us",
        "code": "7000m9xz",
        "ids": ",us7000m9xz,",
        "sources": ",us,",
        "types": ",origin,phase-data,",
        "nst": 42,
        "dmin": 0.082,
        "rms": 0.21,
        "gap": 48.0,
        "magType": "mww",
        "type": "earthquake",
        "title": "M 4.8 - 12 km SSW of Volcano, Hawaii"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [
          -155.284,  // Longitude (decimal degrees)
          19.342,    // Latitude (decimal degrees)
          8.4        // Depth (kilometers)
        ]
      }
    }
  ]
}
```

---

## 2. HDFS Storage Layout & Partitioning Strategy

### Raw Ingestion Zone (Append-Only Landing)
To avoid directory bloat and enable partition pruning, files land in an hourly partitioned tree:
```text
hdfs://namenode:8020/raw/earthquakes/
└── YYYY=2026/
    └── MM=09/
        └── DD=07/
            └── HH=10/
                ├── usgs_quakes_20260907_100000.json
                ├── usgs_quakes_20260907_100500.json
                └── usgs_quakes_20260907_101000.json
```

### Processed Analytics Zone (ORC / Snappy Compressed)
```text
hdfs://namenode:8020/processed/earthquakes/
└── dt=2026-09-07/
    ├── part-00000.snappy.orc
    └── part-00001.snappy.orc
```

---

## 3. Hive Data Definition Language (DDL)

### 3.1 External Raw Table with JSON SerDe
Enables querying nested GeoJSON files directly in HDFS without pre-parsing:

```sql
CREATE DATABASE IF NOT EXISTS seismic_dw;
USE seismic_dw;

-- External Raw Stage Table mapping the GeoJSON feature structure
CREATE EXTERNAL TABLE IF NOT EXISTS earthquakes_raw (
  id STRING,
  properties STRUCT<
    mag: DOUBLE,
    place: STRING,
    time: BIGINT,
    updated: BIGINT,
    felt: INT,
    cdi: DOUBLE,
    mmi: DOUBLE,
    alert: STRING,
    status: STRING,
    tsunami: INT,
    sig: INT,
    net: STRING,
    nst: INT,
    dmin: DOUBLE,
    rms: DOUBLE,
    gap: DOUBLE,
    magType: STRING,
    type: STRING,
    title: STRING
  >,
  geometry STRUCT<
    type: STRING,
    coordinates: ARRAY<DOUBLE>
  >
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/raw/earthquakes/';
```

### 3.2 Processed Flattened Table (ORC Columnar Format)
Optimized for fast vectorization, compression, and sub-queries:

```sql
CREATE TABLE IF NOT EXISTS earthquakes_processed (
  event_id STRING,
  magnitude DOUBLE,
  place STRING,
  region STRING,
  event_time TIMESTAMP,
  epoch_millis BIGINT,
  longitude DOUBLE,
  latitude DOUBLE,
  depth_km DOUBLE,
  felt_reports INT,
  alert_level STRING,
  tsunami_flag INT,
  significance INT,
  magnitude_type STRING,
  seismic_energy_joules DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
```

### 3.3 ETL Projection Query (Raw to Processed)
```sql
INSERT OVERWRITE TABLE earthquakes_processed PARTITION (dt)
SELECT 
  id AS event_id,
  properties.mag AS magnitude,
  properties.place AS place,
  CASE 
    WHEN properties.place LIKE '%Hawaii%' THEN 'HAWAII'
    WHEN properties.place LIKE '%California%' THEN 'CALIFORNIA'
    WHEN properties.place LIKE '%Alaska%' THEN 'ALASKA'
    WHEN properties.place LIKE '%Japan%' THEN 'JAPAN'
    WHEN properties.place LIKE '%Indonesia%' THEN 'INDONESIA'
    WHEN properties.place LIKE '%Chile%' THEN 'CHILE'
    ELSE 'GLOBAL_OTHER'
  END AS region,
  from_unixtime(CAST(properties.time / 1000 AS BIGINT)) AS event_time,
  properties.time AS epoch_millis,
  geometry.coordinates[0] AS longitude,
  geometry.coordinates[1] AS latitude,
  geometry.coordinates[2] AS depth_km,
  COALESCE(properties.felt, 0) AS felt_reports,
  COALESCE(properties.alert, 'none') AS alert_level,
  properties.tsunami AS tsunami_flag,
  properties.sig AS significance,
  properties.magType AS magnitude_type,
  -- Gutenberg-Richter empirical energy formula: 10^(4.8 + 1.5 * M)
  POW(10, 4.8 + 1.5 * properties.mag) AS seismic_energy_joules,
  from_unixtime(CAST(properties.time / 1000 AS BIGINT), 'yyyy-MM-dd') AS dt
FROM earthquakes_raw
WHERE properties.mag IS NOT NULL AND id IS NOT NULL;
```

---

## 4. Apache Pig Extraction Script (`pig/flatten_quakes.pig`)

Pig demonstrates procedural ETL and explicit nested schema handling:

```pig
-- Register JSON loader
REGISTER /usr/local/pig/lib/piggybank.jar;

raw_feed = LOAD '/raw/earthquakes/' USING org.apache.pig.piggybank.storage.JsonLoader(
  'id:chararray, properties:map[], geometry:map[]'
);

flattened = FOREACH raw_feed GENERATE
  id AS event_id,
  (double)properties#'mag' AS magnitude,
  (chararray)properties#'place' AS place,
  (long)properties#'time' AS epoch_time,
  (int)properties#'tsunami' AS tsunami,
  (chararray)properties#'alert' AS alert_level;

valid_quakes = FILTER flattened BY magnitude IS NOT NULL AND magnitude >= 2.5;

STORE valid_quakes INTO '/processed/pig_output' USING PigStorage(',');
```

---

## 5. Apache HBase Table & Row-Key Architecture

### 5.1 Table Design & Column Families
- **Table Name:** `seismic_events`
- **Column Family 1 (`event`):** Fast analytical & geographic attributes (`mag`, `depth`, `lat`, `lon`, `place`, `time`)
- **Column Family 2 (`meta`):** Operational & alert attributes (`alert`, `tsunami`, `felt`, `sig`)

### 5.2 Composite Reverse-Timestamp Row Key
$$\text{RowKey} = \text{REGION\_PREFIX} \parallel \text{"\#"} \parallel (\text{Long.MAX\_VALUE} - \text{timestamp\_ms})$$

**Example:**
- Region: `CALIFORNIA`
- Timestamp: `1725692485000`
- Reversed Timestamp: `9223372036854775807 - 1725692485000 = 9223370311162290807`
- Row Key: `CALIFORNIA#9223370311162290807`

**Why this matters:**
A query for "Latest 10 events in California" performs a simple prefix scan:
- Start Row: `CALIFORNIA#`
- Stop Row: `CALIFORNIA$`
- Limit: `10`
Because higher timestamps produce smaller numbers when subtracted from `Long.MAX_VALUE`, the latest event appears first lexicographically. No secondary indexing or sorting required!

### 5.3 HBase Shell Commands
```hbase
-- Create Table with tuned bloom filters and Snappy compression
create 'seismic_events', 
  {NAME => 'event', VERSIONS => 1, COMPRESSION => 'SNAPPY', BLOOMFILTER => 'ROW'},
  {NAME => 'meta', VERSIONS => 1, COMPRESSION => 'SNAPPY'}

-- Fast query for latest events
scan 'seismic_events', {STARTROW => 'CALIFORNIA#', LIMIT => 5}
```
