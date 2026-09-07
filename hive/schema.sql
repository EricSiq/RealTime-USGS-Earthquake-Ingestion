-- ============================================================================
-- HIVE SCHEMA DEFINITIONS: SEISMIC BIG DATA LAKE
-- Database: seismic_dw
-- ============================================================================

CREATE DATABASE IF NOT EXISTS seismic_dw
COMMENT 'Data warehouse for real-time global seismic telemetry'
LOCATION '/user/hive/warehouse/seismic_dw.db';

USE seismic_dw;

-- ----------------------------------------------------------------------------
-- 1. EXTERNAL RAW TABLE (Schema-on-Read with JsonSerDe)
-- Maps directly over partitioned raw GeoJSON payloads landed in HDFS.
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS earthquakes_raw (
  id STRING COMMENT 'Unique USGS event identifier',
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
  > COMMENT 'Seismic event properties',
  geometry STRUCT<
    type: STRING,
    coordinates: ARRAY<DOUBLE>
  > COMMENT 'Point coordinates: [longitude, latitude, depth_km]'
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/raw/earthquakes/';

-- ----------------------------------------------------------------------------
-- 2. PROCESSED ANALYTICAL TABLE (Columnar ORC with Snappy Compression)
-- Flattened, type-cast, enriched with Gutenberg-Richter energy calculations,
-- and partitioned by date for partition pruning.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS earthquakes_processed (
  event_id STRING COMMENT 'USGS Event ID',
  magnitude DOUBLE COMMENT 'Moment magnitude Mw or equivalent',
  place STRING COMMENT 'Descriptive event location',
  region STRING COMMENT 'Geographic tectonic zone',
  event_time TIMESTAMP COMMENT 'Event timestamp in UTC',
  epoch_millis BIGINT COMMENT 'Raw epoch milliseconds',
  longitude DOUBLE COMMENT 'Longitude in decimal degrees',
  latitude DOUBLE COMMENT 'Latitude in decimal degrees',
  depth_km DOUBLE COMMENT 'Hypocenter focal depth in kilometers',
  felt_reports INT COMMENT 'DYFI citizen felt intensity reports',
  alert_level STRING COMMENT 'PAGER alert color: green, yellow, orange, red',
  tsunami_flag INT COMMENT '1 if oceanic tsunami warning issued, else 0',
  significance INT COMMENT 'USGS composite event significance score (0-1000)',
  magnitude_type STRING COMMENT 'Scale type: mww, ml, md, mb, etc.',
  seismic_energy_joules DOUBLE COMMENT 'Empirical radiated seismic energy (10^(4.8+1.5M))'
)
PARTITIONED BY (dt STRING COMMENT 'Event date in YYYY-MM-DD format')
STORED AS ORC
TBLPROPERTIES (
  "orc.compress"="SNAPPY",
  "orc.create.index"="true",
  "orc.bloom.filter.columns"="region,alert_level,tsunami_flag"
);
