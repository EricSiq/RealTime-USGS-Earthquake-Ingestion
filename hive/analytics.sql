-- ============================================================================
-- HIVE ANALYTICAL WORKLOAD QUERIES
-- ============================================================================
USE seismic_dw;

-- ----------------------------------------------------------------------------
-- ETL Pipeline: Project raw GeoJSON into Processed Partitioned ORC
-- ----------------------------------------------------------------------------
INSERT OVERWRITE TABLE earthquakes_processed PARTITION (dt)
SELECT 
  id AS event_id,
  properties.mag AS magnitude,
  properties.place AS place,
  CASE 
    WHEN UPPER(properties.place) LIKE '%CALIFORNIA%' THEN 'CALIFORNIA'
    WHEN UPPER(properties.place) LIKE '%ALASKA%' THEN 'ALASKA'
    WHEN UPPER(properties.place) LIKE '%HAWAII%' THEN 'HAWAII'
    WHEN UPPER(properties.place) LIKE '%JAPAN%' THEN 'JAPAN'
    WHEN UPPER(properties.place) LIKE '%INDONESIA%' THEN 'INDONESIA'
    WHEN UPPER(properties.place) LIKE '%CHILE%' THEN 'CHILE'
    WHEN UPPER(properties.place) LIKE '%PHILIPPINES%' THEN 'PHILIPPINES'
    WHEN UPPER(properties.place) LIKE '%MEXICO%' THEN 'MEXICO'
    WHEN UPPER(properties.place) LIKE '%NEW ZEALAND%' THEN 'NEW_ZEALAND'
    WHEN UPPER(properties.place) LIKE '%PUERTO RICO%' THEN 'PUERTO_RICO'
    WHEN UPPER(properties.place) LIKE '%TURKEY%' OR UPPER(properties.place) LIKE '%TÜRKIYE%' THEN 'TURKEY'
    WHEN UPPER(properties.place) LIKE '%FIJI%' OR UPPER(properties.place) LIKE '%TONGA%' THEN 'SOUTH_PACIFIC'
    ELSE 'GLOBAL_OTHER'
  END AS region,
  from_unixtime(CAST(properties.time / 1000 AS BIGINT)) AS event_time,
  properties.time AS epoch_millis,
  geometry.coordinates[0] AS longitude,
  geometry.coordinates[1] AS latitude,
  geometry.coordinates[2] AS depth_km,
  COALESCE(properties.felt, 0) AS felt_reports,
  COALESCE(properties.alert, 'none') AS alert_level,
  COALESCE(properties.tsunami, 0) AS tsunami_flag,
  COALESCE(properties.sig, 0) AS significance,
  COALESCE(properties.magType, 'unknown') AS magnitude_type,
  POW(10, 4.8 + 1.5 * properties.mag) AS seismic_energy_joules,
  from_unixtime(CAST(properties.time / 1000 AS BIGINT), 'yyyy-MM-dd') AS dt
FROM earthquakes_raw
WHERE properties.mag IS NOT NULL AND id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- Analytical Query 1: Rolling Hourly Seismic Activity & Energy Output
-- ----------------------------------------------------------------------------
SELECT 
  from_unixtime(CAST(epoch_millis / 3600000 AS BIGINT) * 3600, 'yyyy-MM-dd HH:00:00') AS hour_window,
  COUNT(*) AS total_events,
  ROUND(AVG(magnitude), 2) AS avg_magnitude,
  MAX(magnitude) AS max_magnitude,
  SUM(seismic_energy_joules) AS total_energy_joules
FROM earthquakes_processed
GROUP BY CAST(epoch_millis / 3600000 AS BIGINT) * 3600
ORDER BY hour_window DESC;

-- ----------------------------------------------------------------------------
-- Analytical Query 2: Regional Risk Scorecard & Tsunami Potential
-- ----------------------------------------------------------------------------
SELECT 
  region,
  COUNT(*) AS event_count,
  ROUND(AVG(magnitude), 2) AS avg_magnitude,
  MAX(magnitude) AS max_magnitude,
  ROUND(AVG(depth_km), 1) AS avg_depth_km,
  SUM(tsunami_flag) AS tsunami_warning_count,
  SUM(felt_reports) AS total_felt_reports,
  ROUND(SUM(seismic_energy_joules), 2) AS cumulative_energy_joules
FROM earthquakes_processed
GROUP BY region
ORDER BY event_count DESC;

-- ----------------------------------------------------------------------------
-- Analytical Query 3: Hypocenter Depth Layering Classification
-- ----------------------------------------------------------------------------
SELECT 
  CASE 
    WHEN depth_km < 35.0 THEN 'Shallow (0-35 km) - High Crustal Risk'
    WHEN depth_km BETWEEN 35.0 AND 150.0 THEN 'Intermediate (35-150 km) - Subduction Zone'
    ELSE 'Deep (150-700 km) - Mantle Transition'
  END AS depth_classification,
  COUNT(*) AS event_count,
  ROUND(AVG(magnitude), 2) AS avg_magnitude,
  MAX(magnitude) AS max_magnitude
FROM earthquakes_processed
GROUP BY 
  CASE 
    WHEN depth_km < 35.0 THEN 'Shallow (0-35 km) - High Crustal Risk'
    WHEN depth_km BETWEEN 35.0 AND 150.0 THEN 'Intermediate (35-150 km) - Subduction Zone'
    ELSE 'Deep (150-700 km) - Mantle Transition'
  END;

-- ----------------------------------------------------------------------------
-- Operational Point Query (Baseline for Latency Benchmark against HBase)
-- Simulates looking up the latest events in a specific region
-- ----------------------------------------------------------------------------
SELECT event_id, magnitude, place, depth_km, event_time, alert_level
FROM earthquakes_processed
WHERE region = 'CALIFORNIA'
ORDER BY epoch_millis DESC
LIMIT 10;
