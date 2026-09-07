-- ============================================================================
-- APACHE PIG ETL: Procedural GeoJSON Unnesting & Cleansing
-- ============================================================================

-- Register required PiggyBank JAR for JSON extraction
REGISTER /usr/local/pig/lib/piggybank.jar;

-- Load line-delimited raw JSON files from HDFS
raw_events = LOAD '/raw/earthquakes/*/*/*/*/*.jsonl' 
    USING org.apache.pig.piggybank.storage.JsonLoader(
      'id:chararray, properties:map[], geometry:map[]'
    );

-- Project and unnest nested maps into flat tuple records
projected = FOREACH raw_events GENERATE
    id AS event_id,
    (double)properties#'mag' AS magnitude,
    (chararray)properties#'place' AS place,
    (long)properties#'time' AS epoch_millis,
    (double)geometry#'coordinates'#'0' AS longitude,
    (double)geometry#'coordinates'#'1' AS latitude,
    (double)geometry#'coordinates'#'2' AS depth_km,
    (int)properties#'felt' AS felt_reports,
    (chararray)properties#'alert' AS alert_level,
    (int)properties#'tsunami' AS tsunami_flag;

-- Filter missing or erroneous sensor signals
clean_events = FILTER projected BY 
    event_id IS NOT NULL AND 
    magnitude IS NOT NULL AND 
    magnitude >= 1.0 AND 
    depth_km IS NOT NULL;

-- Store cleaned tabular dataset as tab-separated values ready for downstream bulk loading
STORE clean_events INTO '/processed/pig_flattened_tsv' USING PigStorage('\t');
