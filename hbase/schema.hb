# ============================================================================
# HBASE DDL & TABLE DEFINITIONS: SEISMIC REAL-TIME SERVING STORE
# Table: seismic_events
# ============================================================================

# Disable and drop if recreation is needed
# disable 'seismic_events'
# drop 'seismic_events'

# Create Table with 2 Column Families:
# - 'event': fast analytical & spatial metrics (mag, depth, lat, lon, place, time)
# - 'meta': operational alerts (alert, tsunami, felt, sig, magType)
# Both tuned with ROW-level Bloom filters and BLOCKCACHE enabled for sub-millisecond lookups.
create 'seismic_events', 
  {
    NAME => 'event', 
    VERSIONS => 1, 
    COMPRESSION => 'SNAPPY', 
    BLOOMFILTER => 'ROW', 
    BLOCKCACHE => true,
    TTL => 2592000 # 30-day retention
  },
  {
    NAME => 'meta', 
    VERSIONS => 1, 
    COMPRESSION => 'SNAPPY', 
    BLOOMFILTER => 'ROW', 
    BLOCKCACHE => true
  }

# Describe table to verify RegionServer configuration
describe 'seismic_events'

# ----------------------------------------------------------------------------
# SAMPLE POINT LOOKUP QUERIES
# Demonstrates reverse-timestamp row-key prefix scanning
# RowKey format: <REGION>#<9223372036854775807 - timestamp_millis>
# ----------------------------------------------------------------------------

# Query: Get the 5 most recent earthquakes in California
scan 'seismic_events', {
  STARTROW => 'CALIFORNIA#',
  STOPROW => 'CALIFORNIA$',
  LIMIT => 5
}

# Query: Get the single most recent major earthquake in Alaska
scan 'seismic_events', {
  STARTROW => 'ALASKA#',
  LIMIT => 1,
  COLUMNS => ['event:mag', 'event:place', 'event:time', 'meta:alert']
}
