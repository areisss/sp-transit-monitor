# Data Dictionary — SP Transit Performance Monitor

## Bronze Layer

### `transit_monitor.bronze.sptrans_gps_raw`
Raw GPS positions ingested from SPTrans OlhoVivo API via Kinesis Firehose → S3 → Auto Loader.

| Column | Type | Description |
|---|---|---|
| `vehicle_id` | STRING | Vehicle prefix identifier from SPTrans |
| `line_code` | INT | Internal line code (used for joins) |
| `line_number` | STRING | Public-facing line number (e.g., "8001-10") |
| `line_direction` | INT | 1 = main→secondary terminal, 2 = reverse |
| `latitude` | DOUBLE | GPS latitude (WGS84) |
| `longitude` | DOUBLE | GPS longitude (WGS84) |
| `is_accessible` | BOOLEAN | Whether the vehicle is wheelchair accessible |
| `event_timestamp` | STRING | Timestamp from GPS device (original string) |
| `ingestion_timestamp` | STRING | Timestamp when Lambda produced the record |
| `_bronze_ingested_at` | TIMESTAMP | When Auto Loader wrote the row to Delta |
| `_event_date` | DATE | Partition column derived from event_timestamp |
| `_source_file` | STRING | S3 path of the source JSON file (lineage) |

**Partition:** `_event_date`
**Approximate volume:** ~900K–1.8M rows/hour

---

## Silver Layer

### `transit_monitor.silver.vehicle_positions_enriched`
Cleaned, deduplicated, and GTFS-enriched vehicle positions.

| Column | Type | Description |
|---|---|---|
| `vehicle_id` | STRING | Vehicle prefix identifier |
| `line_code` | INT | Internal line code |
| `line_number` | STRING | Public line number |
| `line_direction` | INT | Direction of travel |
| `latitude` | DOUBLE | Validated GPS latitude (within SP bounds) |
| `longitude` | DOUBLE | Validated GPS longitude (within SP bounds) |
| `is_accessible` | BOOLEAN | Wheelchair accessible |
| `event_ts` | TIMESTAMP | Parsed event timestamp |
| `ingestion_ts` | TIMESTAMP | Parsed ingestion timestamp |
| `route_short_name` | STRING | GTFS route short name (from join) |
| `route_long_name` | STRING | GTFS route long name (from join) |
| `route_type` | INT | GTFS route type (3 = bus) |
| `h3_index` | BIGINT | H3 hexagon cell index at resolution 9 |
| `hour_of_day` | INT | Hour (0-23) in America/Sao_Paulo timezone |
| `is_weekday` | BOOLEAN | True if Monday-Friday |
| `_event_date` | STRING | Partition column |

**Partition:** `_event_date`
**Dedup key:** `(vehicle_id, event_ts)` within 10-minute watermark

### `transit_monitor.silver.stop_arrivals`
Detected bus arrivals at GTFS stops (within 50m radius).

| Column | Type | Description |
|---|---|---|
| `vehicle_id` | STRING | Vehicle that arrived |
| `stop_id` | STRING | GTFS stop identifier |
| `route_id` | STRING | GTFS route identifier |
| `line_code` | INT | Internal line code |
| `arrival_time` | TIMESTAMP | When the vehicle was detected at the stop |
| `latitude` | DOUBLE | Vehicle latitude at detection |
| `longitude` | DOUBLE | Vehicle longitude at detection |
| `distance_to_stop_m` | DOUBLE | Distance from vehicle to stop center (meters) |
| `_event_date` | STRING | Partition column |

**Partition:** `_event_date`
**Dedup:** One arrival per (vehicle, stop) per 5-minute window

---

## Gold Layer

### `transit_monitor.gold.on_time_performance`
Hourly on-time performance metrics per route.

| Column | Type | Description |
|---|---|---|
| `route_id` | STRING | GTFS route |
| `line_code` | INT | Line code |
| `time_period` | STRING | morning_peak / off_peak / evening_peak / night |
| `is_weekday` | BOOLEAN | Weekday flag |
| `total_arrivals` | LONG | Count of observed arrivals in window |
| `on_time_count` | LONG | Arrivals within [-1min, +5min] of schedule |
| `early_count` | LONG | Arrivals more than 1 min early |
| `late_count` | LONG | Arrivals more than 5 min late |
| `on_time_pct` | DOUBLE | Percentage on-time |
| `avg_delay_seconds` | DOUBLE | Mean delay in seconds |
| `median_delay_seconds` | DOUBLE | Median delay |
| `p95_delay_seconds` | DOUBLE | 95th percentile delay |
| `window_start` | TIMESTAMP | Aggregation window start |
| `window_end` | TIMESTAMP | Aggregation window end |
| `_event_date` | STRING | Partition column |

### `transit_monitor.gold.headway_regularity`
Hourly headway statistics per route and stop.

| Column | Type | Description |
|---|---|---|
| `stop_id` | STRING | GTFS stop |
| `line_code` | INT | Line code |
| `route_id` | STRING | GTFS route |
| `time_period` | STRING | Time period classification |
| `is_weekday` | BOOLEAN | Weekday flag |
| `arrival_count` | LONG | Number of arrivals in window |
| `avg_headway_seconds` | DOUBLE | Mean time between consecutive buses |
| `stddev_headway_seconds` | DOUBLE | Standard deviation of headway |
| `headway_cv` | DOUBLE | Coefficient of variation (stddev/mean) |
| `bunching_severity` | STRING | regular / moderate / severe / critical |
| `window_start` | TIMESTAMP | Aggregation window start |
| `window_end` | TIMESTAMP | Aggregation window end |
| `_event_date` | STRING | Partition column |

**Interpretation:** CV > 0.5 = severe bunching, CV > 1.0 = critical

### `transit_monitor.gold.speed_congestion`
Hourly speed and congestion by H3 hex and route.

| Column | Type | Description |
|---|---|---|
| `h3_index` | BIGINT | H3 cell at resolution 9 |
| `line_code` | INT | Line code |
| `line_number` | STRING | Public line number |
| `time_period` | STRING | Time period classification |
| `is_weekday` | BOOLEAN | Weekday flag |
| `observation_count` | LONG | GPS observations in window |
| `avg_speed_kmh` | DOUBLE | Average speed in km/h |
| `congestion_level` | STRING | free_flow / slow / congested / gridlock |
| `window_start` | TIMESTAMP | Aggregation window start |
| `window_end` | TIMESTAMP | Aggregation window end |
| `_event_date` | STRING | Partition column |

**Congestion levels:** Free (>20 km/h), Slow (10-20), Congested (5-10), Gridlock (<5)

### `transit_monitor.gold.fleet_utilization`
Daily actual vs planned fleet deployment per route.

| Column | Type | Description |
|---|---|---|
| `line_code` | INT | Line code |
| `line_number` | STRING | Public line number |
| `route_long_name` | STRING | Route description |
| `actual_vehicles` | LONG | Distinct vehicles observed |
| `planned_trips` | LONG | GTFS scheduled trips |
| `utilization_ratio` | DOUBLE | actual / planned |
| `status` | STRING | no_service / severely_underserved / underserved / normal / over_capacity |
| `_event_date` | STRING | Partition column |

### `transit_monitor.gold.coverage_equity`
Hourly service frequency by H3 hex joined with census demographics.

| Column | Type | Description |
|---|---|---|
| `h3_index` | BIGINT | H3 cell at resolution 9 |
| `hour_of_day` | INT | Hour of day (SP timezone) |
| `unique_vehicles` | LONG | Distinct vehicles in hex during hour |
| `unique_routes` | LONG | Distinct routes serving the hex |
| `population` | INT | Census population in hex |
| `avg_income_brl` | DOUBLE | Average household income |
| `income_bracket` | STRING | low / medium / high / very_high |
| `vehicles_per_1k_population` | DOUBLE | Service intensity metric |
| `service_equity_score` | STRING | underserved / adequate / moderate / well_served |
| `_event_date` | STRING | Partition column |

---

## Reference Tables

### `transit_monitor.reference.gtfs_routes`
GTFS route definitions. Refreshed daily at 03:00 UTC.

### `transit_monitor.reference.gtfs_stops`
GTFS stop locations. Refreshed daily.

### `transit_monitor.reference.gtfs_stop_times`
GTFS scheduled arrival/departure times per stop. Refreshed daily.

### `transit_monitor.reference.gtfs_shapes`
GTFS route geometry (lat/lng sequences). Refreshed daily.

### `transit_monitor.reference.gtfs_trips`
GTFS trip definitions linking routes to stop sequences. Refreshed daily.

### `transit_monitor.reference.census_demographics`
IBGE census tract demographics for Sao Paulo municipality. Refreshed monthly.
