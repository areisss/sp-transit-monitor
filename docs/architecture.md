# SP Transit Performance Monitor — Architecture & Implementation Guide

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Why This Data Source](#2-why-this-data-source)
3. [Architecture Overview](#3-architecture-overview)
4. [Data Ingestion Layer](#4-data-ingestion-layer)
5. [Streaming Buffer Layer](#5-streaming-buffer-layer)
6. [Raw Storage Layer](#6-raw-storage-layer)
7. [Processing Layer — Medallion Architecture](#7-processing-layer--medallion-architecture)
8. [Analytics Layer — Gold Metrics](#8-analytics-layer--gold-metrics)
9. [Dashboard Layer](#9-dashboard-layer)
10. [Infrastructure as Code](#10-infrastructure-as-code)
11. [Testing Strategy](#11-testing-strategy)
12. [CI/CD Pipeline](#12-cicd-pipeline)
13. [Monitoring & Alerting](#13-monitoring--alerting)
14. [Cost Management](#14-cost-management)
15. [Data Flow Walkthrough](#15-data-flow-walkthrough)

---

## 1. Problem Statement

Sao Paulo operates one of the largest bus systems in the world — over 15,000 vehicles across 2,000+ routes serving 12 million people daily. Despite this scale, there is limited public visibility into how well the system actually performs:

- Are buses arriving on schedule?
- Are they bunching together on certain routes, leaving passengers waiting?
- Which neighborhoods are underserved relative to their population?
- Where are the worst congestion bottlenecks?

The city's transit authority (SPTrans) publishes real-time GPS positions through their OlhoVivo API, but the data is ephemeral — once you fetch it, the positions are overwritten in the next update cycle. Nobody is systematically collecting, storing, and analyzing this data over time.

This project builds the infrastructure to capture that stream, transform it into meaningful metrics, and surface insights through dashboards.

---

## 2. Why This Data Source

Most publicly available government data in Brazil (BCB economic indicators, IBGE census, DATASUS health records) is inherently batch — updated daily, monthly, or yearly. Building a streaming architecture on top of batch data would be an artificial exercise that doesn't demonstrate real streaming engineering skills.

The SPTrans OlhoVivo API is different:

- **Genuine streaming volume:** ~15,000 vehicles reporting GPS positions, producing 900K–1.8M events per hour
- **Real-time cadence:** The API updates every 30–60 seconds, making continuous polling meaningful
- **Free and open:** Requires only a developer registration at [sptrans.com.br/desenvolvedores](https://www.sptrans.com.br/desenvolvedores/)
- **Rich enough for real analysis:** GPS coordinates, line/route information, accessibility flags, timestamps — enough to derive speed, headway, on-time performance, and equity metrics
- **Government accountability angle:** The analysis produces metrics that matter for public policy, not just technical demonstrations

### API Structure

The SPTrans OlhoVivo API (v2.1) has two relevant endpoints:

**Authentication:**
```
POST /Login/Autenticar?token={api_token}
```
Returns `true`/`false`. Sets a session cookie that must be passed in subsequent requests.

**Vehicle Positions:**
```
GET /Posicao
```
Returns all active vehicles grouped by line:
```json
{
  "hr": "15:30",
  "l": [
    {
      "c": "8001-10",       // public line number
      "cl": 33000,          // internal line code
      "sl": 1,              // direction (1=main→secondary, 2=reverse)
      "lt0": "Terminal A",  // origin terminal
      "lt1": "Terminal B",  // destination terminal
      "vs": [
        {
          "p": 52011,         // vehicle ID (integer)
          "a": true,          // wheelchair accessible
          "ta": "2026-03-20T15:00:00Z",  // GPS timestamp
          "py": -23.5505,     // latitude
          "px": -46.6340      // longitude
        }
      ]
    }
  ]
}
```

Key quirk discovered during implementation: the `p` (vehicle ID) field is returned as an **integer**, not a string, despite the documentation suggesting otherwise. This required adjusting our Pydantic model to accept `int | str` and coercing to string during normalization.

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                       DATA INGESTION                             │
│                                                                  │
│  EventBridge ──(1 min)──▶ Lambda ──(poll)──▶ SPTrans API        │
│                              │                                   │
│                              ▼                                   │
│                     Kinesis Data Streams                          │
│                     (4 shards, on-demand)                        │
│                              │                                   │
│                              ▼                                   │
│                     Kinesis Firehose                              │
│                     (60s buffer, gzip)                           │
│                              │                                   │
│                              ▼                                   │
│                      S3 Raw Landing                              │
│           (date-partitioned JSONL, gzip)                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                    PROCESSING (Databricks)                       │
│                                                                  │
│  Auto Loader ──▶ Bronze (raw Delta)                             │
│                      │                                           │
│                      ▼                                           │
│               Silver (cleaned, enriched)                         │
│               ├── vehicle_positions_enriched                     │
│               └── stop_arrivals                                  │
│                      │                                           │
│                      ▼                                           │
│               Gold (aggregated metrics)                          │
│               ├── on_time_performance                            │
│               ├── headway_regularity                             │
│               ├── speed_congestion                               │
│               ├── fleet_utilization                              │
│               └── coverage_equity                                │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                    ANALYTICS (Dashboards)                        │
│                                                                  │
│  Databricks SQL ──▶ 5 dashboards with live metrics              │
└─────────────────────────────────────────────────────────────────┘
```

### Why this specific architecture?

Every component was chosen to minimize operational overhead and cost while maintaining a production-grade design:

| Decision | Alternative considered | Why we chose this |
|---|---|---|
| **Lambda** for polling | ECS Fargate, EC2 | No Docker, no cluster, no task definitions. EventBridge triggers it. ~$0.50/month. |
| **EventBridge** for scheduling | CloudWatch Events, Step Functions | Simplest managed scheduler. 1-minute minimum is acceptable since the API updates at ~30-60s intervals. |
| **Kinesis Data Streams** in the middle | Direct Lambda→S3, SQS | Decouples producer from consumer. Enables future real-time consumers (alerts, dashboards) without touching the S3 delivery path. Standard streaming primitive. |
| **Kinesis Firehose** to S3 | Custom Lambda consumer, Spark direct read from Kinesis | Handles buffering, compression, date partitioning, and delivery guarantees automatically. Zero code. |
| **S3** as raw archive | Keep data only in Kinesis/Delta | S3 is the durable foundation. If Kinesis retention (24h) expires or Databricks goes down, no data is lost. Cost is negligible (~$0.023/GB/month). |
| **Auto Loader** for Bronze | Direct Kinesis connector, manual S3 listing | `cloudFiles` is the Databricks-recommended pattern. Uses S3 event notifications for efficiency. Handles schema evolution. |
| **No VPC for Lambda** | Lambda in VPC with NAT Gateway | Lambda only needs internet access (SPTrans API + Kinesis endpoint). VPC would add a NAT Gateway at ~$32/month with no security benefit for this use case. |
| **On-demand Kinesis** | Provisioned shards | Auto-scales. No shard management for a project with predictable but variable load. |

---

## 4. Data Ingestion Layer

### Lambda Producer (`lambda/sptrans_producer/`)

The Lambda function is the entry point of the entire pipeline. It executes every minute and performs three operations:

#### Step 1: Authenticate with SPTrans

```python
# sptrans_client.py
def _authenticate(client, config):
    resp = client.post(url, params={"token": config.sptrans_api_token})
    if not resp.json():
        raise RuntimeError("SPTrans authentication failed")
    return resp.cookies
```

The session cookie is cached at module scope (`_cached_session_cookies`). Since Lambda reuses execution environments across invocations ("warm starts"), this means we authenticate once and reuse the session for subsequent invocations — typically tens of minutes before Lambda recycles the container.

If a request returns 401 (session expired), the client automatically re-authenticates once before raising an error:

```python
def fetch_vehicle_positions(client, config):
    _ensure_authenticated(client, config)
    resp = client.get(url)
    if resp.status_code == 401:
        _cached_session_cookies = None
        _ensure_authenticated(client, config)
        resp = client.get(url)
    resp.raise_for_status()
    return ApiPositionResponse.model_validate(resp.json())
```

**Rationale for session caching:** The SPTrans API has no documented rate limit on authentication, but hitting it 1,440 times/day (once per minute) is unnecessary when sessions last much longer. Caching reduces API calls by ~95%.

#### Step 2: Normalize the response

The API returns a nested structure (lines → vehicles). We flatten it into individual vehicle records:

```python
# models.py
def normalize_positions(response, ingestion_time):
    for line in response.l:
        for vehicle in line.vs:
            positions.append(VehiclePosition(
                vehicle_id=str(vehicle.p),  # coerce int → str
                line_code=line.cl,
                line_number=line.c,
                ...
            ))
```

Each record becomes a flat JSON object with 9 fields. This denormalization happens at ingestion time so that downstream consumers (Kinesis, Firehose, Spark) don't need to understand the nested API structure.

**Why Pydantic models?** Validation at the boundary. If SPTrans changes their API response format, the Pydantic model will raise a clear validation error rather than silently producing corrupt data downstream.

#### Step 3: Write to Kinesis

Records are batched into groups of 500 (Kinesis `put_records` maximum) and written with retry logic for partial failures:

```python
def put_records_batch(kinesis_client, stream_name, records, batch_size=500, max_retries=3):
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        for attempt in range(max_retries):
            response = kinesis_client.put_records(...)
            if response["FailedRecordCount"] == 0:
                break
            # Retry only the failed records with exponential backoff
            retry_records = [r for r, result in zip(records, response["Records"]) if "ErrorCode" in result]
```

**Partition key is `vehicle_id`:** This ensures all records for the same vehicle land on the same shard, which is important for downstream stateful processing (speed calculation, stop arrival detection) that needs to see consecutive positions for the same vehicle in order.

**Why not just put all 11K records in one call?** Kinesis limits `put_records` to 500 records or 5MB per call. With ~300 bytes per record, we hit the 500-record limit first. So a typical invocation makes ~23 `put_records` calls.

### Configuration

All configuration is via environment variables, set by Terraform:

```python
@dataclass(frozen=True)
class Config:
    sptrans_api_base_url: str   # https://api.olhovivo.sptrans.com.br/v2.1
    sptrans_api_token: str      # from SSM Parameter Store
    kinesis_stream_name: str    # transit-monitor-sptrans-gps-raw
    kinesis_batch_size: int     # 500
    aws_region: str             # us-east-1
```

**Why environment variables instead of SSM at runtime?** Lambda cold start latency. Reading from SSM adds ~100ms per cold start. Since the token doesn't change at runtime, injecting it as an environment variable via Terraform is simpler and faster.

---

## 5. Streaming Buffer Layer

### Kinesis Data Streams

```
Stream: transit-monitor-sptrans-gps-raw
Mode:   ON_DEMAND (auto-scaling)
Shards: 4 (auto-managed)
Retention: 24 hours
```

Kinesis sits between the Lambda producer and the Firehose consumer for three reasons:

1. **Decoupling:** If Firehose or S3 has issues, the producer doesn't fail. Records buffer in Kinesis for up to 24 hours.
2. **Fan-out capability:** Future real-time consumers (alerting Lambda, live dashboard via Kinesis Analytics) can read from the same stream independently without affecting the S3 delivery path.
3. **Ordering guarantees:** Records for the same `vehicle_id` (partition key) arrive in order on the same shard, enabling stateful downstream processing.

**Why on-demand mode?** With provisioned shards, you pay for idle capacity and must manually manage scaling. On-demand mode costs slightly more per GB but auto-scales and requires zero management. For a project with predictable load (~12 GB/day), the cost difference is negligible (~$2/month).

### Kinesis Firehose

```
Source:      Kinesis Data Stream (transit-monitor-sptrans-gps-raw)
Destination: S3 (transit-monitor-raw)
Buffer:      60 seconds OR 5 MB (whichever comes first)
Compression: GZIP
Prefix:      sptrans-gps/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/
```

Firehose is a fully managed delivery stream that handles three critical concerns:

1. **Buffering:** Accumulates records for 60 seconds before writing to S3, producing reasonably sized files (~60-180 KB compressed) instead of thousands of tiny files.
2. **Compression:** GZIP reduces storage costs by ~80% and speeds up downstream reads.
3. **Date partitioning:** The S3 prefix uses Firehose's timestamp interpolation syntax to create Hive-style partitions (`year=2026/month=03/day=20/hour=21/`), enabling efficient partition pruning in Spark.

**Why Firehose instead of a custom Lambda consumer?** Firehose handles exactly-once delivery, retry logic, dead-letter routing, and buffering — all of which would require hundreds of lines of custom code. The error prefix (`sptrans-gps-errors/`) catches any records that fail to deliver.

**Output format:** Firehose writes records as concatenated JSON (no newline separator between records). This is its default behavior and is handled by Spark's JSON reader, but it's worth noting because standard line-by-line JSON parsing (like `jq`) won't work directly on the raw files.

---

## 6. Raw Storage Layer

### S3 Bucket Structure

```
s3://transit-monitor-raw/
└── sptrans-gps/
    └── year=2026/
        └── month=03/
            └── day=20/
                └── hour=21/
                    ├── transit-monitor-sptrans-to-s3-1-2026-03-20-21-03-19-*.gz
                    ├── transit-monitor-sptrans-to-s3-1-2026-03-20-21-03-19-*.gz
                    ├── transit-monitor-sptrans-to-s3-1-2026-03-20-21-03-19-*.gz
                    └── transit-monitor-sptrans-to-s3-1-2026-03-20-21-03-19-*.gz
```

Each hour produces ~60 files (one per minute × one per shard). Each file contains ~2,800 records (~60KB compressed).

**Lifecycle policy:**
- 0–30 days: S3 Standard ($0.023/GB)
- 30–90 days: S3 Standard-IA ($0.0125/GB) — 46% cheaper for infrequent access
- 90–365 days: S3 Glacier ($0.004/GB) — 83% cheaper for archival
- After 365 days: Deleted

This is the **immutable raw archive**. No processing happens in this bucket — it's purely a safety net. If the Databricks pipeline has bugs, we can reprocess from raw S3 at any time.

### Other S3 Buckets

- **`transit-monitor-checkpoints`**: Stores Spark Structured Streaming checkpoint data (offsets, state). Never manually modified.
- **`transit-monitor-lambda-code`**: Stores the Lambda deployment zip. Updated by CI/CD.

---

## 7. Processing Layer — Medallion Architecture

The medallion architecture (Bronze → Silver → Gold) is Databricks' recommended pattern for organizing data lakes. Each layer has a clear purpose:

| Layer | Purpose | Data quality | Schema |
|---|---|---|---|
| **Bronze** | Raw ingestion, append-only | As-is from source | Matches API output |
| **Silver** | Cleaned, validated, enriched | Deduplicated, bounds-checked | Business-oriented |
| **Gold** | Aggregated metrics | Analytics-ready | Pre-computed for dashboards |

### Bronze Layer

**Table:** `transit_monitor.bronze.sptrans_gps_raw`

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .schema(SPTRANS_GPS_RAW_SCHEMA)
    .load("s3://transit-monitor-raw/sptrans-gps/")
    .withColumn("_bronze_ingested_at", current_timestamp())
    .withColumn("_event_date", to_date(col("event_timestamp")))
    .withColumn("_source_file", input_file_name())
    .writeStream
    .format("delta")
    .partitionBy("_event_date")
    .trigger(processingTime="30 seconds")
    .toTable("transit_monitor.bronze.sptrans_gps_raw")
```

**Auto Loader with S3 notifications (`useNotifications=true`):** Instead of periodically listing the S3 directory (which becomes slow with thousands of files), Auto Loader creates an SQS queue and S3 event notification automatically. When Firehose writes a new file, S3 sends an event to SQS, and Auto Loader picks it up within seconds. This is both faster and cheaper than directory listing.

**Three metadata columns added:**
- `_bronze_ingested_at`: When Auto Loader processed the record (debugging, latency monitoring)
- `_event_date`: Partition column derived from the GPS timestamp (enables efficient date-range queries)
- `_source_file`: The S3 path of the source file (full lineage — trace any record back to its raw file)

**No transformation at Bronze.** The data enters Delta exactly as it arrived from the API. This preserves the raw data for reprocessing if Silver logic changes.

### Silver Layer — Vehicle Positions

**Table:** `transit_monitor.silver.vehicle_positions_enriched`

The Silver layer applies five transformations in sequence:

#### Transform 1: Parse timestamps
```python
def parse_timestamps(df):
    return df
        .withColumn("event_ts", to_timestamp(col("event_timestamp")))
        .withColumn("ingestion_ts", to_timestamp(col("ingestion_timestamp")))
        .drop("event_timestamp", "ingestion_timestamp")
```
Converts ISO 8601 strings to proper Spark `TimestampType` for windowing and watermarking.

#### Transform 2: Coordinate bounds filter
```python
def filter_sp_bounds(df):
    return df.filter(
        (col("latitude") >= -24.1) & (col("latitude") <= -23.3) &
        (col("longitude") >= -47.0) & (col("longitude") <= -46.2)
    )
```
Removes GPS positions outside the Sao Paulo metropolitan area. This catches GPS initialization errors (0,0 coordinates), satellite drift, and vehicles that might be in a depot outside the metro area. The bounding box is generous enough to cover the entire metro region including suburban areas.

#### Transform 3: Watermark + deduplication
```python
.withWatermark("event_ts", "10 minutes")
.dropDuplicatesWithinWatermark(["vehicle_id", "event_ts"])
```
The watermark tells Spark: "I expect data to arrive at most 10 minutes late. After that, late records can be dropped." This is generous — typical latency from GPS device to our S3 is under 2 minutes. The watermark enables `dropDuplicatesWithinWatermark`, which removes duplicate records (same vehicle, same timestamp) that can occur if:
- Lambda retries after a timeout
- Kinesis delivers a record twice (at-least-once guarantee)
- The SPTrans API returns the same position in consecutive calls

#### Transform 4: GTFS route enrichment
```python
def enrich_with_gtfs(df, gtfs_routes):
    return df.join(broadcast(gtfs_routes), df["line_code"] == gtfs_routes["line_code"], "left")
```
Joins each position with the GTFS routes reference table to add `route_short_name`, `route_long_name`, and `route_type`. This is a **broadcast join** because the routes table is small (~2,000 rows) — broadcasting it to all executors avoids a shuffle.

The join is `left` (not inner) because some vehicles might have line codes not in the current GTFS feed (express services, temporary routes).

#### Transform 5: H3 hexagon index
```python
def add_h3_index(df):
    return df.withColumn("h3_index", h3_index_udf(col("latitude"), col("longitude")))
```
Assigns each position to an H3 hexagonal cell at resolution 9 (~174m edge length). H3 is Uber's hierarchical spatial indexing system. We use it instead of lat/lng for two reasons:

1. **Efficient spatial aggregation:** Grouping by `h3_index` instead of rounding lat/lng gives uniform hexagonal cells with consistent areas, avoiding distortion near the poles (not an issue in SP, but good practice).
2. **Neighbor lookups:** H3 provides `k_ring` functions to find adjacent cells, useful for stop arrival detection and congestion corridor analysis.

Resolution 9 (~174m) was chosen because it's small enough to distinguish individual bus stops (typically 200-500m apart) but large enough to aggregate multiple GPS pings from a vehicle passing through.

### Silver Layer — Stop Arrivals

**Table:** `transit_monitor.silver.stop_arrivals`

This is the most computationally interesting part of the pipeline. It detects when a bus arrives at a GTFS stop by checking if the vehicle position falls within a 50-meter radius:

```python
def detect_stop_arrivals(positions_batch, gtfs_stops):
    return (
        positions_batch
        .crossJoin(broadcast(gtfs_stops))
        .withColumn("distance_to_stop_m",
            haversine_udf(col("latitude"), col("longitude"),
                         col("stop_latitude"), col("stop_longitude")))
        .filter(col("distance_to_stop_m") <= 50.0)
        .dropDuplicates(["vehicle_id", "stop_id", "arrival_window"])
    )
```

**Why 50 meters?** GPS accuracy for city buses is typically 5-15 meters. Bus stops are physical locations where buses pull over. A 50m radius accounts for GPS drift, the length of the bus itself (~12m), and the fact that the bus might stop slightly before or after the stop sign.

**Cross join concern:** Cross-joining every position with every stop would be O(positions × stops) — potentially billions of comparisons. We mitigate this with:
1. **Broadcast join:** The stops table (~15,000 rows) is small enough to broadcast.
2. **`foreachBatch` with 5-minute trigger:** Processing in micro-batches limits the cross-join to ~55K positions × 15K stops = ~825M comparisons per batch, which is manageable on a small cluster.
3. **Deduplication window:** One arrival per (vehicle, stop) per 5-minute window prevents counting the same bus sitting at a stop as multiple arrivals.

---

## 8. Analytics Layer — Gold Metrics

### On-Time Performance

**Question answered:** What percentage of buses arrive on schedule?

**Method:**
1. Join `stop_arrivals` with `gtfs_stop_times` on (stop_id, route_id)
2. For each arrival, find the closest scheduled trip (minimum absolute delay)
3. Classify: early (> 1 min early), on-time (-1 min to +5 min), late (> 5 min late)
4. Aggregate by route, hour, time period, and day type

**Why [-1 min, +5 min]?** This is the standard definition used by transit agencies worldwide (FTA, TfL). Arriving slightly early is penalized because passengers relying on the schedule might miss the bus. The 5-minute late threshold accounts for normal traffic variation.

### Headway Regularity

**Question answered:** Are buses evenly spaced, or are they bunching?

**Method:**
1. For each (stop, route), sort arrivals chronologically
2. Compute headway = time between consecutive arrivals
3. Calculate coefficient of variation (CV) = stddev / mean
4. Classify: regular (CV < 0.3), moderate (0.3-0.5), severe (0.5-1.0), critical (> 1.0)

**Why CV instead of just stddev?** A route with 5-minute headways and 2-minute stddev (CV=0.4) is much worse than a route with 30-minute headways and 5-minute stddev (CV=0.17). CV normalizes for the planned frequency.

**What is bus bunching?** When buses travel the same route, small delays compound. A delayed bus picks up more passengers (longer dwell times), making it more delayed. The bus behind it finds fewer passengers, speeds up, and catches the first bus. Eventually, 2-3 buses arrive together followed by a long gap. CV > 0.5 is a reliable indicator of this phenomenon.

### Speed & Congestion

**Question answered:** Where are the worst congestion points, and when?

**Method:**
1. For each vehicle, compute instantaneous speed between consecutive GPS positions using the Haversine formula
2. Filter out implausible speeds > 150 km/h (GPS jumps)
3. Aggregate by H3 hexagon, route, and hour
4. Classify: free flow (> 20 km/h), slow (10-20), congested (5-10), gridlock (< 5)

**Why Haversine instead of straight-line distance?** At the scale of individual bus segments (100-500m), the difference between Haversine (great-circle) and Euclidean distance is negligible. But Haversine is the correct formula for geographic coordinates and costs almost nothing extra to compute.

**Speed thresholds:** Based on typical urban transit speeds. An average speed of 20 km/h is normal for a city bus (frequent stops, traffic lights). Below 5 km/h means the bus is barely moving — walking speed.

### Fleet Utilization

**Question answered:** How many buses actually run vs what's planned?

**Method:**
1. Count distinct `vehicle_id` per route from Silver positions for the target date
2. Count planned trips per route from GTFS stop_times
3. Compute utilization ratio = actual vehicles / planned trips
4. Classify: no service, severely underserved (< 0.5), underserved (0.5-0.8), normal (0.8-1.2), over capacity (> 1.2)

**Daily batch, not streaming:** Fleet utilization is a daily summary metric. Running it as a streaming job would waste resources for data that's only meaningful at day-end.

### Coverage Equity

**Question answered:** Do low-income neighborhoods get fewer buses?

**Method:**
1. Count unique vehicles per H3 hexagon per hour from Silver positions
2. Join with IBGE census demographics (population, income) aggregated to H3 cells
3. Compute vehicles per 1,000 population
4. Flag "underserved" where income is low AND service frequency is below threshold

**Why this matters:** Transit equity is a major policy concern. If wealthy neighborhoods have 10 buses/hour and low-income neighborhoods have 2 buses/hour despite similar population density, that's a systemic issue worth quantifying.

---

## 9. Dashboard Layer

Five SQL-based dashboards in Databricks SQL, each targeting a specific audience:

| Dashboard | Primary visualization | Audience |
|---|---|---|
| On-Time Performance | Time series of on-time %, worst routes table | Transit planners |
| Headway Regularity | Bunching severity distribution, route-level trends | Operations managers |
| Speed & Congestion | H3 hex map colored by congestion level, speed trends | Traffic engineers |
| Fleet Utilization | Actual vs planned bar chart, underserved routes table | Fleet managers |
| Coverage Equity | Income vs service scatter plot, underserved areas map | Policy makers |

The SQL queries are parameterized (`:line_code_param`, date filters) so dashboards can be interactive.

---

## 10. Infrastructure as Code

### Terraform Module Structure

```
terraform/
├── bootstrap/          # One-time: S3 state bucket + DynamoDB lock
├── environments/dev/   # Environment-specific config
└── modules/
    ├── storage/        # S3 buckets
    ├── kinesis/        # Data Stream + Firehose
    ├── lambda/         # Function + EventBridge + IAM
    ├── monitoring/     # CloudWatch alarms + SNS
    └── databricks/     # Unity Catalog, clusters, jobs (deferred)
```

**Why modules?** Each module encapsulates a logical component with its own IAM roles, policies, and resources. This makes it easy to:
- Understand what each component needs (read the module's IAM policy)
- Destroy a single component without affecting others
- Reuse modules for different environments (dev, staging, prod)

### Key Design Decisions

**Bootstrap is separate from main infrastructure.** The S3 bucket that stores Terraform state cannot itself be managed by the same Terraform state — it's a chicken-and-egg problem. The bootstrap directory uses local state and is applied once manually.

**Sensitive variables are marked `sensitive = true`:** The SPTrans API token is passed as a Terraform variable and never appears in plan output or state file in plaintext.

**Lambda code comes from S3, not inline:** The Lambda function references an S3 object (`s3_bucket` + `s3_key`) rather than a local zip file. This decouples code deployment from infrastructure deployment — CI/CD can update the Lambda code without running Terraform.

**Databricks module is commented out:** To allow deploying AWS infrastructure immediately while deferring Databricks costs until the free trial period.

---

## 11. Testing Strategy

### Lambda Unit Tests (15 tests)

```
lambda/tests/
├── test_handler.py          # 6 tests: batch writing, partial failures, empty records, full handler flow
├── test_sptrans_client.py   # 4 tests: auth, re-auth on 401, API errors, auth failures
└── test_models.py           # 5 tests: normalization, field mapping, empty responses, Kinesis serialization
```

**Mock strategy:** httpx's `MockTransport` for API calls, `unittest.mock.MagicMock` for Kinesis client. No network calls in unit tests.

### Spark Unit Tests (16 tests)

```
tests/unit/databricks/
├── test_geo_utils.py         # 7 tests: Haversine, H3 indexing, SP bounds
├── test_time_utils.py        # 5 tests: time period classification, weekday detection
├── test_silver_transforms.py # 3 tests: timestamp parsing, coordinate filtering
└── test_gold_transforms.py   # 6 tests: on-time classification, congestion levels, headway computation
```

**PySpark local mode:** Tests run with `SparkSession.builder.master("local[1]")` — no cluster needed. This allows running Spark tests in CI on standard GitHub Actions runners.

### Integration Tests (3 tests)

```
tests/integration/
└── test_lambda_kinesis.py    # End-to-end: put_records → read back from stream
```

Uses `moto` to mock AWS services locally. Verifies that records written to Kinesis can be read back with correct content and ordering.

---

## 12. CI/CD Pipeline

Four GitHub Actions workflows:

### `ci.yml` — On every PR
1. **Lint:** `ruff check` + `ruff format --check` (catches style issues, unused imports)
2. **Lambda tests:** Install deps, run `pytest lambda/tests/`
3. **Spark tests:** Install PySpark + Java 17, run `pytest tests/unit/databricks/`

### `terraform-plan.yml` — On PRs touching `terraform/`
1. Configure AWS credentials from GitHub Secrets
2. `terraform init` + `terraform validate` + `terraform plan`
3. Post the plan as a PR comment (reviewers see exactly what will change)

### `terraform-apply.yml` — On merge to main touching `terraform/`
1. Requires `production` environment approval
2. `terraform apply -auto-approve`

### `lambda-deploy.yml` — On merge to main touching `lambda/`
1. Install dependencies with `--platform manylinux2014_x86_64 --python-version 3.12` (cross-compile for Lambda's Linux runtime)
2. Create zip, upload to S3
3. `aws lambda update-function-code`
4. `aws lambda wait function-updated` (verify deployment)

---

## 13. Monitoring & Alerting

Four CloudWatch alarms, all routing to an SNS topic:

| Alarm | Condition | What it means |
|---|---|---|
| `lambda-errors` | > 5 errors in 15 min | SPTrans API is down or code bug |
| `lambda-duration` | Avg > 45s (timeout is 60s) | API response is slow, risk of timeout |
| `lambda-no-invocations` | < 1 invocation in 5 min | EventBridge schedule is disabled or broken |
| `kinesis-iterator-age` | > 1 hour | Firehose consumer is falling behind |

The "no invocations" alarm uses `treat_missing_data = "breaching"` — if CloudWatch has no data points (which happens when the schedule is disabled), the alarm fires. This catches accidental schedule deletion.

---

## 14. Cost Management

### Current monthly estimate

| Service | Cost |
|---|---|
| Lambda + EventBridge | ~$0.50 |
| Kinesis Data Streams (on-demand) | ~$20 |
| Kinesis Firehose | ~$11 |
| S3 | ~$3 |
| CloudWatch + SNS | ~$5 |
| **AWS Total** | **~$40** |
| Databricks (when activated) | ~$100–160 |
| **Full Total** | **~$140–200** |

### Cost controls implemented

1. **S3 lifecycle rules:** Automatic transition to cheaper storage tiers
2. **Kinesis on-demand:** No idle shard costs
3. **Lambda outside VPC:** Saves ~$32/month NAT Gateway
4. **EventBridge pause capability:** One command to stop all ingestion costs
5. **Databricks deferred:** Not activated until free trial

### How to pause/resume

**Pause** (stops Lambda invocations, Kinesis and Firehose idle at minimal cost):
```bash
aws scheduler update-schedule --name transit-monitor-sptrans-poll-30s --state DISABLED ...
```

**Resume:**
```bash
aws scheduler update-schedule --name transit-monitor-sptrans-poll-30s --state ENABLED ...
```

---

## 15. Data Flow Walkthrough

Here's what happens to a single bus GPS position, from physical bus to dashboard:

### Second 0: Bus reports position
A bus (vehicle 52011) on line 8001-10 reports its GPS coordinates (-23.5505, -46.6340) to SPTrans' internal system.

### Second ~30: SPTrans API updates
The `/Posicao` endpoint now includes this position in its response.

### Second ~60: Lambda polls
EventBridge triggers our Lambda function. It authenticates (or reuses cached session), calls `GET /Posicao`, receives ~11,000 vehicle positions including vehicle 52011.

### Second ~61: Normalization
The nested response is flattened. Vehicle 52011 becomes:
```json
{
  "vehicle_id": "52011",
  "line_code": 33000,
  "line_number": "8001-10",
  "line_direction": 1,
  "latitude": -23.5505,
  "longitude": -46.634,
  "is_accessible": true,
  "event_timestamp": "2026-03-20T15:00:00Z",
  "ingestion_timestamp": "2026-03-20T15:00:32+00:00"
}
```

### Second ~62: Kinesis ingestion
The record is written to Kinesis Data Streams with partition key `"52011"`. Kinesis routes it to shard 2 (determined by hash of partition key).

### Second ~120: Firehose delivery
After 60 seconds of buffering, Firehose writes a batch of ~2,800 records (including ours) to:
```
s3://transit-monitor-raw/sptrans-gps/year=2026/month=03/day=20/hour=15/firehose-...-shard2.gz
```

### Second ~150: Bronze ingestion
Databricks Auto Loader receives an S3 notification via SQS, reads the new file, and appends the records to the Bronze Delta table with metadata columns (`_bronze_ingested_at`, `_source_file`).

### Second ~180: Silver processing
The Silver streaming job:
1. Parses the timestamp string to a Spark Timestamp
2. Confirms the coordinates are within SP bounds (they are)
3. Deduplicates against other records within the 10-minute watermark
4. Joins with GTFS routes to add `route_short_name: "8001-10"`, `route_long_name: "Terminal Campo Limpo - Terminal Vila Mariana"`
5. Computes H3 index at resolution 9
6. Writes to `transit_monitor.silver.vehicle_positions_enriched`

### Second ~180 (parallel): Stop arrival check
The stop arrival job checks if vehicle 52011's position (-23.5505, -46.6340) is within 50m of any GTFS stop. If the Se Cathedral stop (S002: -23.5505, -46.6340) is 12 meters away, an arrival event is recorded in `transit_monitor.silver.stop_arrivals`.

### Second ~480: Gold aggregation
Every 5 minutes, the Gold streaming jobs process new arrivals:
- **On-time:** The arrival at Se Cathedral is compared to the scheduled time. If the bus was supposed to arrive at 15:00 and arrived at 15:02, it's classified as "on_time" (+120 seconds delay).
- **Headway:** The time since the previous bus arrived at Se Cathedral on line 8001-10 is computed. If the last bus was 8 minutes ago (planned: 10 minutes), the headway is 480 seconds.
- **Speed:** The distance from vehicle 52011's previous position (30 seconds ago) divided by time gives instantaneous speed. If it moved 200m in 30s, that's 24 km/h — "free flow."

### Dashboard query
A transit planner opens the On-Time Performance dashboard and sees that line 8001-10 has 72% on-time performance during the morning peak, with a median delay of +3 minutes. The worst stop is Se Cathedral with only 58% on-time, suggesting congestion in the city center.

---

## Appendix: Files Reference

| Path | Purpose |
|---|---|
| `lambda/sptrans_producer/handler.py` | Lambda entry point |
| `lambda/sptrans_producer/sptrans_client.py` | SPTrans API client with session caching |
| `lambda/sptrans_producer/models.py` | Pydantic models and normalization |
| `lambda/sptrans_producer/config.py` | Environment-based configuration |
| `databricks/streaming/bronze/ingest_from_s3.py` | Auto Loader → Bronze Delta |
| `databricks/streaming/silver/clean_enrich_gps.py` | Cleaning, dedup, GTFS join, H3 |
| `databricks/streaming/silver/stop_arrival_detection.py` | Stateful stop arrival detection |
| `databricks/streaming/gold/on_time_performance.py` | Schedule adherence metrics |
| `databricks/streaming/gold/headway_regularity.py` | Bus bunching detection |
| `databricks/streaming/gold/speed_congestion.py` | Speed and congestion classification |
| `databricks/streaming/gold/fleet_utilization.py` | Actual vs planned fleet (batch) |
| `databricks/batch/coverage_equity.py` | Transit equity analysis (batch) |
| `databricks/batch/load_gtfs_schedules.py` | GTFS reference data loader |
| `databricks/batch/load_census_demographics.py` | IBGE census data loader |
| `databricks/utils/geo_utils.py` | H3, Haversine, SP bounds |
| `databricks/utils/time_utils.py` | Timezone, time period classification |
| `databricks/schemas/bronze_schemas.py` | Bronze StructType definitions |
| `databricks/schemas/silver_schemas.py` | Silver StructType definitions |
| `terraform/modules/kinesis/main.tf` | Kinesis Data Stream + Firehose |
| `terraform/modules/lambda/main.tf` | Lambda + EventBridge + IAM |
| `terraform/modules/storage/main.tf` | S3 buckets with lifecycle |
| `terraform/modules/monitoring/main.tf` | CloudWatch alarms + SNS |
| `terraform/modules/databricks/main.tf` | Unity Catalog, clusters, jobs |
| `dashboards/queries/*.sql` | SQL for 5 Databricks dashboards |
