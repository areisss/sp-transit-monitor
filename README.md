# SP Transit Performance Monitor

Real-time performance monitoring of Sao Paulo's bus transit system using SPTrans OlhoVivo API, AWS streaming infrastructure, and Databricks Lakehouse.

**What it does:** Ingests ~15,000 bus GPS positions every 30 seconds, processes them through a medallion architecture (Bronze → Silver → Gold), and produces dashboards answering: Are buses on time? Are they bunching? Which neighborhoods are underserved? Where is congestion worst?

```
EventBridge (30s) → Lambda (poll API) → Kinesis Data Streams → Firehose → S3 (raw JSON)
                                                                                  ↓
                                                                    Databricks Auto Loader
                                                                          ↓
                                                            Bronze → Silver → Gold (Delta)
                                                                          ↓
                                                            Databricks SQL Dashboards
```

---

## Prerequisites

You need **four accounts/services** set up before deploying anything:

### 1. SPTrans OlhoVivo API Key (free)

This is the data source — real-time GPS positions of all buses in Sao Paulo.

1. Go to [https://www.sptrans.com.br/desenvolvedores/](https://www.sptrans.com.br/desenvolvedores/)
2. Click **"Cadastre-se"** (Register) and create a developer account
3. After email verification, log in to the developer portal
4. Go to **"Aplicações"** → **"Nova Aplicação"** (New Application)
5. Fill in the form (app name, description — anything works)
6. Once created, you'll see your **API Token** — save this, you'll need it for Lambda configuration

**Test your token works:**
```bash
# Authenticate (returns "true" if successful)
curl -X POST "https://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=YOUR_TOKEN_HERE" -c cookies.txt

# Fetch positions (returns JSON with all bus positions)
curl "https://api.olhovivo.sptrans.com.br/v2.1/Posicao" -b cookies.txt
```

### 2. AWS Account

You need an AWS account with billing enabled. The estimated cost is **~$37–43/month** for the AWS portion (Lambda, Kinesis, S3, CloudWatch).

1. Go to [https://aws.amazon.com/](https://aws.amazon.com/) and create an account if you don't have one
2. Set up an IAM user (or use SSO) with programmatic access
3. Install and configure the AWS CLI:
   ```bash
   brew install awscli
   aws configure
   # Enter your Access Key ID, Secret Access Key, region (us-east-1), output (json)
   ```
4. Verify it works:
   ```bash
   aws sts get-caller-identity
   ```

**Required IAM permissions** for the deploying user:
- `AmazonKinesisFullAccess`
- `AmazonS3FullAccess`
- `AWSLambda_FullAccess`
- `AmazonEventBridgeSchedulerFullAccess`
- `IAMFullAccess` (for creating roles)
- `AmazonSSMFullAccess`
- `CloudWatchFullAccess`
- `AmazonSNSFullAccess`
- `AmazonDynamoDBFullAccess` (for Terraform state lock)

Or use `AdministratorAccess` for simplicity in a personal project.

### 3. Databricks Account

Databricks runs the streaming/batch processing and dashboards. The estimated cost is **~$100–160/month**.

1. Go to [https://www.databricks.com/try-databricks](https://www.databricks.com/try-databricks)
2. Choose **"Get started with Databricks on AWS"**
3. You'll be prompted to either:
   - **Option A (recommended for personal):** Use the Databricks-managed free trial (14 days, then pay-as-you-go)
   - **Option B:** Link to your own AWS account via a cross-account IAM role (gives you full control)
4. Once your workspace is created, you'll have:
   - A **Workspace URL** (e.g., `https://dbc-abc123.cloud.databricks.com`)
   - A **Personal Access Token** (go to User Settings → Developer → Access Tokens → Generate)

**Save these — you'll need them:**
```bash
export DATABRICKS_HOST="https://dbc-abc123.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
```

**Verify it works:**
```bash
# Install Databricks CLI
pip install databricks-cli

# Test connection
databricks workspace list /
```

### 4. Terraform (local tool)

```bash
brew install terraform
terraform --version  # Should be >= 1.7
```

### 5. Python 3.12+ (local development)

```bash
brew install python@3.12
python3.12 --version
```

---

## Project Structure

```
sp-transit-monitor/
├── .github/workflows/          # CI/CD pipelines
│   ├── ci.yml                  # Lint + tests on PR
│   ├── terraform-plan.yml      # Terraform plan on PR
│   ├── terraform-apply.yml     # Terraform apply on merge to main
│   └── lambda-deploy.yml       # Package + deploy Lambda
├── terraform/                  # Infrastructure as Code
│   ├── bootstrap/              # S3 state bucket + DynamoDB lock
│   ├── environments/dev/       # Dev environment config
│   └── modules/                # Reusable Terraform modules
│       ├── kinesis/            # Data Stream + Firehose → S3
│       ├── lambda/             # Function + EventBridge schedule
│       ├── storage/            # S3 buckets (raw, checkpoints, lambda)
│       ├── databricks/         # Workspace, clusters, jobs, catalog
│       └── monitoring/         # CloudWatch alarms + SNS
├── lambda/                     # Lambda producer (Python 3.12)
│   ├── sptrans_producer/       # Source code
│   │   ├── handler.py          # Entry point: poll API → Kinesis
│   │   ├── sptrans_client.py   # SPTrans API client
│   │   ├── models.py           # Pydantic models + normalization
│   │   └── config.py           # Environment config
│   └── tests/                  # Unit tests
├── databricks/                 # Databricks jobs (PySpark)
│   ├── streaming/
│   │   ├── bronze/             # Auto Loader: S3 → Delta
│   │   ├── silver/             # Clean, dedup, enrich, stop arrivals
│   │   └── gold/               # On-time, headway, speed, fleet
│   ├── batch/                  # GTFS loader, census, coverage equity
│   ├── schemas/                # StructType definitions
│   └── utils/                  # H3, Haversine, timezone helpers
├── dashboards/queries/         # SQL for Databricks dashboards
├── tests/                      # Spark tests + integration tests
├── docs/                       # Data dictionary, cost estimate
└── pyproject.toml              # Python project config
```

---

## Deployment Guide (Step by Step)

### Step 1: Clone and set up local environment

```bash
cd ~/dev/projects/sp-transit-monitor

# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install all dependencies
pip install -e ".[lambda,databricks,dev]"
```

### Step 2: Run tests locally

```bash
# Lambda unit tests (no external dependencies)
pytest lambda/tests/ -v

# Spark unit tests (needs Java 17 installed)
PYTHONPATH=. pytest tests/unit/databricks/ -v -m "not integration"

# Integration tests (uses moto — mock AWS)
PYTHONPATH=. pytest tests/integration/ -v
```

### Step 3: Deploy Terraform bootstrap (one-time)

This creates the S3 bucket and DynamoDB table that Terraform itself uses for state management.

```bash
cd terraform/bootstrap
terraform init
terraform plan
terraform apply
```

### Step 4: Configure secrets

```bash
# Edit terraform.tfvars with your email for alerts
cd ../environments/dev
# Edit terraform.tfvars:
#   alert_email = "your-real-email@example.com"

# Set Databricks credentials
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token-here"
```

### Step 5: Deploy main infrastructure

```bash
cd terraform/environments/dev
terraform init
terraform plan    # Review the plan carefully
terraform apply   # Type "yes" to confirm
```

This creates:
- Kinesis Data Stream (`transit-monitor-sptrans-gps-raw`)
- Kinesis Firehose → S3 delivery stream
- S3 buckets (raw, checkpoints, lambda-code)
- Lambda function + EventBridge 30s schedule
- CloudWatch alarms + SNS notifications
- Databricks Unity Catalog schemas, cluster, and jobs

### Step 6: Set the SPTrans API token

After Terraform creates the SSM parameter with a placeholder value, update it with your real token:

```bash
aws ssm put-parameter \
  --name "/transit-monitor/sptrans-api-token" \
  --value "YOUR_ACTUAL_SPTRANS_TOKEN" \
  --type SecureString \
  --overwrite
```

Then update the Lambda environment variable:
```bash
aws lambda update-function-configuration \
  --function-name transit-monitor-sptrans-producer \
  --environment "Variables={SPTRANS_API_TOKEN=YOUR_ACTUAL_SPTRANS_TOKEN,KINESIS_STREAM_NAME=transit-monitor-sptrans-gps-raw,SPTRANS_API_BASE_URL=https://api.olhovivo.sptrans.com.br/v2.1}"
```

### Step 7: Deploy Lambda code

```bash
# Package Lambda
cd lambda
pip install -r requirements.txt -t package/
cd package && zip -r9 ../../sptrans-producer.zip .
cd ../.. && cd lambda && zip -g ../sptrans-producer.zip -r sptrans_producer/
cd ..

# Upload to S3 and update Lambda
aws s3 cp sptrans-producer.zip s3://transit-monitor-lambda-code/lambda/sptrans-producer.zip

aws lambda update-function-code \
  --function-name transit-monitor-sptrans-producer \
  --s3-bucket transit-monitor-lambda-code \
  --s3-key lambda/sptrans-producer.zip
```

### Step 8: Verify ingestion is working

```bash
# Check Lambda is being invoked (wait ~1 minute)
aws logs tail /aws/lambda/transit-monitor-sptrans-producer --follow

# Check Kinesis stream has data
aws kinesis describe-stream-summary \
  --stream-name transit-monitor-sptrans-gps-raw

# Check S3 for raw files (wait ~2 minutes for Firehose buffer)
aws s3 ls s3://transit-monitor-raw/sptrans-gps/ --recursive | head -5
```

You should see:
- Lambda logs showing "Fetched ~15000 vehicle positions"
- gzipped JSON files appearing in S3 every ~60 seconds

### Step 9: Load GTFS reference data (Databricks)

Before the Silver layer can enrich data, you need GTFS reference tables:

1. Open your Databricks workspace
2. Import `databricks/batch/load_gtfs_schedules.py` as a notebook
3. Run it — this downloads SPTrans GTFS and populates:
   - `transit_monitor.reference.gtfs_routes`
   - `transit_monitor.reference.gtfs_stops`
   - `transit_monitor.reference.gtfs_stop_times`
   - `transit_monitor.reference.gtfs_shapes`
   - `transit_monitor.reference.gtfs_trips`

### Step 10: Start streaming jobs (Databricks)

The Terraform Databricks module creates the jobs, but you can also start them manually:

1. **Bronze ingest** — Run `databricks/streaming/bronze/ingest_from_s3.py` as a continuous job
2. **Silver enrichment** — Run `databricks/streaming/silver/clean_enrich_gps.py`
3. **Silver stop arrivals** — Run `databricks/streaming/silver/stop_arrival_detection.py`
4. **Gold metrics** — Run the gold jobs (on_time, headway, speed)

### Step 11: Create dashboards

1. Go to **Databricks SQL** → **Queries**
2. Create 5 queries using the SQL files in `dashboards/queries/`
3. Create a dashboard and add visualizations from each query

---

## Verification Checklist

| Check | How to verify |
|---|---|
| Lambda invocations | CloudWatch Logs: successful every 30s |
| Kinesis throughput | CloudWatch metrics: `IncomingRecords > 0` |
| S3 raw files | `aws s3 ls s3://transit-monitor-raw/sptrans-gps/` — new files every ~60s |
| Bronze table | `SELECT count(*) FROM transit_monitor.bronze.sptrans_gps_raw` — growing |
| Silver table | `SELECT count(*) FROM transit_monitor.silver.vehicle_positions_enriched` — no dups |
| Gold metrics | `SELECT * FROM transit_monitor.gold.on_time_performance LIMIT 10` |
| Alarms work | Disable EventBridge rule → alarm fires within 5 min → re-enable |

---

## Architecture Decisions

| Decision | Why |
|---|---|
| **Lambda over ECS** | No Docker, no cluster. EventBridge triggers every 30s. ~$0.50/month. |
| **Kinesis in the middle** | Decouples producer from consumer. Enables future real-time consumers. |
| **Firehose to S3** | Handles buffering, compression, partitioning. S3 = durable archive. |
| **Auto Loader over Kinesis connector** | `cloudFiles` is the Databricks-recommended pattern. Uses S3 notifications. |
| **No VPC/NAT** | Lambda only needs internet (SPTrans API + Kinesis endpoint). Saves ~$32/month. |
| **On-demand Kinesis** | Auto-scales. No shard management for a personal project. |
| **Spot instances** | Streaming cluster uses SPOT_WITH_FALLBACK. Up to 80% savings. |
| **H3 resolution 9** | ~174m edge — good for stop detection (50m radius) and neighborhood analysis. |

---

## Cost Breakdown

| Service | Monthly Cost |
|---|---|
| Lambda + EventBridge | ~$0.50 |
| Kinesis Data Streams (on-demand) | ~$20 |
| Kinesis Firehose | ~$11 |
| S3 (raw + Delta + checkpoints) | ~$3 |
| CloudWatch + SNS | ~$5 |
| Databricks (streaming cluster + SQL) | ~$100–160 |
| **Total** | **~$140–200** |

See [docs/cost_estimate.md](docs/cost_estimate.md) for detailed breakdown and optimization strategies.

---

## Key Metrics Produced

1. **On-time performance** — % of buses arriving within [-1min, +5min] of GTFS schedule
2. **Headway regularity** — Coefficient of variation of inter-arrival times (CV > 0.5 = bunching)
3. **Speed/congestion** — Average speed per H3 hex, classified as Free/Slow/Congested/Gridlock
4. **Fleet utilization** — Actual active vehicles vs GTFS planned trips per route
5. **Coverage equity** — Buses per hour per neighborhood, correlated with census income data

---

## Data Dictionary

See [docs/data_dictionary.md](docs/data_dictionary.md) for complete schema documentation of all 11 tables across Bronze, Silver, Gold, and Reference layers.

---

## CI/CD

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | PR to main | Ruff lint + Lambda tests + Spark tests |
| `terraform-plan.yml` | PR touching `terraform/` | `terraform plan` + PR comment |
| `terraform-apply.yml` | Merge to main touching `terraform/` | `terraform apply` |
| `lambda-deploy.yml` | Merge to main touching `lambda/` | Package zip → S3 → update function |

**GitHub Secrets needed:**
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

---

## Stopping / Tearing Down

To stop incurring costs:

```bash
# 1. Disable the EventBridge schedule (stops Lambda invocations)
aws scheduler delete-schedule --name transit-monitor-sptrans-poll-30s

# 2. Stop Databricks cluster and jobs (from Databricks UI or API)

# 3. Full teardown (destroys everything)
cd terraform/environments/dev
terraform destroy
```

**Warning:** `terraform destroy` deletes all S3 data, Kinesis streams, and Databricks resources permanently.
