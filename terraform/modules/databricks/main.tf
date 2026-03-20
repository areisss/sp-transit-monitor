variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

variable "raw_bucket_name" {
  description = "S3 bucket name for raw data"
  type        = string
}

variable "checkpoints_bucket_name" {
  description = "S3 bucket name for streaming checkpoints"
  type        = string
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

# --- Unity Catalog ---

resource "databricks_catalog" "transit_monitor" {
  name    = "transit_monitor"
  comment = "SP Transit Performance Monitor - medallion architecture"
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.transit_monitor.name
  name         = "bronze"
  comment      = "Raw ingested data from SPTrans API via S3 Auto Loader"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.transit_monitor.name
  name         = "silver"
  comment      = "Cleaned, deduplicated, and enriched vehicle positions"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.transit_monitor.name
  name         = "gold"
  comment      = "Aggregated transit performance metrics"
}

resource "databricks_schema" "reference" {
  catalog_name = databricks_catalog.transit_monitor.name
  name         = "reference"
  comment      = "Reference data: GTFS schedules, census demographics"
}

# --- Cluster for Streaming Jobs ---

resource "databricks_cluster" "streaming" {
  cluster_name            = "${var.project_name}-streaming"
  spark_version           = "14.3.x-scala2.12"
  node_type_id            = "i3.xlarge"
  autotermination_minutes = 30

  autoscale {
    min_workers = 1
    max_workers = 3
  }

  aws_attributes {
    first_on_demand = 1
    spot_bid_price_percent = 100
    availability           = "SPOT_WITH_FALLBACK"
  }

  spark_conf = {
    "spark.databricks.delta.optimizeWrite.enabled" = "true"
    "spark.databricks.delta.autoCompact.enabled"   = "true"
  }

  library {
    pypi {
      package = "h3>=3.7,<5.0"
    }
  }
}

# --- Streaming Jobs ---

resource "databricks_job" "bronze_ingest" {
  name = "${var.project_name}-bronze-ingest"

  task {
    task_key = "bronze_ingest"

    existing_cluster_id = databricks_cluster.streaming.id

    spark_python_task {
      python_file = "dbfs:/repos/${var.project_name}/databricks/streaming/bronze/ingest_from_s3.py"

      parameters = [
        "--raw-bucket", var.raw_bucket_name,
        "--checkpoint-bucket", var.checkpoints_bucket_name,
      ]
    }
  }

  continuous {
    pause_status = "UNPAUSED"
  }
}

resource "databricks_job" "silver_enrich" {
  name = "${var.project_name}-silver-enrich"

  task {
    task_key = "silver_enrich"

    existing_cluster_id = databricks_cluster.streaming.id

    spark_python_task {
      python_file = "dbfs:/repos/${var.project_name}/databricks/streaming/silver/clean_enrich_gps.py"

      parameters = [
        "--checkpoint-bucket", var.checkpoints_bucket_name,
      ]
    }
  }

  continuous {
    pause_status = "UNPAUSED"
  }
}

resource "databricks_job" "silver_stop_arrivals" {
  name = "${var.project_name}-silver-stop-arrivals"

  task {
    task_key = "stop_arrival_detection"

    existing_cluster_id = databricks_cluster.streaming.id

    spark_python_task {
      python_file = "dbfs:/repos/${var.project_name}/databricks/streaming/silver/stop_arrival_detection.py"

      parameters = [
        "--checkpoint-bucket", var.checkpoints_bucket_name,
      ]
    }
  }

  continuous {
    pause_status = "UNPAUSED"
  }
}

resource "databricks_job" "gold_streaming" {
  name = "${var.project_name}-gold-streaming"

  task {
    task_key = "on_time_performance"

    existing_cluster_id = databricks_cluster.streaming.id

    spark_python_task {
      python_file = "dbfs:/repos/${var.project_name}/databricks/streaming/gold/on_time_performance.py"
      parameters  = ["--checkpoint-bucket", var.checkpoints_bucket_name]
    }
  }

  task {
    task_key = "headway_regularity"

    existing_cluster_id = databricks_cluster.streaming.id

    spark_python_task {
      python_file = "dbfs:/repos/${var.project_name}/databricks/streaming/gold/headway_regularity.py"
      parameters  = ["--checkpoint-bucket", var.checkpoints_bucket_name]
    }
  }

  task {
    task_key = "speed_congestion"

    existing_cluster_id = databricks_cluster.streaming.id

    spark_python_task {
      python_file = "dbfs:/repos/${var.project_name}/databricks/streaming/gold/speed_congestion.py"
      parameters  = ["--checkpoint-bucket", var.checkpoints_bucket_name]
    }
  }

  continuous {
    pause_status = "UNPAUSED"
  }
}

# --- Batch Jobs ---

resource "databricks_job" "gtfs_loader" {
  name = "${var.project_name}-gtfs-loader"

  task {
    task_key = "load_gtfs"

    existing_cluster_id = databricks_cluster.streaming.id

    spark_python_task {
      python_file = "dbfs:/repos/${var.project_name}/databricks/batch/load_gtfs_schedules.py"
    }
  }

  schedule {
    quartz_cron_expression = "0 0 3 * * ?"
    timezone_id            = "America/Sao_Paulo"
  }
}

resource "databricks_job" "coverage_equity" {
  name = "${var.project_name}-coverage-equity"

  task {
    task_key = "coverage_equity"

    existing_cluster_id = databricks_cluster.streaming.id

    spark_python_task {
      python_file = "dbfs:/repos/${var.project_name}/databricks/batch/coverage_equity.py"
    }
  }

  schedule {
    quartz_cron_expression = "0 0 * * * ?"
    timezone_id            = "America/Sao_Paulo"
  }
}
