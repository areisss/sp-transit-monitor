# Databricks notebook source
"""Bronze Layer: Auto Loader ingestion from S3 raw JSON files to Delta.

Reads gzipped JSON files deposited by Kinesis Firehose and writes to the
Bronze Delta table with minimal transformation (add lineage columns).
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/Workspace/repos/sp-transit-monitor")
from databricks.schemas.bronze_schemas import SPTRANS_GPS_RAW_SCHEMA

BRONZE_TABLE = "transit_monitor.bronze.sptrans_gps_raw"


def create_bronze_stream(spark: SparkSession, raw_bucket: str, checkpoint_bucket: str) -> None:
    """Start the Bronze Auto Loader streaming query."""
    raw_path = f"s3://{raw_bucket}/sptrans-gps/"
    schema_location = f"s3://{checkpoint_bucket}/bronze/schema/"
    checkpoint_location = f"s3://{checkpoint_bucket}/bronze/sptrans_gps/"

    (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.useNotifications", "true")
        .schema(SPTRANS_GPS_RAW_SCHEMA)
        .load(raw_path)
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("_source_file", F.input_file_name())
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .partitionBy("_event_date")
        .trigger(processingTime="30 seconds")
        .toTable(BRONZE_TABLE)
        .awaitTermination()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze Auto Loader ingestion")
    parser.add_argument("--raw-bucket", required=True, help="S3 bucket with raw Firehose files")
    parser.add_argument("--checkpoint-bucket", required=True, help="S3 bucket for checkpoints")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("bronze-sptrans-gps-ingest").getOrCreate()
    create_bronze_stream(spark, args.raw_bucket, args.checkpoint_bucket)


if __name__ == "__main__":
    main()
