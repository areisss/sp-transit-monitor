# Databricks notebook source
"""Silver Layer: Clean, deduplicate, and enrich vehicle GPS positions.

Transforms:
1. Parse timestamps and filter to Sao Paulo metro area bounds
2. Watermark at 10 minutes on event_ts
3. Deduplicate within watermark on (vehicle_id, event_ts)
4. Broadcast join with GTFS routes for route name/type enrichment
5. Compute H3 hexagon index at resolution 9
"""

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

sys.path.insert(0, "/Workspace/repos/sp-transit-monitor")
from databricks.utils.geo_utils import h3_index_udf, is_within_sp_bounds
from databricks.utils.time_utils import extract_hour_of_day, is_weekday

BRONZE_TABLE = "transit_monitor.bronze.sptrans_gps_raw"
SILVER_TABLE = "transit_monitor.silver.vehicle_positions_enriched"
GTFS_ROUTES_TABLE = "transit_monitor.reference.gtfs_routes"


def parse_timestamps(df: DataFrame) -> DataFrame:
    """Parse string timestamps to proper TimestampType columns."""
    return (
        df.withColumn("event_ts", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("ingestion_ts", F.to_timestamp(F.col("ingestion_timestamp")))
        .drop("event_timestamp", "ingestion_timestamp")
    )


def filter_sp_bounds(df: DataFrame) -> DataFrame:
    """Remove positions outside the Sao Paulo metropolitan area."""
    return df.filter(is_within_sp_bounds(F.col("latitude"), F.col("longitude")))


def enrich_with_gtfs(df: DataFrame, gtfs_routes: DataFrame) -> DataFrame:
    """Broadcast join with GTFS routes to add route name and type."""
    gtfs_slim = gtfs_routes.select(
        F.col("route_id").alias("gtfs_route_id"),
        F.col("route_short_name"),
        F.col("route_long_name"),
        F.col("route_type"),
        F.col("line_code").alias("gtfs_line_code"),
    )

    return df.join(F.broadcast(gtfs_slim), df["line_code"] == gtfs_slim["gtfs_line_code"], "left").drop(
        "gtfs_line_code", "gtfs_route_id"
    )


def add_h3_index(df: DataFrame) -> DataFrame:
    """Compute H3 hexagon cell index for each position."""
    return df.withColumn("h3_index", h3_index_udf(F.col("latitude"), F.col("longitude")))


def add_time_features(df: DataFrame) -> DataFrame:
    """Add derived time features for downstream analysis."""
    return (
        df.withColumn("hour_of_day", extract_hour_of_day(F.col("event_ts")))
        .withColumn("is_weekday", is_weekday(F.col("event_ts")))
        .withColumn("_event_date", F.col("event_ts").cast(DateType()).cast("string"))
    )


def create_silver_stream(spark: SparkSession, checkpoint_bucket: str) -> None:
    """Start the Silver streaming query."""
    checkpoint_location = f"s3://{checkpoint_bucket}/silver/vehicle_positions_enriched/"

    gtfs_routes = spark.table(GTFS_ROUTES_TABLE)

    bronze_stream = (
        spark.readStream.table(BRONZE_TABLE)
        .transform(parse_timestamps)
        .withWatermark("event_ts", "10 minutes")
        .dropDuplicatesWithinWatermark(["vehicle_id", "event_ts"])
        .transform(filter_sp_bounds)
        .transform(lambda df: enrich_with_gtfs(df, gtfs_routes))
        .transform(add_h3_index)
        .transform(add_time_features)
        .drop("_bronze_ingested_at", "_source_file")
    )

    (
        bronze_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .partitionBy("_event_date")
        .trigger(processingTime="30 seconds")
        .toTable(SILVER_TABLE)
        .awaitTermination()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver GPS cleaning and enrichment")
    parser.add_argument("--checkpoint-bucket", required=True, help="S3 bucket for checkpoints")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("silver-vehicle-positions-enriched").getOrCreate()
    create_silver_stream(spark, args.checkpoint_bucket)


if __name__ == "__main__":
    main()
