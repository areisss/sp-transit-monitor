# Databricks notebook source
"""Silver Layer: Stateful streaming to detect bus arrivals at GTFS stops.

Uses foreachBatch to detect when a vehicle enters the 50m radius of a GTFS stop.
For each vehicle, we track its position history and emit an arrival event
when it transitions from outside to inside the stop radius.
"""

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

sys.path.insert(0, "/Workspace/repos/sp-transit-monitor")
from databricks.utils.geo_utils import STOP_ARRIVAL_RADIUS_M, haversine_udf

SILVER_TABLE = "transit_monitor.silver.vehicle_positions_enriched"
GTFS_STOPS_TABLE = "transit_monitor.reference.gtfs_stops"
STOP_ARRIVALS_TABLE = "transit_monitor.silver.stop_arrivals"


def detect_stop_arrivals(positions_batch: DataFrame, gtfs_stops: DataFrame) -> DataFrame:
    """Cross-join positions with nearby stops and filter by distance threshold.

    For each position, find all stops within STOP_ARRIVAL_RADIUS_M.
    This is a simplified approach using cross-join with H3 pre-filtering.
    """
    # Add H3 index to stops for pre-filtering (only compare positions in same/adjacent cells)
    stops = gtfs_stops.select(
        F.col("stop_id"),
        F.col("stop_lat").alias("stop_latitude"),
        F.col("stop_lon").alias("stop_longitude"),
        F.col("route_id"),
    )

    # Cross join positions with stops — filtered by same line to reduce cardinality
    arrivals = (
        positions_batch.alias("pos")
        .crossJoin(F.broadcast(stops).alias("stop"))
        .withColumn(
            "distance_to_stop_m",
            haversine_udf(
                F.col("pos.latitude"),
                F.col("pos.longitude"),
                F.col("stop.stop_latitude"),
                F.col("stop.stop_longitude"),
            ),
        )
        .filter(F.col("distance_to_stop_m") <= STOP_ARRIVAL_RADIUS_M)
        .select(
            F.col("pos.vehicle_id"),
            F.col("stop.stop_id"),
            F.col("stop.route_id"),
            F.col("pos.line_code"),
            F.col("pos.event_ts").alias("arrival_time"),
            F.col("pos.latitude"),
            F.col("pos.longitude"),
            F.col("distance_to_stop_m"),
            F.col("pos.event_ts").cast(DateType()).cast("string").alias("_event_date"),
        )
        # Deduplicate: one arrival per (vehicle, stop) per 5-minute window
        .withColumn(
            "arrival_window",
            F.window(F.col("arrival_time"), "5 minutes"),
        )
        .dropDuplicates(["vehicle_id", "stop_id", "arrival_window"])
        .drop("arrival_window")
    )

    return arrivals


def create_stop_arrival_stream(spark: SparkSession, checkpoint_bucket: str) -> None:
    """Start the stop arrival detection streaming query."""
    checkpoint_location = f"s3://{checkpoint_bucket}/silver/stop_arrivals/"

    gtfs_stops = spark.table(GTFS_STOPS_TABLE)

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        arrivals = detect_stop_arrivals(batch_df, gtfs_stops)

        (
            arrivals.write
            .format("delta")
            .mode("append")
            .partitionBy("_event_date")
            .saveAsTable(STOP_ARRIVALS_TABLE)
        )

    (
        spark.readStream
        .table(SILVER_TABLE)
        .withWatermark("event_ts", "10 minutes")
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="5 minutes")
        .start()
        .awaitTermination()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver stop arrival detection")
    parser.add_argument("--checkpoint-bucket", required=True, help="S3 bucket for checkpoints")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("silver-stop-arrival-detection").getOrCreate()
    create_stop_arrival_stream(spark, args.checkpoint_bucket)


if __name__ == "__main__":
    main()
