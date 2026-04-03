# Databricks notebook source
"""Gold Layer: Headway regularity metrics.

Measures the time between consecutive bus arrivals at the same stop on the same route.
Even headways indicate good service; high coefficient of variation (CV > 0.5)
indicates bus bunching.

Produces hourly aggregations of headway statistics by route and stop.
"""

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

sys.path.insert(0, "/Workspace/repos/sp-transit-monitor")
from databricks.utils.time_utils import classify_time_period, extract_hour_of_day, is_weekday

STOP_ARRIVALS_TABLE = "transit_monitor.silver.stop_arrivals"
GOLD_TABLE = "transit_monitor.gold.headway_regularity"


def compute_headways(arrivals: DataFrame) -> DataFrame:
    """Compute headway (time between consecutive arrivals) at each stop+route."""
    window_spec = Window.partitionBy("stop_id", "line_code").orderBy("arrival_time")

    return (
        arrivals.withColumn("prev_arrival_time", F.lag("arrival_time").over(window_spec))
        .withColumn("prev_vehicle_id", F.lag("vehicle_id").over(window_spec))
        .filter(F.col("prev_arrival_time").isNotNull())
        .withColumn(
            "headway_seconds",
            F.unix_timestamp("arrival_time") - F.unix_timestamp("prev_arrival_time"),
        )
        # Filter out unreasonable headways (< 30s or > 2 hours)
        .filter((F.col("headway_seconds") >= 30) & (F.col("headway_seconds") <= 7200))
    )


def aggregate_headway_metrics(headways: DataFrame) -> DataFrame:
    """Aggregate headway statistics per route, stop, and time window."""
    return (
        headways.withColumn("hour_of_day", extract_hour_of_day(F.col("arrival_time")))
        .withColumn("time_period", classify_time_period(F.col("hour_of_day")))
        .withColumn("is_weekday", is_weekday(F.col("arrival_time")))
        .withColumn("_event_date", F.col("arrival_time").cast(DateType()).cast("string"))
        .groupBy(
            F.window(F.col("arrival_time"), "1 hour").alias("time_window"),
            "stop_id",
            "line_code",
            "route_id",
            "time_period",
            "is_weekday",
            "_event_date",
        )
        .agg(
            F.count("*").alias("arrival_count"),
            F.avg("headway_seconds").alias("avg_headway_seconds"),
            F.stddev("headway_seconds").alias("stddev_headway_seconds"),
            F.min("headway_seconds").alias("min_headway_seconds"),
            F.max("headway_seconds").alias("max_headway_seconds"),
            F.percentile_approx("headway_seconds", 0.5).alias("median_headway_seconds"),
        )
        .withColumn(
            "headway_cv",
            F.when(
                F.col("avg_headway_seconds") > 0, F.col("stddev_headway_seconds") / F.col("avg_headway_seconds")
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "bunching_severity",
            F.when(F.col("headway_cv") > 1.0, F.lit("critical"))
            .when(F.col("headway_cv") > 0.5, F.lit("severe"))
            .when(F.col("headway_cv") > 0.3, F.lit("moderate"))
            .otherwise(F.lit("regular")),
        )
        .withColumn("window_start", F.col("time_window.start"))
        .withColumn("window_end", F.col("time_window.end"))
        .drop("time_window")
    )


def create_headway_stream(spark: SparkSession, checkpoint_bucket: str) -> None:
    """Start the headway regularity streaming aggregation."""
    checkpoint_location = f"s3://{checkpoint_bucket}/gold/headway_regularity/"

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        headways = compute_headways(batch_df)
        metrics = aggregate_headway_metrics(headways)

        (metrics.write.format("delta").mode("append").partitionBy("_event_date").saveAsTable(GOLD_TABLE))

    (
        spark.readStream.table(STOP_ARRIVALS_TABLE)
        .withWatermark("arrival_time", "15 minutes")
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="5 minutes")
        .start()
        .awaitTermination()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold headway regularity metrics")
    parser.add_argument("--checkpoint-bucket", required=True, help="S3 bucket for checkpoints")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("gold-headway-regularity").getOrCreate()
    create_headway_stream(spark, args.checkpoint_bucket)


if __name__ == "__main__":
    main()
