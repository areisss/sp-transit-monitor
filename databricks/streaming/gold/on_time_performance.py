# Databricks notebook source
"""Gold Layer: On-time performance metrics.

Compares actual stop arrivals against GTFS scheduled stop times.
On-time = arrival within [-1 min, +5 min] of scheduled time.

Produces 5-minute windowed aggregations of on-time percentages
by route, stop, time period, and day type.
"""

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

sys.path.insert(0, "/Workspace/repos/sp-transit-monitor")
from databricks.utils.time_utils import classify_time_period, extract_hour_of_day, is_weekday

STOP_ARRIVALS_TABLE = "transit_monitor.silver.stop_arrivals"
GTFS_STOP_TIMES_TABLE = "transit_monitor.reference.gtfs_stop_times"
GOLD_TABLE = "transit_monitor.gold.on_time_performance"

ON_TIME_EARLY_THRESHOLD_SEC = -60  # 1 minute early
ON_TIME_LATE_THRESHOLD_SEC = 300  # 5 minutes late


def classify_on_time(delay_seconds_col: F.Column) -> F.Column:
    """Classify arrival delay into categories."""
    return (
        F.when(delay_seconds_col < ON_TIME_EARLY_THRESHOLD_SEC, F.lit("early"))
        .when(delay_seconds_col <= ON_TIME_LATE_THRESHOLD_SEC, F.lit("on_time"))
        .otherwise(F.lit("late"))
    )


def compute_on_time_metrics(arrivals: DataFrame, gtfs_stop_times: DataFrame) -> DataFrame:
    """Join arrivals with scheduled times and compute on-time performance."""
    # Join arrivals with nearest scheduled time for the same stop + route
    scheduled = gtfs_stop_times.select(
        F.col("stop_id").alias("sched_stop_id"),
        F.col("route_id").alias("sched_route_id"),
        F.col("arrival_time").alias("scheduled_arrival"),
        F.col("trip_id"),
    )

    with_schedule = (
        arrivals.alias("a")
        .join(
            scheduled.alias("s"),
            (F.col("a.stop_id") == F.col("s.sched_stop_id")) & (F.col("a.route_id") == F.col("s.sched_route_id")),
            "inner",
        )
        .withColumn(
            "delay_seconds",
            F.unix_timestamp(F.col("a.arrival_time")) - F.unix_timestamp(F.col("s.scheduled_arrival")),
        )
        # Keep only the closest scheduled trip for each arrival
        .withColumn(
            "abs_delay",
            F.abs(F.col("delay_seconds")),
        )
        .withColumn(
            "rank",
            F.row_number().over(
                F.Window.partitionBy("a.vehicle_id", "a.stop_id", "a.arrival_time").orderBy("abs_delay")
            ),
        )
        .filter(F.col("rank") == 1)
        .drop("rank", "abs_delay", "sched_stop_id", "sched_route_id")
    )

    return (
        with_schedule.withColumn("on_time_status", classify_on_time(F.col("delay_seconds")))
        .withColumn("hour_of_day", extract_hour_of_day(F.col("arrival_time")))
        .withColumn("time_period", classify_time_period(F.col("hour_of_day")))
        .withColumn("is_weekday", is_weekday(F.col("arrival_time")))
        .withColumn("_event_date", F.col("arrival_time").cast(DateType()).cast("string"))
    )


def aggregate_on_time(df: DataFrame) -> DataFrame:
    """Aggregate on-time performance by route, time period, and day type."""
    return (
        df.groupBy(
            F.window(F.col("arrival_time"), "1 hour").alias("time_window"),
            "route_id",
            "line_code",
            "time_period",
            "is_weekday",
            "_event_date",
        )
        .agg(
            F.count("*").alias("total_arrivals"),
            F.sum(F.when(F.col("on_time_status") == "on_time", 1).otherwise(0)).alias("on_time_count"),
            F.sum(F.when(F.col("on_time_status") == "early", 1).otherwise(0)).alias("early_count"),
            F.sum(F.when(F.col("on_time_status") == "late", 1).otherwise(0)).alias("late_count"),
            F.avg("delay_seconds").alias("avg_delay_seconds"),
            F.percentile_approx("delay_seconds", 0.5).alias("median_delay_seconds"),
            F.percentile_approx("delay_seconds", 0.95).alias("p95_delay_seconds"),
        )
        .withColumn("on_time_pct", F.round(F.col("on_time_count") / F.col("total_arrivals") * 100, 2))
        .withColumn("window_start", F.col("time_window.start"))
        .withColumn("window_end", F.col("time_window.end"))
        .drop("time_window")
    )


def create_on_time_stream(spark: SparkSession, checkpoint_bucket: str) -> None:
    """Start the on-time performance streaming aggregation."""
    checkpoint_location = f"s3://{checkpoint_bucket}/gold/on_time_performance/"

    gtfs_stop_times = spark.table(GTFS_STOP_TIMES_TABLE)

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        metrics = compute_on_time_metrics(batch_df, gtfs_stop_times)
        aggregated = aggregate_on_time(metrics)

        (aggregated.write.format("delta").mode("append").partitionBy("_event_date").saveAsTable(GOLD_TABLE))

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
    parser = argparse.ArgumentParser(description="Gold on-time performance metrics")
    parser.add_argument("--checkpoint-bucket", required=True, help="S3 bucket for checkpoints")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("gold-on-time-performance").getOrCreate()
    create_on_time_stream(spark, args.checkpoint_bucket)


if __name__ == "__main__":
    main()
