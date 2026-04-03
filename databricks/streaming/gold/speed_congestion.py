# Databricks notebook source
"""Gold Layer: Speed and congestion metrics.

Computes average vehicle speeds per route segment and H3 hexagon,
classifying congestion levels: Free (>20 km/h), Slow (10-20),
Congested (5-10), Gridlock (<5).

Uses consecutive position pairs to calculate instantaneous speed.
"""

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

sys.path.insert(0, "/Workspace/repos/sp-transit-monitor")
from databricks.utils.geo_utils import MAX_BUS_SPEED_KMH, haversine_udf
from databricks.utils.time_utils import classify_time_period, extract_hour_of_day, is_weekday

SILVER_TABLE = "transit_monitor.silver.vehicle_positions_enriched"
GOLD_TABLE = "transit_monitor.gold.speed_congestion"


def compute_instantaneous_speed(df: DataFrame) -> DataFrame:
    """Compute speed between consecutive GPS positions for each vehicle."""
    window_spec = Window.partitionBy("vehicle_id").orderBy("event_ts")

    return (
        df.withColumn("prev_lat", F.lag("latitude").over(window_spec))
        .withColumn("prev_lng", F.lag("longitude").over(window_spec))
        .withColumn("prev_ts", F.lag("event_ts").over(window_spec))
        .filter(F.col("prev_ts").isNotNull())
        .withColumn(
            "distance_m",
            haversine_udf(F.col("prev_lat"), F.col("prev_lng"), F.col("latitude"), F.col("longitude")),
        )
        .withColumn(
            "time_delta_seconds",
            F.unix_timestamp("event_ts") - F.unix_timestamp("prev_ts"),
        )
        .filter(F.col("time_delta_seconds") > 0)
        .withColumn(
            "speed_kmh",
            (F.col("distance_m") / 1000.0) / (F.col("time_delta_seconds") / 3600.0),
        )
        # Filter out implausible speeds (GPS jumps)
        .filter(F.col("speed_kmh") <= MAX_BUS_SPEED_KMH)
        .drop("prev_lat", "prev_lng", "prev_ts")
    )


def classify_congestion(speed_col: F.Column) -> F.Column:
    """Classify average speed into congestion levels."""
    return (
        F.when(speed_col > 20.0, F.lit("free_flow"))
        .when(speed_col > 10.0, F.lit("slow"))
        .when(speed_col > 5.0, F.lit("congested"))
        .otherwise(F.lit("gridlock"))
    )


def aggregate_speed_metrics(speeds: DataFrame) -> DataFrame:
    """Aggregate speed by H3 hex, route, and time window."""
    return (
        speeds.withColumn("hour_of_day", extract_hour_of_day(F.col("event_ts")))
        .withColumn("time_period", classify_time_period(F.col("hour_of_day")))
        .withColumn("is_weekday", is_weekday(F.col("event_ts")))
        .withColumn("_event_date", F.col("event_ts").cast(DateType()).cast("string"))
        .groupBy(
            F.window(F.col("event_ts"), "1 hour").alias("time_window"),
            "h3_index",
            "line_code",
            "line_number",
            "time_period",
            "is_weekday",
            "_event_date",
        )
        .agg(
            F.count("*").alias("observation_count"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.stddev("speed_kmh").alias("stddev_speed_kmh"),
            F.percentile_approx("speed_kmh", 0.1).alias("p10_speed_kmh"),
            F.percentile_approx("speed_kmh", 0.5).alias("median_speed_kmh"),
            F.percentile_approx("speed_kmh", 0.9).alias("p90_speed_kmh"),
            F.avg("distance_m").alias("avg_segment_distance_m"),
        )
        .withColumn("congestion_level", classify_congestion(F.col("avg_speed_kmh")))
        .withColumn("window_start", F.col("time_window.start"))
        .withColumn("window_end", F.col("time_window.end"))
        .drop("time_window")
    )


def create_speed_stream(spark: SparkSession, checkpoint_bucket: str) -> None:
    """Start the speed/congestion streaming aggregation."""
    checkpoint_location = f"s3://{checkpoint_bucket}/gold/speed_congestion/"

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        speeds = compute_instantaneous_speed(batch_df)
        metrics = aggregate_speed_metrics(speeds)

        (metrics.write.format("delta").mode("append").partitionBy("_event_date").saveAsTable(GOLD_TABLE))

    (
        spark.readStream.table(SILVER_TABLE)
        .withWatermark("event_ts", "10 minutes")
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="5 minutes")
        .start()
        .awaitTermination()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold speed and congestion metrics")
    parser.add_argument("--checkpoint-bucket", required=True, help="S3 bucket for checkpoints")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("gold-speed-congestion").getOrCreate()
    create_speed_stream(spark, args.checkpoint_bucket)


if __name__ == "__main__":
    main()
