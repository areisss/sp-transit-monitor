# Databricks notebook source
"""Gold Layer: Fleet utilization metrics (daily batch).

Compares the number of unique vehicles observed per route against the number
of planned trips from GTFS to identify under-served routes.
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/Workspace/repos/sp-transit-monitor")

SILVER_TABLE = "transit_monitor.silver.vehicle_positions_enriched"
GTFS_ROUTES_TABLE = "transit_monitor.reference.gtfs_routes"
GTFS_STOP_TIMES_TABLE = "transit_monitor.reference.gtfs_stop_times"
GOLD_TABLE = "transit_monitor.gold.fleet_utilization"


def compute_actual_fleet(positions: DataFrame, target_date: str) -> DataFrame:
    """Count distinct active vehicles per route on the target date."""
    return (
        positions.filter(F.col("_event_date") == target_date)
        .groupBy("line_code", "line_number")
        .agg(
            F.countDistinct("vehicle_id").alias("actual_vehicles"),
            F.count("*").alias("total_observations"),
            F.min("event_ts").alias("first_seen"),
            F.max("event_ts").alias("last_seen"),
        )
    )


def compute_planned_trips(gtfs_stop_times: DataFrame, gtfs_routes: DataFrame) -> DataFrame:
    """Count planned trips per route from GTFS schedule."""
    return (
        gtfs_stop_times.select("route_id", "trip_id")
        .distinct()
        .groupBy("route_id")
        .agg(F.countDistinct("trip_id").alias("planned_trips"))
        .join(
            gtfs_routes.select("route_id", "line_code", "route_short_name", "route_long_name"),
            "route_id",
            "inner",
        )
    )


def compute_fleet_utilization(actual: DataFrame, planned: DataFrame, target_date: str) -> DataFrame:
    """Join actual vs planned and compute utilization ratio."""
    return (
        actual.alias("a")
        .join(
            planned.alias("p"),
            F.col("a.line_code") == F.col("p.line_code"),
            "full_outer",
        )
        .select(
            F.coalesce(F.col("a.line_code"), F.col("p.line_code")).alias("line_code"),
            F.coalesce(F.col("a.line_number"), F.col("p.route_short_name")).alias("line_number"),
            F.col("p.route_long_name"),
            F.coalesce(F.col("a.actual_vehicles"), F.lit(0)).alias("actual_vehicles"),
            F.coalesce(F.col("p.planned_trips"), F.lit(0)).alias("planned_trips"),
            F.col("a.total_observations"),
            F.col("a.first_seen"),
            F.col("a.last_seen"),
        )
        .withColumn(
            "utilization_ratio",
            F.when(F.col("planned_trips") > 0, F.col("actual_vehicles") / F.col("planned_trips")).otherwise(
                F.lit(None)
            ),
        )
        .withColumn(
            "status",
            F.when(F.col("actual_vehicles") == 0, F.lit("no_service"))
            .when(F.col("utilization_ratio") < 0.5, F.lit("severely_underserved"))
            .when(F.col("utilization_ratio") < 0.8, F.lit("underserved"))
            .when(F.col("utilization_ratio") <= 1.2, F.lit("normal"))
            .otherwise(F.lit("over_capacity")),
        )
        .withColumn("_event_date", F.lit(target_date))
    )


def main() -> None:
    spark = SparkSession.builder.appName("gold-fleet-utilization").getOrCreate()

    # Use yesterday as the target date by default
    target_date = spark.sql("SELECT date_sub(current_date(), 1) AS d").collect()[0]["d"].isoformat()

    positions = spark.table(SILVER_TABLE)
    gtfs_stop_times = spark.table(GTFS_STOP_TIMES_TABLE)
    gtfs_routes = spark.table(GTFS_ROUTES_TABLE)

    actual = compute_actual_fleet(positions, target_date)
    planned = compute_planned_trips(gtfs_stop_times, gtfs_routes)
    utilization = compute_fleet_utilization(actual, planned, target_date)

    (
        utilization.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_event_date = '{target_date}'")
        .saveAsTable(GOLD_TABLE)
    )


if __name__ == "__main__":
    main()
