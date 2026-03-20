# Databricks notebook source
"""Gold Layer (Batch): Coverage equity analysis.

Computes buses per hour per H3 hexagon and joins with IBGE census data
to analyze whether low-income neighborhoods receive equitable transit service.

Runs hourly.
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

sys.path.insert(0, "/Workspace/repos/sp-transit-monitor")
from databricks.utils.time_utils import classify_time_period, extract_hour_of_day, is_weekday

SILVER_TABLE = "transit_monitor.silver.vehicle_positions_enriched"
CENSUS_TABLE = "transit_monitor.reference.census_demographics"
GOLD_TABLE = "transit_monitor.gold.coverage_equity"


def compute_service_frequency(positions: DataFrame, target_date: str) -> DataFrame:
    """Count unique vehicles per H3 hex per hour."""
    return (
        positions
        .filter(F.col("_event_date") == target_date)
        .withColumn("hour_of_day", extract_hour_of_day(F.col("event_ts")))
        .withColumn("time_period", classify_time_period(F.col("hour_of_day")))
        .withColumn("is_weekday", is_weekday(F.col("event_ts")))
        .groupBy("h3_index", "hour_of_day", "time_period", "is_weekday")
        .agg(
            F.countDistinct("vehicle_id").alias("unique_vehicles"),
            F.count("*").alias("total_observations"),
            F.countDistinct("line_code").alias("unique_routes"),
        )
    )


def join_with_census(service_freq: DataFrame, census: DataFrame) -> DataFrame:
    """Join service frequency with census demographics per H3 hex."""
    census_h3 = (
        census
        .groupBy("h3_index")
        .agg(
            F.sum("population").alias("population"),
            F.sum("households").alias("households"),
            F.avg("avg_income_brl").alias("avg_income_brl"),
            F.avg("population_density").alias("population_density"),
            F.first("income_bracket").alias("income_bracket"),
        )
    )

    return (
        service_freq.alias("sf")
        .join(census_h3.alias("c"), "h3_index", "left")
        .withColumn(
            "vehicles_per_1k_population",
            F.when(
                F.col("population") > 0,
                F.col("unique_vehicles") / (F.col("population") / 1000),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "service_equity_score",
            F.when(
                (F.col("income_bracket") == "low") & (F.col("vehicles_per_1k_population") < 2),
                F.lit("underserved"),
            )
            .when(
                (F.col("income_bracket") == "low") & (F.col("vehicles_per_1k_population") >= 2),
                F.lit("adequate"),
            )
            .when(F.col("income_bracket").isin("high", "very_high"), F.lit("well_served"))
            .otherwise(F.lit("moderate")),
        )
    )


def main() -> None:
    spark = SparkSession.builder.appName("gold-coverage-equity").getOrCreate()

    # Process the most recent complete hour
    target_date = spark.sql("SELECT date_sub(current_date(), 0) AS d").collect()[0]["d"].isoformat()

    positions = spark.table(SILVER_TABLE)
    census = spark.table(CENSUS_TABLE)

    service_freq = compute_service_frequency(positions, target_date)
    equity = join_with_census(service_freq, census)

    (
        equity
        .withColumn("_event_date", F.lit(target_date))
        .write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_event_date = '{target_date}'")
        .saveAsTable(GOLD_TABLE)
    )


if __name__ == "__main__":
    main()
