# Databricks notebook source
"""Batch Job: Load IBGE census demographics data.

Downloads census tract data (population, income, density) from IBGE
and creates H3-indexed reference table for equity analysis.
Runs monthly.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, "/Workspace/repos/sp-transit-monitor")
from databricks.utils.geo_utils import h3_index_udf

CENSUS_TABLE = "transit_monitor.reference.census_demographics"

# IBGE census data - simplified schema for Sao Paulo municipality
CENSUS_SCHEMA = StructType(
    [
        StructField("tract_code", StringType(), nullable=False),
        StructField("municipality_code", StringType(), nullable=False),
        StructField("municipality_name", StringType(), nullable=True),
        StructField("population", IntegerType(), nullable=True),
        StructField("households", IntegerType(), nullable=True),
        StructField("avg_income_brl", DoubleType(), nullable=True),
        StructField("area_km2", DoubleType(), nullable=True),
        StructField("centroid_lat", DoubleType(), nullable=False),
        StructField("centroid_lon", DoubleType(), nullable=False),
    ]
)

# Sao Paulo municipality IBGE code
SP_MUNICIPALITY_CODE = "3550308"


def load_census_data(spark: SparkSession, census_path: str) -> "DataFrame":
    """Load census data from CSV/Parquet and filter to Sao Paulo."""
    census = (
        spark.read.option("header", "true")
        .schema(CENSUS_SCHEMA)
        .csv(census_path)
        .filter(F.col("municipality_code") == SP_MUNICIPALITY_CODE)
    )

    return (
        census.withColumn("h3_index", h3_index_udf(F.col("centroid_lat"), F.col("centroid_lon")))
        .withColumn(
            "population_density",
            F.when(F.col("area_km2") > 0, F.col("population") / F.col("area_km2")).otherwise(F.lit(None)),
        )
        .withColumn(
            "income_bracket",
            F.when(F.col("avg_income_brl") < 1500, F.lit("low"))
            .when(F.col("avg_income_brl") < 4000, F.lit("medium"))
            .when(F.col("avg_income_brl") < 10000, F.lit("high"))
            .otherwise(F.lit("very_high")),
        )
        .withColumn("_loaded_at", F.current_timestamp())
    )


def main() -> None:
    spark = SparkSession.builder.appName("load-census-demographics").getOrCreate()

    # Census data should be pre-staged to this location
    census_path = "s3://transit-monitor-raw/reference/ibge-census/"

    census_df = load_census_data(spark, census_path)

    (census_df.write.format("delta").mode("overwrite").saveAsTable(CENSUS_TABLE))

    print(f"Census load complete: {census_df.count()} tracts in Sao Paulo")


if __name__ == "__main__":
    main()
