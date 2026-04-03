# Databricks notebook source
"""Batch Job: Download and parse GTFS schedule data from SPTrans.

Loads the GTFS static feed (routes, stops, stop_times, shapes, trips)
into Unity Catalog reference tables. Runs daily at 03:00 UTC.

SPTrans GTFS feed: http://www.sptrans.com.br/developers/
"""

import tempfile
import zipfile

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

GTFS_DOWNLOAD_URL = "https://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS"

ROUTES_TABLE = "transit_monitor.reference.gtfs_routes"
STOPS_TABLE = "transit_monitor.reference.gtfs_stops"
STOP_TIMES_TABLE = "transit_monitor.reference.gtfs_stop_times"
SHAPES_TABLE = "transit_monitor.reference.gtfs_shapes"
TRIPS_TABLE = "transit_monitor.reference.gtfs_trips"

ROUTES_SCHEMA = StructType(
    [
        StructField("route_id", StringType(), nullable=False),
        StructField("agency_id", StringType(), nullable=True),
        StructField("route_short_name", StringType(), nullable=True),
        StructField("route_long_name", StringType(), nullable=True),
        StructField("route_type", IntegerType(), nullable=True),
        StructField("route_color", StringType(), nullable=True),
        StructField("route_text_color", StringType(), nullable=True),
    ]
)

STOPS_SCHEMA = StructType(
    [
        StructField("stop_id", StringType(), nullable=False),
        StructField("stop_name", StringType(), nullable=True),
        StructField("stop_lat", DoubleType(), nullable=False),
        StructField("stop_lon", DoubleType(), nullable=False),
    ]
)

STOP_TIMES_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), nullable=False),
        StructField("arrival_time", StringType(), nullable=True),
        StructField("departure_time", StringType(), nullable=True),
        StructField("stop_id", StringType(), nullable=False),
        StructField("stop_sequence", IntegerType(), nullable=True),
    ]
)

SHAPES_SCHEMA = StructType(
    [
        StructField("shape_id", StringType(), nullable=False),
        StructField("shape_pt_lat", DoubleType(), nullable=False),
        StructField("shape_pt_lon", DoubleType(), nullable=False),
        StructField("shape_pt_sequence", IntegerType(), nullable=False),
    ]
)

TRIPS_SCHEMA = StructType(
    [
        StructField("route_id", StringType(), nullable=False),
        StructField("service_id", StringType(), nullable=True),
        StructField("trip_id", StringType(), nullable=False),
        StructField("trip_headsign", StringType(), nullable=True),
        StructField("direction_id", IntegerType(), nullable=True),
        StructField("shape_id", StringType(), nullable=True),
    ]
)


def download_gtfs(url: str, dest_dir: str) -> str:
    """Download and extract the GTFS zip file. Returns the extraction directory."""
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    zip_path = f"{dest_dir}/gtfs.zip"
    with open(zip_path, "wb") as f:
        f.write(resp.content)

    extract_dir = f"{dest_dir}/gtfs"
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    return extract_dir


def load_gtfs_csv(spark: SparkSession, path: str, schema: StructType) -> "DataFrame":
    """Read a GTFS CSV file with the given schema."""
    return spark.read.option("header", "true").schema(schema).csv(path)


def add_line_code_to_routes(routes_df: "DataFrame") -> "DataFrame":
    """Extract numeric line_code from route_id for joining with real-time data."""
    return routes_df.withColumn(
        "line_code",
        F.regexp_extract(F.col("route_id"), r"(\d+)", 1).cast(IntegerType()),
    )


def add_route_id_to_stop_times(stop_times_df: "DataFrame", trips_df: "DataFrame") -> "DataFrame":
    """Join stop_times with trips to get route_id for each stop_time row."""
    return stop_times_df.join(trips_df.select("trip_id", "route_id"), "trip_id", "inner")


def main() -> None:
    spark = SparkSession.builder.appName("load-gtfs-schedules").getOrCreate()

    with tempfile.TemporaryDirectory() as tmp_dir:
        gtfs_dir = download_gtfs(GTFS_DOWNLOAD_URL, tmp_dir)

        # Copy to DBFS for Spark access
        dbfs_path = "/tmp/gtfs_import"
        dbutils = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils()  # noqa: F841
        spark.sparkContext._jsc.hadoopConfiguration()

        # Load each GTFS file
        routes = load_gtfs_csv(spark, f"{gtfs_dir}/routes.txt", ROUTES_SCHEMA)
        routes = add_line_code_to_routes(routes)

        stops = load_gtfs_csv(spark, f"{gtfs_dir}/stops.txt", STOPS_SCHEMA)
        trips = load_gtfs_csv(spark, f"{gtfs_dir}/trips.txt", TRIPS_SCHEMA)

        stop_times = load_gtfs_csv(spark, f"{gtfs_dir}/stop_times.txt", STOP_TIMES_SCHEMA)
        stop_times = add_route_id_to_stop_times(stop_times, trips)

        shapes = load_gtfs_csv(spark, f"{gtfs_dir}/shapes.txt", SHAPES_SCHEMA)

    # Add ingestion metadata
    load_ts = F.current_timestamp()

    for df, table_name in [
        (routes, ROUTES_TABLE),
        (stops, STOPS_TABLE),
        (stop_times, STOP_TIMES_TABLE),
        (shapes, SHAPES_TABLE),
        (trips, TRIPS_TABLE),
    ]:
        (df.withColumn("_loaded_at", load_ts).write.format("delta").mode("overwrite").saveAsTable(table_name))

    print(
        f"GTFS load complete: {routes.count()} routes, {stops.count()} stops, "
        f"{stop_times.count()} stop_times, {shapes.count()} shapes, {trips.count()} trips"
    )


if __name__ == "__main__":
    main()
