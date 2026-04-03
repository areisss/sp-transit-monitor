"""Unit tests for Silver layer transforms — PySpark local mode."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from databricks.streaming.silver.clean_enrich_gps import (
    filter_sp_bounds,
    parse_timestamps,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-transforms")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


RAW_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType()),
        StructField("line_code", IntegerType()),
        StructField("line_number", StringType()),
        StructField("line_direction", IntegerType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("is_accessible", BooleanType()),
        StructField("event_timestamp", StringType()),
        StructField("ingestion_timestamp", StringType()),
    ]
)


def _make_raw_df(spark, rows):
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)


class TestParseTimestamps:
    def test_converts_string_to_timestamp(self, spark):
        df = _make_raw_df(
            spark,
            [
                ("V001", 33000, "8001-10", 1, -23.55, -46.63, True, "2026-03-14T15:00:00", "2026-03-14T15:30:00"),
            ],
        )

        result = parse_timestamps(df)

        assert "event_ts" in result.columns
        assert "ingestion_ts" in result.columns
        assert "event_timestamp" not in result.columns
        assert "ingestion_timestamp" not in result.columns

        row = result.collect()[0]
        assert row["event_ts"] is not None
        assert row["ingestion_ts"] is not None


class TestFilterSpBounds:
    def test_keeps_valid_sp_coordinates(self, spark):
        df = spark.createDataFrame(
            [(-23.55, -46.63), (-23.80, -46.50)],
            ["latitude", "longitude"],
        )

        result = filter_sp_bounds(df)

        assert result.count() == 2

    def test_removes_out_of_bounds(self, spark):
        df = spark.createDataFrame(
            [
                (-23.55, -46.63),  # valid SP
                (-22.91, -43.17),  # Rio de Janeiro
                (-25.43, -49.27),  # Curitiba
                (0.0, 0.0),  # null island
            ],
            ["latitude", "longitude"],
        )

        result = filter_sp_bounds(df)

        assert result.count() == 1
        row = result.collect()[0]
        assert row["latitude"] == pytest.approx(-23.55)
