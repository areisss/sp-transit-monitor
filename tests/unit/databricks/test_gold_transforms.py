"""Unit tests for Gold layer transforms — PySpark local mode."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.streaming.gold.headway_regularity import compute_headways
from databricks.streaming.gold.on_time_performance import classify_on_time
from databricks.streaming.gold.speed_congestion import (
    classify_congestion,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-transforms")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestClassifyOnTime:
    def test_on_time(self, spark):
        df = spark.range(1).withColumn("delay", F.lit(120))
        result = df.withColumn("status", classify_on_time(F.col("delay"))).collect()[0]["status"]
        assert result == "on_time"

    def test_early(self, spark):
        df = spark.range(1).withColumn("delay", F.lit(-120))
        result = df.withColumn("status", classify_on_time(F.col("delay"))).collect()[0]["status"]
        assert result == "early"

    def test_late(self, spark):
        df = spark.range(1).withColumn("delay", F.lit(600))
        result = df.withColumn("status", classify_on_time(F.col("delay"))).collect()[0]["status"]
        assert result == "late"

    def test_boundary_on_time(self, spark):
        # Exactly 5 minutes late = on_time
        df = spark.range(1).withColumn("delay", F.lit(300))
        result = df.withColumn("status", classify_on_time(F.col("delay"))).collect()[0]["status"]
        assert result == "on_time"

    def test_boundary_early(self, spark):
        # Exactly 1 minute early = on_time
        df = spark.range(1).withColumn("delay", F.lit(-60))
        result = df.withColumn("status", classify_on_time(F.col("delay"))).collect()[0]["status"]
        assert result == "on_time"


class TestClassifyCongestion:
    def test_free_flow(self, spark):
        df = spark.range(1).withColumn("speed", F.lit(25.0))
        result = df.withColumn("level", classify_congestion(F.col("speed"))).collect()[0]["level"]
        assert result == "free_flow"

    def test_slow(self, spark):
        df = spark.range(1).withColumn("speed", F.lit(15.0))
        result = df.withColumn("level", classify_congestion(F.col("speed"))).collect()[0]["level"]
        assert result == "slow"

    def test_congested(self, spark):
        df = spark.range(1).withColumn("speed", F.lit(7.0))
        result = df.withColumn("level", classify_congestion(F.col("speed"))).collect()[0]["level"]
        assert result == "congested"

    def test_gridlock(self, spark):
        df = spark.range(1).withColumn("speed", F.lit(3.0))
        result = df.withColumn("level", classify_congestion(F.col("speed"))).collect()[0]["level"]
        assert result == "gridlock"


class TestComputeHeadways:
    def test_computes_headway_between_arrivals(self, spark):
        schema = StructType(
            [
                StructField("vehicle_id", StringType()),
                StructField("stop_id", StringType()),
                StructField("line_code", IntegerType()),
                StructField("route_id", StringType()),
                StructField("arrival_time", TimestampType()),
                StructField("latitude", DoubleType()),
                StructField("longitude", DoubleType()),
                StructField("distance_to_stop_m", DoubleType()),
                StructField("_event_date", StringType()),
            ]
        )

        from datetime import datetime

        data = [
            ("V001", "S1", 100, "R1", datetime(2026, 3, 14, 8, 0, 0), -23.55, -46.63, 10.0, "2026-03-14"),
            ("V002", "S1", 100, "R1", datetime(2026, 3, 14, 8, 10, 0), -23.55, -46.63, 15.0, "2026-03-14"),
            ("V003", "S1", 100, "R1", datetime(2026, 3, 14, 8, 20, 0), -23.55, -46.63, 12.0, "2026-03-14"),
        ]

        df = spark.createDataFrame(data, schema=schema)
        result = compute_headways(df)

        rows = result.orderBy("arrival_time").collect()
        assert len(rows) == 2
        assert rows[0]["headway_seconds"] == 600  # 10 minutes
        assert rows[1]["headway_seconds"] == 600
