"""Unit tests for time_utils — requires PySpark local mode."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks.utils.time_utils import (
    classify_time_period,
    is_weekday,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-time-utils")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestClassifyTimePeriod:
    def test_morning_peak(self, spark):
        df = spark.range(1).withColumn("hour", F.lit(7))
        result = df.withColumn("period", classify_time_period(F.col("hour"))).collect()[0]["period"]
        assert result == "morning_peak"

    def test_off_peak(self, spark):
        df = spark.range(1).withColumn("hour", F.lit(12))
        result = df.withColumn("period", classify_time_period(F.col("hour"))).collect()[0]["period"]
        assert result == "off_peak"

    def test_evening_peak(self, spark):
        df = spark.range(1).withColumn("hour", F.lit(18))
        result = df.withColumn("period", classify_time_period(F.col("hour"))).collect()[0]["period"]
        assert result == "evening_peak"

    def test_night(self, spark):
        df = spark.range(1).withColumn("hour", F.lit(23))
        result = df.withColumn("period", classify_time_period(F.col("hour"))).collect()[0]["period"]
        assert result == "night"

    def test_early_morning_is_night(self, spark):
        df = spark.range(1).withColumn("hour", F.lit(4))
        result = df.withColumn("period", classify_time_period(F.col("hour"))).collect()[0]["period"]
        assert result == "night"


class TestIsWeekday:
    def test_monday_is_weekday(self, spark):
        # 2026-03-16 is a Monday
        df = spark.sql("SELECT TIMESTAMP('2026-03-16 12:00:00') AS ts")
        result = df.withColumn("wd", is_weekday(F.col("ts"))).collect()[0]["wd"]
        assert result is True

    def test_sunday_is_not_weekday(self, spark):
        # 2026-03-15 is a Sunday
        df = spark.sql("SELECT TIMESTAMP('2026-03-15 12:00:00') AS ts")
        result = df.withColumn("wd", is_weekday(F.col("ts"))).collect()[0]["wd"]
        assert result is False
