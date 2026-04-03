"""Schema definitions for the Silver layer."""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VEHICLE_POSITIONS_ENRICHED_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), nullable=False),
        StructField("line_code", IntegerType(), nullable=False),
        StructField("line_number", StringType(), nullable=False),
        StructField("line_direction", IntegerType(), nullable=False),
        StructField("latitude", DoubleType(), nullable=False),
        StructField("longitude", DoubleType(), nullable=False),
        StructField("is_accessible", BooleanType(), nullable=True),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("ingestion_ts", TimestampType(), nullable=False),
        StructField("route_short_name", StringType(), nullable=True),
        StructField("route_long_name", StringType(), nullable=True),
        StructField("route_type", IntegerType(), nullable=True),
        StructField("h3_index", LongType(), nullable=False),
        StructField("_event_date", StringType(), nullable=False),
    ]
)

STOP_ARRIVALS_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), nullable=False),
        StructField("stop_id", StringType(), nullable=False),
        StructField("route_id", StringType(), nullable=True),
        StructField("line_code", IntegerType(), nullable=False),
        StructField("arrival_time", TimestampType(), nullable=False),
        StructField("latitude", DoubleType(), nullable=False),
        StructField("longitude", DoubleType(), nullable=False),
        StructField("distance_to_stop_m", DoubleType(), nullable=False),
        StructField("_event_date", StringType(), nullable=False),
    ]
)
