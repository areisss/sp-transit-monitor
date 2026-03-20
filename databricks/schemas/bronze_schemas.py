"""Schema definitions for the Bronze layer."""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

SPTRANS_GPS_RAW_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), nullable=False),
    StructField("line_code", IntegerType(), nullable=False),
    StructField("line_number", StringType(), nullable=False),
    StructField("line_direction", IntegerType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("is_accessible", BooleanType(), nullable=True),
    StructField("event_timestamp", StringType(), nullable=False),
    StructField("ingestion_timestamp", StringType(), nullable=False),
])
