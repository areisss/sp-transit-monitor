"""Geospatial utility functions for H3 indexing and distance calculations."""

import math

import h3
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

# Earth radius in kilometers
EARTH_RADIUS_KM = 6371.0

# Sao Paulo metropolitan area bounding box
SP_LAT_MIN = -24.1
SP_LAT_MAX = -23.3
SP_LNG_MIN = -47.0
SP_LNG_MAX = -46.2

# H3 resolution 9: ~174m edge length, good for stop detection
H3_RESOLUTION = 9

# Maximum plausible speed for a bus in km/h
MAX_BUS_SPEED_KMH = 150.0

# Stop arrival radius in meters
STOP_ARRIVAL_RADIUS_M = 50.0


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance between two points in kilometers."""
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return EARTH_RADIUS_KM * c


def latlng_to_h3(lat: float, lng: float, resolution: int = H3_RESOLUTION) -> int:
    """Convert lat/lng to H3 cell index as integer."""
    return h3.latlng_to_cell(lat, lng, resolution)


@F.udf(LongType())
def h3_index_udf(lat: float, lng: float) -> int | None:
    """Spark UDF to compute H3 index from lat/lng coordinates."""
    if lat is None or lng is None:
        return None
    return h3.latlng_to_cell(lat, lng, H3_RESOLUTION)


@F.udf(DoubleType())
def haversine_udf(lat1: float, lon1: float, lat2: float, lon2: float) -> float | None:
    """Spark UDF for Haversine distance in meters."""
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return None
    return haversine_distance_km(lat1, lon1, lat2, lon2) * 1000


def is_within_sp_bounds(lat_col: Column, lng_col: Column) -> Column:
    """Return a boolean Column filtering to Sao Paulo metro area bounds."""
    return (lat_col >= SP_LAT_MIN) & (lat_col <= SP_LAT_MAX) & (lng_col >= SP_LNG_MIN) & (lng_col <= SP_LNG_MAX)
