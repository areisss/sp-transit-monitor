"""Time and timezone utility functions for SP Transit pipeline."""

from pyspark.sql import Column
from pyspark.sql import functions as F

SP_TIMEZONE = "America/Sao_Paulo"


def to_sp_local_time(ts_col: Column) -> Column:
    """Convert a UTC timestamp column to Sao Paulo local time."""
    return F.from_utc_timestamp(ts_col, SP_TIMEZONE)


def extract_hour_of_day(ts_col: Column) -> Column:
    """Extract hour of day (0-23) from a timestamp, in SP timezone."""
    return F.hour(to_sp_local_time(ts_col))


def extract_day_of_week(ts_col: Column) -> Column:
    """Extract day of week (1=Sunday, 7=Saturday) from a timestamp, in SP timezone."""
    return F.dayofweek(to_sp_local_time(ts_col))


def is_weekday(ts_col: Column) -> Column:
    """Return True if the timestamp falls on a weekday (Mon-Fri) in SP timezone."""
    dow = extract_day_of_week(ts_col)
    return (dow >= 2) & (dow <= 6)


def classify_time_period(hour_col: Column) -> Column:
    """Classify hour into time periods for analysis."""
    return (
        F.when((hour_col >= 6) & (hour_col < 9), F.lit("morning_peak"))
        .when((hour_col >= 9) & (hour_col < 16), F.lit("off_peak"))
        .when((hour_col >= 16) & (hour_col < 20), F.lit("evening_peak"))
        .otherwise(F.lit("night"))
    )
