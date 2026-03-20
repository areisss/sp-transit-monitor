"""Unit tests for geo_utils module."""

import math

import pytest

from databricks.utils.geo_utils import (
    EARTH_RADIUS_KM,
    SP_LAT_MAX,
    SP_LAT_MIN,
    SP_LNG_MAX,
    SP_LNG_MIN,
    haversine_distance_km,
    latlng_to_h3,
)


class TestHaversineDistanceKm:
    def test_same_point_returns_zero(self):
        dist = haversine_distance_km(-23.55, -46.63, -23.55, -46.63)
        assert dist == pytest.approx(0.0, abs=1e-10)

    def test_known_distance(self):
        # Paulista Ave to Se Cathedral ~2.5km
        dist = haversine_distance_km(-23.5613, -46.6559, -23.5505, -46.6340)
        assert 1.5 < dist < 3.5

    def test_symmetry(self):
        d1 = haversine_distance_km(-23.55, -46.63, -23.56, -46.64)
        d2 = haversine_distance_km(-23.56, -46.64, -23.55, -46.63)
        assert d1 == pytest.approx(d2, rel=1e-10)

    def test_large_distance(self):
        # SP to Rio ~360km
        dist = haversine_distance_km(-23.55, -46.63, -22.91, -43.17)
        assert 300 < dist < 420


class TestLatlngToH3:
    def test_returns_integer(self):
        result = latlng_to_h3(-23.55, -46.63, resolution=9)
        assert isinstance(result, str)

    def test_different_locations_different_cells(self):
        h1 = latlng_to_h3(-23.55, -46.63, resolution=9)
        h2 = latlng_to_h3(-23.60, -46.70, resolution=9)
        assert h1 != h2

    def test_nearby_points_same_cell(self):
        # Two points ~10m apart should be in the same res-9 cell (~174m edge)
        h1 = latlng_to_h3(-23.550000, -46.630000, resolution=9)
        h2 = latlng_to_h3(-23.550001, -46.630001, resolution=9)
        assert h1 == h2


class TestSpBounds:
    def test_bounds_cover_sp(self):
        # Central SP
        assert SP_LAT_MIN <= -23.55 <= SP_LAT_MAX
        assert SP_LNG_MIN <= -46.63 <= SP_LNG_MAX

    def test_bounds_exclude_rio(self):
        rio_lat, rio_lng = -22.91, -43.17
        in_bounds = SP_LAT_MIN <= rio_lat <= SP_LAT_MAX and SP_LNG_MIN <= rio_lng <= SP_LNG_MAX
        assert not in_bounds
