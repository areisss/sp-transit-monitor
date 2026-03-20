from datetime import datetime, timezone

from sptrans_producer.models import (
    ApiLine,
    ApiPositionResponse,
    ApiVehicle,
    VehiclePosition,
    normalize_positions,
)


def _make_api_response(num_lines: int = 2, vehicles_per_line: int = 3) -> ApiPositionResponse:
    lines = []
    for i in range(num_lines):
        vehicles = [
            ApiVehicle(
                p=f"{10000 + i * 100 + j}",
                a=j % 2 == 0,
                ta=f"2026-03-14T15:{j:02d}:00",
                py=-23.55 + j * 0.001,
                px=-46.63 + j * 0.001,
            )
            for j in range(vehicles_per_line)
        ]
        lines.append(
            ApiLine(
                c=f"800{i + 1}-10",
                cl=33000 + i,
                sl=i % 2 + 1,
                lt0="Terminal A",
                lt1="Terminal B",
                vs=vehicles,
            )
        )
    return ApiPositionResponse(hr="15:30", l=lines)


class TestNormalizePositions:
    def test_flattens_nested_response(self):
        response = _make_api_response(num_lines=2, vehicles_per_line=3)
        ingestion_time = datetime(2026, 3, 14, 15, 30, 0, tzinfo=timezone.utc)

        positions = normalize_positions(response, ingestion_time)

        assert len(positions) == 6

    def test_maps_fields_correctly(self):
        response = _make_api_response(num_lines=1, vehicles_per_line=1)
        ingestion_time = datetime(2026, 3, 14, 15, 30, 0, tzinfo=timezone.utc)

        positions = normalize_positions(response, ingestion_time)
        pos = positions[0]

        assert pos.vehicle_id == "10000"
        assert pos.line_code == 33000
        assert pos.line_number == "8001-10"
        assert pos.line_direction == 1
        assert pos.latitude == -23.55
        assert pos.longitude == -46.63
        assert pos.is_accessible is True
        assert pos.event_timestamp == "2026-03-14T15:00:00"
        assert pos.ingestion_timestamp == ingestion_time.isoformat()

    def test_empty_response(self):
        response = ApiPositionResponse(hr="15:30", l=[])
        ingestion_time = datetime(2026, 3, 14, 15, 30, 0, tzinfo=timezone.utc)

        positions = normalize_positions(response, ingestion_time)

        assert positions == []

    def test_line_with_no_vehicles(self):
        response = ApiPositionResponse(
            hr="15:30",
            l=[ApiLine(c="8001-10", cl=33000, sl=1, lt0="A", lt1="B", vs=[])],
        )
        ingestion_time = datetime(2026, 3, 14, 15, 30, 0, tzinfo=timezone.utc)

        positions = normalize_positions(response, ingestion_time)

        assert positions == []


class TestVehiclePosition:
    def test_to_kinesis_record(self):
        pos = VehiclePosition(
            vehicle_id="10001",
            line_code=33000,
            line_number="8001-10",
            line_direction=1,
            latitude=-23.55,
            longitude=-46.63,
            is_accessible=True,
            event_timestamp="2026-03-14T15:00:00",
            ingestion_timestamp="2026-03-14T15:30:00+00:00",
        )

        record = pos.to_kinesis_record()

        assert record["PartitionKey"] == "10001"
        assert isinstance(record["Data"], bytes)
        assert b"10001" in record["Data"]
