from datetime import datetime

from pydantic import BaseModel, Field


class VehiclePosition(BaseModel):
    """Normalized GPS position for a single vehicle."""

    vehicle_id: str = Field(description="Unique vehicle prefix (p)")
    line_code: int = Field(description="Internal line code (cl)")
    line_number: str = Field(description="Public line number (c)")
    line_direction: int = Field(description="Direction: 1=main→secondary, 2=secondary→main (sl)")
    latitude: float = Field(description="GPS latitude (py)")
    longitude: float = Field(description="GPS longitude (px)")
    is_accessible: bool = Field(description="Wheelchair accessible (a)")
    event_timestamp: str = Field(description="ISO 8601 timestamp from GPS device (ta)")
    ingestion_timestamp: str = Field(description="ISO 8601 timestamp when record was produced")

    def to_kinesis_record(self) -> dict:
        return {
            "Data": self.model_dump_json().encode("utf-8"),
            "PartitionKey": self.vehicle_id,
        }


class ApiVehicle(BaseModel):
    """Raw vehicle from SPTrans /Posicao response."""

    p: int | str  # vehicle prefix (vehicle_id) — API returns int
    a: bool  # accessible
    ta: str  # event timestamp
    py: float  # latitude
    px: float  # longitude


class ApiLine(BaseModel):
    """Raw line grouping from SPTrans /Posicao response."""

    c: str  # line number (public-facing)
    cl: int  # line code (internal)
    sl: int  # direction
    lt0: str  # origin terminal
    lt1: str  # destination terminal
    vs: list[ApiVehicle] = Field(default_factory=list)  # vehicles


class ApiPositionResponse(BaseModel):
    """Top-level SPTrans /Posicao response."""

    hr: str  # reference time
    l: list[ApiLine] = Field(default_factory=list)  # lines


def normalize_positions(response: ApiPositionResponse, ingestion_time: datetime) -> list[VehiclePosition]:
    """Flatten the nested API response into a list of VehiclePosition records."""
    ingestion_ts = ingestion_time.isoformat()
    positions: list[VehiclePosition] = []

    for line in response.l:
        for vehicle in line.vs:
            positions.append(
                VehiclePosition(
                    vehicle_id=str(vehicle.p),
                    line_code=line.cl,
                    line_number=line.c,
                    line_direction=line.sl,
                    latitude=vehicle.py,
                    longitude=vehicle.px,
                    is_accessible=vehicle.a,
                    event_timestamp=vehicle.ta,
                    ingestion_timestamp=ingestion_ts,
                )
            )

    return positions
