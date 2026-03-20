import httpx
import pytest

from sptrans_producer.config import Config
from sptrans_producer.sptrans_client import (
    clear_session_cache,
    fetch_vehicle_positions,
)

SAMPLE_POSITION_RESPONSE = {
    "hr": "15:30",
    "l": [
        {
            "c": "8001-10",
            "cl": 33000,
            "sl": 1,
            "lt0": "Terminal A",
            "lt1": "Terminal B",
            "vs": [
                {"p": "10001", "a": True, "ta": "2026-03-14T15:00:00", "py": -23.55, "px": -46.63},
                {"p": "10002", "a": False, "ta": "2026-03-14T15:00:05", "py": -23.56, "px": -46.64},
            ],
        }
    ],
}

TEST_CONFIG = Config(
    sptrans_api_base_url="https://api.test.sptrans.com.br/v2.1",
    sptrans_api_token="test-token-123",
    kinesis_stream_name="test-stream",
    kinesis_batch_size=500,
    aws_region="us-east-1",
)


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_session_cache()
    yield
    clear_session_cache()


class TestFetchVehiclePositions:
    def test_authenticates_and_fetches(self):
        transport = httpx.MockTransport(self._mock_handler_success)
        client = httpx.Client(transport=transport)

        response = fetch_vehicle_positions(client, TEST_CONFIG)

        assert len(response.l) == 1
        assert len(response.l[0].vs) == 2
        assert response.l[0].vs[0].p == "10001"

    def test_reauthenticates_on_401(self):
        call_count = {"auth": 0, "position": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if "Autenticar" in str(request.url):
                call_count["auth"] += 1
                return httpx.Response(200, json=True)

            call_count["position"] += 1
            if call_count["position"] == 1:
                return httpx.Response(401)
            return httpx.Response(200, json=SAMPLE_POSITION_RESPONSE)

        transport = httpx.MockTransport(handler)
        client = httpx.Client(transport=transport)

        response = fetch_vehicle_positions(client, TEST_CONFIG)

        assert call_count["auth"] == 2  # initial + re-auth
        assert call_count["position"] == 2
        assert len(response.l) == 1

    def test_raises_on_api_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if "Autenticar" in str(request.url):
                return httpx.Response(200, json=True)
            return httpx.Response(500)

        transport = httpx.MockTransport(handler)
        client = httpx.Client(transport=transport)

        with pytest.raises(httpx.HTTPStatusError):
            fetch_vehicle_positions(client, TEST_CONFIG)

    def test_raises_on_auth_failure(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if "Autenticar" in str(request.url):
                return httpx.Response(200, json=False)
            return httpx.Response(200, json=SAMPLE_POSITION_RESPONSE)

        transport = httpx.MockTransport(handler)
        client = httpx.Client(transport=transport)

        with pytest.raises(RuntimeError, match="authentication failed"):
            fetch_vehicle_positions(client, TEST_CONFIG)

    @staticmethod
    def _mock_handler_success(request: httpx.Request) -> httpx.Response:
        if "Autenticar" in str(request.url):
            return httpx.Response(200, json=True)
        if "Posicao" in str(request.url):
            return httpx.Response(200, json=SAMPLE_POSITION_RESPONSE)
        return httpx.Response(404)
