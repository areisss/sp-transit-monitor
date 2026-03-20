import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from sptrans_producer.config import Config
from sptrans_producer.handler import lambda_handler, put_records_batch
from sptrans_producer.models import ApiLine, ApiPositionResponse, ApiVehicle, VehiclePosition

TEST_CONFIG = Config(
    sptrans_api_base_url="https://api.test.sptrans.com.br/v2.1",
    sptrans_api_token="test-token",
    kinesis_stream_name="test-stream",
    kinesis_batch_size=500,
    aws_region="us-east-1",
)

SAMPLE_API_RESPONSE = ApiPositionResponse(
    hr="15:30",
    l=[
        ApiLine(
            c="8001-10",
            cl=33000,
            sl=1,
            lt0="Terminal A",
            lt1="Terminal B",
            vs=[
                ApiVehicle(p="10001", a=True, ta="2026-03-14T15:00:00", py=-23.55, px=-46.63),
                ApiVehicle(p="10002", a=False, ta="2026-03-14T15:00:05", py=-23.56, px=-46.64),
            ],
        )
    ],
)


class TestPutRecordsBatch:
    def test_writes_all_records_successfully(self):
        kinesis = MagicMock()
        kinesis.put_records.return_value = {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "1"}]}

        records = [
            VehiclePosition(
                vehicle_id=f"{i}",
                line_code=33000,
                line_number="8001-10",
                line_direction=1,
                latitude=-23.55,
                longitude=-46.63,
                is_accessible=True,
                event_timestamp="2026-03-14T15:00:00",
                ingestion_timestamp="2026-03-14T15:30:00+00:00",
            )
            for i in range(10)
        ]

        written = put_records_batch(kinesis, "test-stream", records, batch_size=5)

        assert written == 10
        assert kinesis.put_records.call_count == 2

    def test_handles_partial_failures_with_retry(self):
        kinesis = MagicMock()
        # First call: 1 out of 2 fails
        kinesis.put_records.side_effect = [
            {
                "FailedRecordCount": 1,
                "Records": [
                    {"SequenceNumber": "1"},
                    {"ErrorCode": "ProvisionedThroughputExceededException"},
                ],
            },
            # Retry: succeeds
            {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "2"}]},
        ]

        records = [
            VehiclePosition(
                vehicle_id=f"{i}",
                line_code=33000,
                line_number="8001-10",
                line_direction=1,
                latitude=-23.55,
                longitude=-46.63,
                is_accessible=True,
                event_timestamp="2026-03-14T15:00:00",
                ingestion_timestamp="2026-03-14T15:30:00+00:00",
            )
            for i in range(2)
        ]

        written = put_records_batch(kinesis, "test-stream", records, batch_size=500)

        assert written == 2

    def test_empty_records(self):
        kinesis = MagicMock()

        written = put_records_batch(kinesis, "test-stream", [], batch_size=500)

        assert written == 0
        kinesis.put_records.assert_not_called()


class TestLambdaHandler:
    @patch("sptrans_producer.handler._get_http_client")
    @patch("sptrans_producer.handler._get_kinesis_client")
    @patch("sptrans_producer.handler._get_config")
    @patch("sptrans_producer.handler.fetch_vehicle_positions")
    @patch("sptrans_producer.handler.put_records_batch")
    def test_successful_invocation(self, mock_put, mock_fetch, mock_config, mock_kinesis, mock_http):
        mock_config.return_value = TEST_CONFIG
        mock_kinesis.return_value = MagicMock()
        mock_http.return_value = MagicMock()
        mock_fetch.return_value = SAMPLE_API_RESPONSE
        mock_put.return_value = 2

        result = lambda_handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["records_written"] == 2
        assert body["records_total"] == 2
        assert body["lines_count"] == 1

    @patch("sptrans_producer.handler._get_http_client")
    @patch("sptrans_producer.handler._get_kinesis_client")
    @patch("sptrans_producer.handler._get_config")
    @patch("sptrans_producer.handler.fetch_vehicle_positions")
    def test_api_failure_returns_500(self, mock_fetch, mock_config, mock_kinesis, mock_http):
        mock_config.return_value = TEST_CONFIG
        mock_kinesis.return_value = MagicMock()
        mock_http.return_value = MagicMock()
        mock_fetch.side_effect = RuntimeError("API down")

        result = lambda_handler({}, None)

        assert result["statusCode"] == 500

    @patch("sptrans_producer.handler._get_http_client")
    @patch("sptrans_producer.handler._get_kinesis_client")
    @patch("sptrans_producer.handler._get_config")
    @patch("sptrans_producer.handler.fetch_vehicle_positions")
    def test_empty_response(self, mock_fetch, mock_config, mock_kinesis, mock_http):
        mock_config.return_value = TEST_CONFIG
        mock_kinesis.return_value = MagicMock()
        mock_http.return_value = MagicMock()
        mock_fetch.return_value = ApiPositionResponse(hr="15:30", l=[])

        result = lambda_handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["records_written"] == 0
