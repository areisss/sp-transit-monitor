import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from sptrans_producer.config import Config
from sptrans_producer.handler import lambda_handler, put_records_kinesis, put_records_s3
from sptrans_producer.models import ApiLine, ApiPositionResponse, ApiVehicle, VehiclePosition

TEST_CONFIG_KINESIS = Config(
    sptrans_api_base_url="https://api.test.sptrans.com.br/v2.1",
    sptrans_api_token="test-token",
    output_mode="kinesis",
    kinesis_stream_name="test-stream",
    kinesis_batch_size=500,
    s3_raw_bucket="",
    s3_prefix="",
    aws_region="us-east-1",
)

TEST_CONFIG_S3 = Config(
    sptrans_api_base_url="https://api.test.sptrans.com.br/v2.1",
    sptrans_api_token="test-token",
    output_mode="s3",
    kinesis_stream_name="",
    kinesis_batch_size=500,
    s3_raw_bucket="test-bucket",
    s3_prefix="sptrans-gps",
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


class TestPutRecordsKinesis:
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

        written = put_records_kinesis(kinesis, "test-stream", records, batch_size=5)

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

        written = put_records_kinesis(kinesis, "test-stream", records, batch_size=500)

        assert written == 2

    def test_empty_records(self):
        kinesis = MagicMock()

        written = put_records_kinesis(kinesis, "test-stream", [], batch_size=500)

        assert written == 0
        kinesis.put_records.assert_not_called()


class TestPutRecordsS3:
    def test_writes_gzipped_jsonl_to_s3(self):
        s3 = MagicMock()
        ingestion_time = datetime(2026, 3, 14, 15, 30, 0, tzinfo=UTC)

        records = [
            VehiclePosition(
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
        ]

        written = put_records_s3(s3, "test-bucket", "sptrans-gps", records, ingestion_time)

        assert written == 1
        s3.put_object.assert_called_once()
        call_kwargs = s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert "year=2026/month=03/day=14/hour=15" in call_kwargs["Key"]
        assert call_kwargs["ContentEncoding"] == "gzip"


class TestLambdaHandler:
    @patch("sptrans_producer.handler._get_http_client")
    @patch("sptrans_producer.handler._get_kinesis_client")
    @patch("sptrans_producer.handler._get_config")
    @patch("sptrans_producer.handler.fetch_vehicle_positions")
    @patch("sptrans_producer.handler.put_records_kinesis")
    def test_kinesis_mode(self, mock_put, mock_fetch, mock_config, mock_kinesis, mock_http):
        mock_config.return_value = TEST_CONFIG_KINESIS
        mock_kinesis.return_value = MagicMock()
        mock_http.return_value = MagicMock()
        mock_fetch.return_value = SAMPLE_API_RESPONSE
        mock_put.return_value = 2

        result = lambda_handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["records_written"] == 2
        assert body["output_mode"] == "kinesis"

    @patch("sptrans_producer.handler._get_http_client")
    @patch("sptrans_producer.handler._get_s3_client")
    @patch("sptrans_producer.handler._get_config")
    @patch("sptrans_producer.handler.fetch_vehicle_positions")
    @patch("sptrans_producer.handler.put_records_s3")
    def test_s3_mode(self, mock_put, mock_fetch, mock_config, mock_s3, mock_http):
        mock_config.return_value = TEST_CONFIG_S3
        mock_s3.return_value = MagicMock()
        mock_http.return_value = MagicMock()
        mock_fetch.return_value = SAMPLE_API_RESPONSE
        mock_put.return_value = 2

        result = lambda_handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["records_written"] == 2
        assert body["output_mode"] == "s3"

    @patch("sptrans_producer.handler._get_http_client")
    @patch("sptrans_producer.handler._get_config")
    @patch("sptrans_producer.handler.fetch_vehicle_positions")
    def test_api_failure_returns_500(self, mock_fetch, mock_config, mock_http):
        mock_config.return_value = TEST_CONFIG_S3
        mock_http.return_value = MagicMock()
        mock_fetch.side_effect = RuntimeError("API down")

        result = lambda_handler({}, None)

        assert result["statusCode"] == 500

    @patch("sptrans_producer.handler._get_http_client")
    @patch("sptrans_producer.handler._get_config")
    @patch("sptrans_producer.handler.fetch_vehicle_positions")
    def test_empty_response(self, mock_fetch, mock_config, mock_http):
        mock_config.return_value = TEST_CONFIG_S3
        mock_http.return_value = MagicMock()
        mock_fetch.return_value = ApiPositionResponse(hr="15:30", l=[])

        result = lambda_handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["records_written"] == 0
