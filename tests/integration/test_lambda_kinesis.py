"""Integration tests: Lambda → Kinesis → S3 pipeline using moto."""

import json
import os

import boto3
import pytest
from moto import mock_aws

from sptrans_producer.handler import put_records_batch
from sptrans_producer.models import VehiclePosition

pytestmark = pytest.mark.integration


@pytest.fixture
def aws_config():
    """Set up mock AWS environment."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield
    for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SECURITY_TOKEN", "AWS_SESSION_TOKEN"]:
        os.environ.pop(key, None)


@pytest.fixture
def sample_positions():
    return [
        VehiclePosition(
            vehicle_id=f"{10000 + i}",
            line_code=33000,
            line_number="8001-10",
            line_direction=1,
            latitude=-23.55 + i * 0.001,
            longitude=-46.63 + i * 0.001,
            is_accessible=i % 2 == 0,
            event_timestamp="2026-03-14T15:00:00",
            ingestion_timestamp="2026-03-14T15:30:00+00:00",
        )
        for i in range(100)
    ]


@mock_aws
class TestLambdaToKinesis:
    def test_put_records_to_kinesis(self, aws_config, sample_positions):
        kinesis = boto3.client("kinesis", region_name="us-east-1")
        stream_name = "test-transit-stream"

        kinesis.create_stream(StreamName=stream_name, ShardCount=1)

        # Wait for stream to become active
        waiter = kinesis.get_waiter("stream_exists")
        waiter.wait(StreamName=stream_name)

        written = put_records_batch(kinesis, stream_name, sample_positions, batch_size=50)

        assert written == 100

    def test_read_back_records(self, aws_config, sample_positions):
        kinesis = boto3.client("kinesis", region_name="us-east-1")
        stream_name = "test-transit-stream-read"

        kinesis.create_stream(StreamName=stream_name, ShardCount=1)
        waiter = kinesis.get_waiter("stream_exists")
        waiter.wait(StreamName=stream_name)

        put_records_batch(kinesis, stream_name, sample_positions[:5], batch_size=500)

        # Read back from stream
        desc = kinesis.describe_stream(StreamName=stream_name)
        shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]

        iterator = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        response = kinesis.get_records(ShardIterator=iterator, Limit=10)
        records = response["Records"]

        assert len(records) == 5
        first_record = json.loads(records[0]["Data"])
        assert first_record["vehicle_id"] == "10000"
        assert first_record["line_code"] == 33000

    def test_empty_batch(self, aws_config):
        kinesis = boto3.client("kinesis", region_name="us-east-1")
        written = put_records_batch(kinesis, "any-stream", [], batch_size=500)
        assert written == 0
