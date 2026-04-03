import gzip
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

import boto3
import httpx

from sptrans_producer.config import Config
from sptrans_producer.models import VehiclePosition, normalize_positions
from sptrans_producer.sptrans_client import fetch_vehicle_positions

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Reused across warm invocations
_kinesis_client = None
_s3_client = None
_http_client = None
_config = None


def _get_config() -> Config:
    global _config
    if _config is None:
        _config = Config.from_env()
    return _config


def _get_kinesis_client(config: Config) -> Any:
    global _kinesis_client
    if _kinesis_client is None:
        _kinesis_client = boto3.client("kinesis", region_name=config.aws_region)
    return _kinesis_client


def _get_s3_client(config: Config) -> Any:
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=config.aws_region)
    return _s3_client


def _get_http_client() -> httpx.Client:
    global _http_client
    if _http_client is None:
        _http_client = httpx.Client(timeout=30.0)
    return _http_client


def put_records_s3(
    s3_client: Any,
    bucket: str,
    prefix: str,
    records: list[VehiclePosition],
    ingestion_time: datetime,
) -> int:
    """Write records to S3 as gzipped JSONL, matching the Firehose output format."""
    jsonl = "\n".join(r.model_dump_json() for r in records).encode("utf-8")
    compressed = gzip.compress(jsonl)

    ts = ingestion_time.strftime("%Y-%m-%dT%H-%M-%S")
    key = (
        f"{prefix}/"
        f"year={ingestion_time.strftime('%Y')}/"
        f"month={ingestion_time.strftime('%m')}/"
        f"day={ingestion_time.strftime('%d')}/"
        f"hour={ingestion_time.strftime('%H')}/"
        f"{ts}-{len(records)}.json.gz"
    )

    s3_client.put_object(Bucket=bucket, Key=key, Body=compressed, ContentEncoding="gzip")
    return len(records)


def put_records_kinesis(
    kinesis_client: Any,
    stream_name: str,
    records: list[VehiclePosition],
    batch_size: int = 500,
    max_retries: int = 3,
) -> int:
    """Write records to Kinesis in batches, retrying partial failures."""
    total_written = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        kinesis_records = [r.to_kinesis_record() for r in batch]

        for attempt in range(max_retries):
            response = kinesis_client.put_records(
                StreamName=stream_name,
                Records=kinesis_records,
            )

            failed_count = response.get("FailedRecordCount", 0)
            if failed_count == 0:
                total_written += len(kinesis_records)
                break

            retry_records = []
            for record, result in zip(kinesis_records, response["Records"], strict=False):
                if "ErrorCode" in result:
                    retry_records.append(record)

            logger.warning(
                "Kinesis batch %d: %d/%d failed (attempt %d/%d)",
                i // batch_size,
                failed_count,
                len(kinesis_records),
                attempt + 1,
                max_retries,
            )

            total_written += len(kinesis_records) - len(retry_records)
            kinesis_records = retry_records

            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
        else:
            logger.error(
                "Kinesis batch %d: %d records failed after %d retries",
                i // batch_size,
                len(kinesis_records),
                max_retries,
            )

    return total_written


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda entry point: poll SPTrans API and write to S3 or Kinesis."""
    config = _get_config()
    http_client = _get_http_client()

    ingestion_time = datetime.now(UTC)

    try:
        api_response = fetch_vehicle_positions(http_client, config)
    except Exception:
        logger.exception("Failed to fetch vehicle positions from SPTrans API")
        return {"statusCode": 500, "body": json.dumps({"error": "API fetch failed"})}

    positions = normalize_positions(api_response, ingestion_time)
    logger.info("Fetched %d vehicle positions across %d lines", len(positions), len(api_response.l))

    if not positions:
        logger.warning("No positions returned from API")
        return {"statusCode": 200, "body": json.dumps({"records_written": 0})}

    if config.output_mode == "kinesis":
        kinesis = _get_kinesis_client(config)
        records_written = put_records_kinesis(kinesis, config.kinesis_stream_name, positions, config.kinesis_batch_size)
        logger.info("Successfully wrote %d/%d records to Kinesis", records_written, len(positions))
    else:
        s3 = _get_s3_client(config)
        records_written = put_records_s3(s3, config.s3_raw_bucket, config.s3_prefix, positions, ingestion_time)
        logger.info("Successfully wrote %d records to S3", records_written)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "records_written": records_written,
                "records_total": len(positions),
                "lines_count": len(api_response.l),
                "ingestion_time": ingestion_time.isoformat(),
                "output_mode": config.output_mode,
            }
        ),
    }
