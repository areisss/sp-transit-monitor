import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    sptrans_api_base_url: str
    sptrans_api_token: str
    output_mode: str  # "kinesis" or "s3"
    kinesis_stream_name: str
    kinesis_batch_size: int
    s3_raw_bucket: str
    s3_prefix: str
    aws_region: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            sptrans_api_base_url=os.environ.get("SPTRANS_API_BASE_URL", "https://api.olhovivo.sptrans.com.br/v2.1"),
            sptrans_api_token=os.environ["SPTRANS_API_TOKEN"],
            output_mode=os.environ.get("OUTPUT_MODE", "s3"),
            kinesis_stream_name=os.environ.get("KINESIS_STREAM_NAME", ""),
            kinesis_batch_size=int(os.environ.get("KINESIS_BATCH_SIZE", "500")),
            s3_raw_bucket=os.environ.get("S3_RAW_BUCKET", "transit-monitor-raw"),
            s3_prefix=os.environ.get("S3_PREFIX", "sptrans-gps"),
            aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        )
