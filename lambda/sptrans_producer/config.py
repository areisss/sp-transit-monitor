import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    sptrans_api_base_url: str
    sptrans_api_token: str
    kinesis_stream_name: str
    kinesis_batch_size: int
    aws_region: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            sptrans_api_base_url=os.environ.get("SPTRANS_API_BASE_URL", "https://api.olhovivo.sptrans.com.br/v2.1"),
            sptrans_api_token=os.environ["SPTRANS_API_TOKEN"],
            kinesis_stream_name=os.environ["KINESIS_STREAM_NAME"],
            kinesis_batch_size=int(os.environ.get("KINESIS_BATCH_SIZE", "500")),
            aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        )
