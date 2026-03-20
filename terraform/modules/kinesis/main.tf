variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

variable "raw_bucket_arn" {
  description = "ARN of the raw S3 bucket for Firehose delivery"
  type        = string
}

variable "raw_bucket_id" {
  description = "ID of the raw S3 bucket for Firehose delivery"
  type        = string
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

# --- Kinesis Data Stream ---

resource "aws_kinesis_stream" "sptrans_gps" {
  name = "${var.project_name}-sptrans-gps-raw"

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  retention_period = 24

  tags = var.tags
}

# --- IAM Role for Firehose ---

data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.project_name}-firehose-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "firehose_policy" {
  # Read from Kinesis Data Stream
  statement {
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.sptrans_gps.arn]
  }

  # Write to S3
  statement {
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      var.raw_bucket_arn,
      "${var.raw_bucket_arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${var.project_name}-firehose-policy"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose_policy.json
}

# --- Kinesis Firehose Delivery Stream ---

resource "aws_kinesis_firehose_delivery_stream" "sptrans_to_s3" {
  name        = "${var.project_name}-sptrans-to-s3"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.sptrans_gps.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = var.raw_bucket_arn
    prefix              = "sptrans-gps/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "sptrans-gps-errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    buffering_size     = 5
    buffering_interval = 60
    compression_format = "GZIP"
  }

  tags = var.tags
}

# --- Outputs ---

output "stream_name" {
  value = aws_kinesis_stream.sptrans_gps.name
}

output "stream_arn" {
  value = aws_kinesis_stream.sptrans_gps.arn
}

output "firehose_arn" {
  value = aws_kinesis_firehose_delivery_stream.sptrans_to_s3.arn
}
