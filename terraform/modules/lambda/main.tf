variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

variable "kinesis_stream_name" {
  description = "Kinesis Data Stream name for the producer to write to"
  type        = string
}

variable "kinesis_stream_arn" {
  description = "Kinesis Data Stream ARN"
  type        = string
}

variable "lambda_code_bucket" {
  description = "S3 bucket for Lambda deployment artifacts"
  type        = string
}

variable "sptrans_api_token" {
  description = "SPTrans OlhoVivo API token"
  type        = string
  sensitive   = true
}

variable "sptrans_api_token_ssm_param" {
  description = "SSM Parameter Store path for SPTrans API token"
  type        = string
  default     = "/transit-monitor/sptrans-api-token"
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

# --- SSM Parameter for API Token ---

resource "aws_ssm_parameter" "sptrans_api_token" {
  name        = var.sptrans_api_token_ssm_param
  description = "SPTrans OlhoVivo API token"
  type        = "SecureString"
  value       = "PLACEHOLDER_REPLACE_AFTER_DEPLOY"

  lifecycle {
    ignore_changes = [value]
  }

  tags = var.tags
}

# --- IAM Role for Lambda ---

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.project_name}-producer-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "lambda_policy" {
  # CloudWatch Logs
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }

  # Write to Kinesis
  statement {
    effect = "Allow"
    actions = [
      "kinesis:PutRecord",
      "kinesis:PutRecords",
    ]
    resources = [var.kinesis_stream_arn]
  }

  # Read SSM Parameter
  statement {
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.sptrans_api_token.arn]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${var.project_name}-producer-lambda-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_policy.json
}

# --- Lambda Function ---

resource "aws_lambda_function" "sptrans_producer" {
  function_name = "${var.project_name}-sptrans-producer"
  runtime       = "python3.12"
  handler       = "sptrans_producer.handler.lambda_handler"
  memory_size   = 256
  timeout       = 60

  s3_bucket = var.lambda_code_bucket
  s3_key    = "lambda/sptrans-producer.zip"

  role = aws_iam_role.lambda.arn

  environment {
    variables = {
      SPTRANS_API_TOKEN    = var.sptrans_api_token
      KINESIS_STREAM_NAME  = var.kinesis_stream_name
      SPTRANS_API_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
    }
  }

  tags = var.tags
}

# --- CloudWatch Log Group ---

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${aws_lambda_function.sptrans_producer.function_name}"
  retention_in_days = 14
  tags              = var.tags
}

# --- EventBridge Scheduler (30 second intervals) ---

data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.project_name}-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "scheduler_policy" {
  statement {
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.sptrans_producer.arn]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "${var.project_name}-scheduler-policy"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_policy.json
}

resource "aws_scheduler_schedule" "sptrans_poll" {
  name       = "${var.project_name}-sptrans-poll-30s"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(1 minute)"

  target {
    arn      = aws_lambda_function.sptrans_producer.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}

# --- Outputs ---

output "lambda_function_name" {
  value = aws_lambda_function.sptrans_producer.function_name
}

output "lambda_function_arn" {
  value = aws_lambda_function.sptrans_producer.arn
}
