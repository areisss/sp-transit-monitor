terraform {
  required_version = ">= 1.7"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }

  backend "s3" {
    bucket         = "transit-monitor-terraform-state"
    key            = "dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "transit-monitor-terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# --- Storage ---

module "storage" {
  source       = "../../modules/storage"
  project_name = var.project_name
  tags         = local.common_tags
}

# --- Kinesis ---

module "kinesis" {
  source         = "../../modules/kinesis"
  project_name   = var.project_name
  raw_bucket_arn = module.storage.raw_bucket_arn
  raw_bucket_id  = module.storage.raw_bucket_id
  tags           = local.common_tags
}

# --- Lambda ---

module "lambda" {
  source              = "../../modules/lambda"
  project_name        = var.project_name
  output_mode         = "kinesis"
  kinesis_stream_name = module.kinesis.stream_name
  kinesis_stream_arn  = module.kinesis.stream_arn
  lambda_code_bucket  = module.storage.lambda_code_bucket_name
  sptrans_api_token   = var.sptrans_api_token
  tags                = local.common_tags
}

# --- Monitoring ---

module "monitoring" {
  source               = "../../modules/monitoring"
  project_name         = var.project_name
  lambda_function_name = module.lambda.lambda_function_name
  kinesis_stream_name  = module.kinesis.stream_name
  alert_email          = var.alert_email
  tags                 = local.common_tags
}

# --- Databricks ---

module "databricks" {
  source                  = "../../modules/databricks"
  project_name            = var.project_name
  raw_bucket_name         = module.storage.raw_bucket_name
  checkpoints_bucket_name = module.storage.checkpoints_bucket_name
  tags                    = local.common_tags
}

