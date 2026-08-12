terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  # 127.0.0.1, not localhost — avoids IPv6-first DNS delays on macOS.
  endpoints {
    s3 = "http://127.0.0.1:4566"
  }
}

resource "aws_s3_bucket" "raw_zone" {
  bucket = "nexus-raw-zone"
}

output "s3_bucket_name" {
  value       = aws_s3_bucket.raw_zone.id
  description = "Name of the raw zone bucket created in LocalStack"
}