"""Generates synthetic customer records and uploads them as CSV to the
LocalStack S3 raw zone. Entry point for the ingestion stage of the pipeline.
"""
import os
import random
from datetime import datetime

import boto3
import pandas as pd

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET_NAME = "nexus-raw-zone"

CITIES = ["New York", "Chicago", "San Francisco", "Austin", "Seattle"]
TIERS = ["Standard", "Premium"]
BASE_CUSTOMERS = [
    {"customer_id": 101, "first_name": "Alice", "last_name": "Smith"},
    {"customer_id": 102, "first_name": "Bob", "last_name": "Johnson"},
    {"customer_id": 103, "first_name": "Charlie", "last_name": "Brown"},
]


def generate_customer_data() -> pd.DataFrame:
    """Builds a synthetic customer dataset, randomizing city/tier per run so
    the SCD Type 2 merge downstream actually gets attribute changes to react to.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    data = [
        {**customer, "city": random.choice(CITIES), "tier": random.choice(TIERS), "updated_at": today}
        for customer in BASE_CUSTOMERS
    ]
    return pd.DataFrame(data)


def upload_to_local_s3(df: pd.DataFrame) -> None:
    """Writes the dataframe to CSV and uploads it to the raw zone bucket."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=AWS_REGION
    )

    # Bucket may not exist yet on a fresh LocalStack instance.
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except Exception:
        s3_client.create_bucket(Bucket=BUCKET_NAME)

    file_name = f"customers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = f"/tmp/{file_name}"
    df.to_csv(file_path, index=False)

    s3_client.upload_file(file_path, BUCKET_NAME, f"raw/{file_name}")
    print(f"Successfully uploaded {file_name} to s3://{BUCKET_NAME}/raw/")


if __name__ == "__main__":
    df_customers = generate_customer_data()
    upload_to_local_s3(df_customers)
