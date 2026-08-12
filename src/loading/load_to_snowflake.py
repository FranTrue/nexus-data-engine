"""Loads new raw customer CSVs from S3 (LocalStack) into Snowflake and
applies the SCD Type 2 merge defined in scd_type_2_customers.sql.

Snowflake's cloud infrastructure cannot reach LocalStack directly (it only
exists on this machine), so this script bridges the two explicitly: download
each file locally, PUT it into a Snowflake internal stage, then COPY INTO the
staging table and run the merge. A loaded_files manifest table tracks which
S3 keys have already been processed, so reruns and accumulated backlogs
(e.g. the scheduler was down for a day) are handled safely.
"""
import json
import os
from pathlib import Path

import boto3
import snowflake.connector

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET_NAME = "nexus-raw-zone"
RAW_PREFIX = "raw/"
STAGE_NAME = "NEXUS_DB.RAW.CUSTOMER_STAGE"
MANIFEST_TABLE = "loaded_files"
TRANSFORM_SQL_PATH = Path(__file__).resolve().parents[1] / "transformations" / "scd_type_2_customers.sql"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=AWS_REGION,
    )


def filter_pending(objects: list, already_loaded: set) -> list:
    """Returns S3 keys not yet in the manifest, oldest first. Pure logic, no I/O."""
    pending = [obj for obj in objects if obj["Key"] not in already_loaded]
    pending.sort(key=lambda obj: obj["LastModified"])
    return [obj["Key"] for obj in pending]


def list_pending_keys(conn) -> list:
    """Lists raw/ objects that aren't recorded in the loaded_files manifest yet."""
    response = _s3_client().list_objects_v2(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX)
    objects = response.get("Contents", [])
    if not objects:
        return []

    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT s3_key FROM {MANIFEST_TABLE}")
        already_loaded = {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()

    return filter_pending(objects, already_loaded)


def download_from_s3(key: str) -> Path:
    local_path = Path("/tmp") / Path(key).name
    _s3_client().download_file(BUCKET_NAME, key, str(local_path))
    print(f"Downloaded s3://{BUCKET_NAME}/{key} to {local_path}")
    return local_path


def connect_to_snowflake():
    """Builds a Snowflake connection from the AIRFLOW_CONN_SNOWFLAKE_DEFAULT env var."""
    conn_info = json.loads(os.environ["AIRFLOW_CONN_SNOWFLAKE_DEFAULT"])
    extra = conn_info.get("extra", {})
    return snowflake.connector.connect(
        user=conn_info["login"],
        password=conn_info["password"],
        account=extra["account"],
        warehouse=extra["warehouse"],
        database=extra["database"],
        schema=conn_info.get("schema", "RAW"),
        role=extra.get("role"),
    )


def ensure_manifest_table(conn) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} "
            "(s3_key VARCHAR PRIMARY KEY, loaded_at TIMESTAMP)"
        )
    finally:
        cursor.close()


def mark_as_loaded(conn, key: str) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"INSERT INTO {MANIFEST_TABLE} (s3_key, loaded_at) VALUES (%s, CURRENT_TIMESTAMP())",
            (key,),
        )
    finally:
        cursor.close()


def parse_transform_statements(sql_text: str) -> tuple:
    """Splits scd_type_2_customers.sql into (ddl_statements, merge_statements).

    The file has four statements in a fixed order: (1) recreate the staging
    table, (2) create the dimension table if missing, (3) the MERGE that
    closes out changed records, (4) the INSERT that adds new/current
    records. COPY INTO has to happen between (2) and (3), so callers run the
    DDL batch, then COPY INTO, then the merge batch.
    """
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    if len(statements) != 4:
        raise ValueError(
            f"Expected 4 statements in the transform SQL, found {len(statements)}. "
            "This script assumes: create stg table, create dim table, MERGE, INSERT."
        )
    return statements[:2], statements[2:]


def load_and_transform(local_path: Path, conn) -> None:
    """Stages one file, loads it, and runs the SCD Type 2 merge."""
    ddl_statements, merge_statements = parse_transform_statements(TRANSFORM_SQL_PATH.read_text())

    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME}")

        for statement in ddl_statements:
            cursor.execute(statement)

        cursor.execute(f"PUT file://{local_path} @{STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        cursor.execute(
            f"COPY INTO stg_customers FROM @{STAGE_NAME} "
            f"FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1) PATTERN='.*{local_path.name}.*'"
        )

        for statement in merge_statements:
            cursor.execute(statement)

        cursor.execute(f"REMOVE @{STAGE_NAME} PATTERN='.*{local_path.name}.*'")
        print(f"Loaded {local_path.name} into stg_customers and merged into dim_customers")
    finally:
        cursor.close()


def main() -> None:
    conn = connect_to_snowflake()
    try:
        ensure_manifest_table(conn)
        pending_keys = list_pending_keys(conn)
        if not pending_keys:
            print("No new files to load.")
            return

        for key in pending_keys:
            local_path = download_from_s3(key)
            load_and_transform(local_path, conn)
            mark_as_loaded(conn, key)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
