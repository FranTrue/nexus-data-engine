from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customer_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Orchestrates synthetic customer ingestion to S3 and the Snowflake SCD Type 2 load",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "s3", "localstack", "snowflake"],
) as dag:

    ingest_customers = BashOperator(
        task_id="run_customer_generator",
        bash_command="python /opt/airflow/src/ingestion/data_generator.py",
    )

    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command="python /opt/airflow/src/loading/load_to_snowflake.py",
    )

    ingest_customers >> load_to_snowflake