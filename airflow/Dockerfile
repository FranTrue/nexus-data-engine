FROM apache/airflow:2.8.1-python3.11

# Build context = repo root, so we can COPY both deps and pipeline code below.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake \
    -r /tmp/requirements.txt

COPY airflow/dags/ /opt/airflow/dags/
COPY src/ /opt/airflow/src/
