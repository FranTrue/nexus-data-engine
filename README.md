# Nexus Data Engine

An end-to-end data pipeline built to mirror how a real company (e.g. an e-commerce or fintech)
would move customer data from raw ingestion to an analytics-ready warehouse:

```
Python (ingestion) -> S3 / LocalStack (raw zone) -> Airflow on Kubernetes (orchestration) -> Snowflake (SCD Type 2 warehouse)
```

Everything runs locally except the final warehouse hop, which uses a real Snowflake account
(a free trial works fine).

## Architecture

| Layer          | Tool                                   | Role                                                              |
|----------------|-----------------------------------------|--------------------------------------------------------------------|
| Ingestion      | Python (`pandas`, `boto3`)             | Generates synthetic customer records and uploads them as CSV      |
| Raw storage    | LocalStack (S3 emulation)              | Local `nexus-raw-zone` bucket, provisioned with Terraform          |
| Orchestration  | Apache Airflow on Kubernetes (`kind`)  | Runs the DAG: ingest -> load -> transform, deployed via Helm       |
| Warehouse      | Snowflake                              | `dim_customers` table, updated with a SCD Type 2 merge             |

LocalStack only exists on this machine, so Snowflake's cloud infrastructure can't read from it
directly (no external stage). The `load_to_snowflake` task bridges the gap explicitly: it
downloads new files from LocalStack, `PUT`s each into a Snowflake **internal** stage, then runs
`COPY INTO` + `MERGE`. A `loaded_files` manifest table in Snowflake tracks which S3 keys have
already been processed, so a backlog (e.g. the scheduler was down for a day) is handled safely
instead of only picking up the single latest file.

`data_generator.py` randomizes each customer's city/tier on every run, so the SCD Type 2 merge's
update path (closing out a changed record, inserting the new current one) actually gets
exercised instead of being a permanent no-op.

## Repository layout

```
src/ingestion/data_generator.py        # Generates customer CSVs and uploads them to S3
src/loading/load_to_snowflake.py       # Bridges S3 -> Snowflake internal stage -> COPY INTO -> MERGE
src/transformations/scd_type_2_customers.sql  # Staging + dimension DDL and the SCD Type 2 MERGE
tests/                                 # Unit tests for the pure logic in src/ (see Testing)
airflow/dags/customer_pipeline_dag.py  # DAG wiring the two tasks together
airflow/Dockerfile                     # Custom Airflow image (bakes in DAGs, src/, and dependencies)
airflow/values.yaml                    # Helm values for the Airflow chart
infrastructure/terraform/main.tf       # Creates the nexus-raw-zone S3 bucket in LocalStack
infrastructure/k8s/kind-config.yaml    # 3-node local Kubernetes cluster definition
docker-compose.yaml                    # Runs LocalStack
```

## Prerequisites

- Homebrew
- Miniforge (conda)
- Docker (Docker Desktop or Colima)
- `kind`, `kubectl`, `helm` — `brew install kind kubectl helm`
- A Snowflake account (trial is fine)

## Setup

### 1. Raw storage (LocalStack + Terraform)

```bash
docker compose up -d
curl http://localhost:4566/_localstack/health   # confirm it's healthy

cd infrastructure/terraform
terraform init
terraform apply
```

Note: the AWS provider endpoint uses `http://127.0.0.1:4566`, not `localhost`. On macOS,
`localhost` can resolve to IPv6 first, and since Docker only publishes the port on IPv4, every
API call pays a DNS-fallback delay — a two-minute `apply` instead of a two-second one.

### 2. Python environment

```bash
conda create -n nexus python=3.11 -y
conda activate nexus
python -m pip install -r requirements-dev.txt   # use `python -m pip`, not bare `pip`, to avoid PATH mismatches

python src/ingestion/data_generator.py
aws --endpoint-url=http://127.0.0.1:4566 s3 ls s3://nexus-raw-zone/raw/
```

### 3. Airflow on Kubernetes

```bash
kind create cluster --config infrastructure/k8s/kind-config.yaml
kubectl get nodes   # confirm all 3 are Ready

docker build -f airflow/Dockerfile -t nexus-data-engine-airflow:latest .
kind load docker-image nexus-data-engine-airflow:latest --name data-platform-local

helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

Create static Fernet and webserver secret keys first, so Airflow components don't rotate
credentials (and restart) on every deploy — generate your own rather than reusing any example
values:

Use `nexus-*` names, not `airflow-*` — the chart auto-creates its own secrets named
`<release-name>-fernet-key` / `<release-name>-webserver-secret-key`, and since this release is
named `airflow`, those defaults collide with the obvious names:

```bash
kubectl create namespace airflow

kubectl create secret generic nexus-fernet-key \
  --namespace airflow \
  --from-literal=fernet-key="$(python3 -c 'import secrets, base64; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())')"

kubectl create secret generic nexus-webserver-secret-key \
  --namespace airflow \
  --from-literal=webserver-secret-key="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"

helm install airflow apache-airflow/airflow \
  --version 1.12.0 \
  --namespace airflow \
  -f airflow/values.yaml
```

Note: `fernetKeySecretName` only takes effect on a fresh `helm install`, not `helm upgrade` — if
you're updating an existing release, the original (auto-generated) Fernet key stays in place
until you reinstall.

**The chart version pin (`1.12.0`) is required.** The `apache-airflow` repo's latest chart
targets Airflow 3.x by default (different component names — `api-server` instead of
`webserver`, a separate `dag-processor`), which is incompatible with the 2.8.1 image this
project builds. Chart `1.12.0` is the version whose default `appVersion` is `2.8.1`.

Access the UI:
```bash
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
```
Open `http://localhost:8080` — `admin` / `admin`.

### 4. Snowflake

Run in a Snowsight worksheet (select all statements and use **Run All** — running them one
line at a time with the cursor will silently skip the rest of the block):

```sql
CREATE WAREHOUSE IF NOT EXISTS NEXUS_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS NEXUS_DB;
CREATE SCHEMA IF NOT EXISTS NEXUS_DB.RAW;
CREATE SCHEMA IF NOT EXISTS NEXUS_DB.ANALYTICS;

CREATE ROLE IF NOT EXISTS NEXUS_LOADER;
GRANT USAGE ON WAREHOUSE NEXUS_WH TO ROLE NEXUS_LOADER;
GRANT USAGE ON DATABASE NEXUS_DB TO ROLE NEXUS_LOADER;
GRANT USAGE, CREATE STAGE, CREATE TABLE ON SCHEMA NEXUS_DB.RAW TO ROLE NEXUS_LOADER;
GRANT USAGE, CREATE TABLE ON SCHEMA NEXUS_DB.ANALYTICS TO ROLE NEXUS_LOADER;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA NEXUS_DB.RAW TO ROLE NEXUS_LOADER;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA NEXUS_DB.ANALYTICS TO ROLE NEXUS_LOADER;

CREATE USER IF NOT EXISTS AIRFLOW_SVC
  PASSWORD = '<choose-a-strong-password>'
  DEFAULT_ROLE = NEXUS_LOADER
  DEFAULT_WAREHOUSE = NEXUS_WH
  MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE NEXUS_LOADER TO USER AIRFLOW_SVC;

-- Optional: lets your own admin session read the tables NEXUS_LOADER creates,
-- since ACCOUNTADMIN doesn't automatically inherit another role's grants.
GRANT SELECT ON ALL TABLES IN SCHEMA NEXUS_DB.RAW TO ROLE ACCOUNTADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA NEXUS_DB.RAW TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA NEXUS_DB.ANALYTICS TO ROLE ACCOUNTADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA NEXUS_DB.ANALYTICS TO ROLE ACCOUNTADMIN;
```

Store the connection as a Kubernetes Secret (never commit this — it's excluded from Helm
values and from git on purpose). Replace `<account>` (the `org-account` identifier from your
Snowsight URL) and `<password>`:

```bash
kubectl create secret generic airflow-snowflake-conn \
  --namespace airflow \
  --from-literal=AIRFLOW_CONN_SNOWFLAKE_DEFAULT='{"conn_type":"snowflake","login":"AIRFLOW_SVC","password":"<password>","schema":"RAW","extra":{"account":"<account>","database":"NEXUS_DB","warehouse":"NEXUS_WH","role":"NEXUS_LOADER"}}'
```

Then apply it to the running release:
```bash
helm upgrade airflow apache-airflow/airflow \
  --version 1.12.0 \
  --namespace airflow \
  -f airflow/values.yaml
```

## Running the pipeline

From the Airflow UI, unpause and trigger `customer_ingestion_pipeline`. It runs two tasks in
order: `run_customer_generator` (Python -> S3) and `load_to_snowflake` (S3 -> Snowflake stage
-> `MERGE` into `dim_customers`).

Verify in Snowsight:
```sql
SELECT * FROM NEXUS_DB.RAW.dim_customers;
```

## Testing

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

Tests cover the pure logic extracted from `load_to_snowflake.py` (statement parsing, backlog
filtering) and `data_generator.py`'s output shape — no live S3/Snowflake connection needed. CI
runs the same suite, plus `terraform validate` and a `helm template` render of `values.yaml`
against the pinned chart version, on every push and PR.

## Troubleshooting notes

- **Bitnami image not found** (`docker.io/bitnami/postgresql:...: not found`): Bitnami moved
  older/pinned tags to the `bitnamilegacy/` namespace. `airflow/values.yaml` already repoints
  the chart's bundled Postgres subchart there.
- **Task logs unreadable in the UI** (`Could not read served logs`): KubernetesExecutor task
  pods are ephemeral and this project has no remote logging configured. Debug directly instead:
  ```bash
  kubectl run debug-snowflake --rm -it --restart=Never \
    --namespace=airflow \
    --image=nexus-data-engine-airflow:latest \
    --overrides='{"spec":{"containers":[{"name":"debug-snowflake","image":"nexus-data-engine-airflow:latest","imagePullPolicy":"Never","command":["python","/opt/airflow/src/loading/load_to_snowflake.py"],"envFrom":[{"secretRef":{"name":"airflow-snowflake-conn"}}],"env":[{"name":"AWS_ENDPOINT_URL","value":"http://host.docker.internal:4566"},{"name":"AWS_DEFAULT_REGION","value":"us-east-1"}]}]}}'
  ```
- **kind cluster containers not running / `containerd snapshotter` errors**: usually Docker
  Desktop doesn't have enough resources for a 3-node cluster plus the Airflow stack. Allocate
  at least 4 CPUs / 8 GB RAM in Docker Desktop settings, then `kind delete cluster --name
  data-platform-local` and recreate.
- **Snowsight silently skips statements**: the Run button (▶) only executes the statement under
  the cursor unless you select the full block first (or use Run All). If a `CREATE`/`GRANT`
  seems to have had no effect, this is almost always why.

## Teardown

```bash
kind delete cluster --name data-platform-local
docker compose down -v
cd infrastructure/terraform && terraform destroy
```
