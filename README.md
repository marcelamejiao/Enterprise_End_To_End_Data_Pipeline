# Enterprise End-to-End Data Pipeline

Portfolio project demonstrating secure batch and streaming data pipelines, data quality, governance, monitoring, and CI/CD on a retail orders use case.

See [PLAN.md](PLAN.md) for the full build plan.

## Architecture

![Architecture diagram: Pipeline A (S3, Lambda, Snowpipe) and Pipeline B (Kafka, Spark) both feed the Bronze/Silver/Gold medallion layers in Snowflake, consumed by BI/analytics, with Pipeline C governance and monitoring wrapping the whole system.](docs/img/architecture.png)

- **Pipeline A — Batch ingestion**: S3 → (Lambda file-tracking + Snowpipe, independently) → Snowflake Bronze/Silver/Gold (medallion architecture) using SQL transformations, Dynamic Tables, and Streams/Tasks for CDC. The Lambda tracks every file delivery in DynamoDB, flags duplicates, and quarantines invalid files before Snowpipe loads them.
- **Pipeline B — Real-time event processing**: Kafka → Spark Structured Streaming, with checkpointing, watermarking, and dedup, writing to Delta Lake or Snowflake.
- **Pipeline C — Governance & operational monitoring**: PII masking, role-based access, pipeline run logs, freshness checks, alerting, and CI/CD via GitHub Actions.

Details in [docs/architecture.md](docs/architecture.md) (added as the project is built out).

## Repo layout

```
.
├── data/              # synthetic data generators
├── aws/               # S3 + Lambda setup
├── snowflake/         # DDL, transformations, CDC, privacy policies
├── streaming/          # Kafka producer + Spark consumer
├── tests/             # pytest suite
├── monitoring/        # run logs, freshness checks, alerting
├── infrastructure/    # Ansible playbooks
├── docs/              # architecture, data dictionary, incident runbook
└── .github/workflows/ # CI/CD
```

## Getting started

Steps 1–2 are required no matter what. After that, **pick one path** for how
files land in Bronze — Path A needs no AWS account, Path B automates it with
S3/Lambda/Snowpipe. Both paths converge again at step 3, which is also
required no matter which path you took.

### 1. Local setup (required)

1. `pip install -r requirements.txt`
2. `python data/generate_sample_data.py` — writes `data/sample_customers.csv`, `sample_orders.csv`, `sample_order_events.csv`

### 2. Snowflake foundation (required — do this before picking a path below)

3. In a Snowflake worksheet (or `snowsql`), run `snowflake/01_database_setup.sql`. This creates the database, schemas, warehouse, and raw tables that **both paths below depend on** — run it even if you're going straight to Path B.

### Now pick a path for Pipeline A's ingestion

#### Path A — manual load, no AWS account needed

- Run **Part 1 only** of `snowflake/02_bronze_ingestion.sql` (Part 2, Snowpipe, belongs to Path B).
- Load the sample files into each table's stage, then run Part 1's `COPY INTO` statements:
  ```
  snowsql -q "PUT file://data/sample_orders.csv @RETAIL_DWH.BRONZE.%RAW_ORDERS"
  snowsql -q "PUT file://data/sample_customers.csv @RETAIL_DWH.BRONZE.%RAW_CUSTOMERS"
  ```
- Done with ingestion — continue at step 3 below.

#### Path B — AWS automation, needs an AWS account

- Follow [aws/s3_setup.md](aws/s3_setup.md) steps 1–4: provision the bucket/Lambda/DynamoDB table, create the Snowflake storage integration (note: this is a two-pass IAM trust setup — the doc explains why), create the stage/pipe (Part 2 of `02_bronze_ingestion.sql`), and wire Snowpipe's own S3 notification.
- Verify by uploading a file and checking both Snowflake and DynamoDB, per step 4 in that doc.
- Done with ingestion — continue at step 3 below.

### 3. Finish Pipeline A (required, both paths converge here)

4. Run `snowflake/03_silver_transformations.sql`, `04_gold_metrics.sql`, `05_streams_tasks.sql`, `06_privacy_policies.sql`, in order.
5. Verify: `SELECT * FROM RETAIL_DWH.GOLD.DAILY_SALES;` should return aggregated rows.

### 4. Pipeline B — Streaming

6. Follow [streaming/README.md](streaming/README.md): start Kafka via `docker compose`, run the producer, run the Spark consumer.

### 5. Pipeline C — Governance & monitoring

7. Run `monitoring/freshness_checks.sql` against your Snowflake account to create the freshness/anomaly views used in step 9.
8. Confirm masking works: query `RAW_CUSTOMERS.EMAIL` as a role other than `PII_ACCESS_ROLE` (should show `***MASKED***`), then as `PII_ACCESS_ROLE` (should show the real value).
9. Set `SNOWFLAKE_ACCOUNT`/`SNOWFLAKE_USER`/`SNOWFLAKE_PASSWORD`/`SNOWFLAKE_WAREHOUSE`/`ALERT_WEBHOOK_URL` env vars and run `python monitoring/alerting.py` (or schedule it) to exercise the alert path.

### 6. CI/CD (GitHub)

10. In the GitHub repo's Settings → Secrets, add: `SNOWFLAKE_ACCOUNT`/`USER`/`PASSWORD`/`WAREHOUSE`/`DEPLOY_ROLE`, `AWS_ACCESS_KEY_ID`/`SECRET_ACCESS_KEY`/`DEFAULT_REGION`, `SNOWFLAKE_IAM_USER_ARN`/`EXTERNAL_ID`, `ALERT_WEBHOOK_URL`.
11. Open a PR touching `snowflake/`, `streaming/`, or Python files to trigger `lint-and-test.yml` / `streaming-ci.yml`. Merging to `main` with `snowflake/**` changes triggers `deploy-snowflake.yml` automatically. Run `deploy-infra.yml` manually (`workflow_dispatch`) only when you intend to (re)provision AWS — it never runs on its own, to avoid surprise cloud cost.

### Tests

`pytest tests/` — Snowflake-backed tests self-skip without credentials; streaming dedup and Lambda file-tracker tests run fully locally (`local[2]` Spark, `moto`-mocked AWS), no account needed for those.

## Status

All three pipelines scaffolded end-to-end: Pipeline A (Snowflake medallion architecture + Snowpipe), Pipeline B (Kafka + Spark Structured Streaming with checkpointing/dedup), and Pipeline C (masking, RBAC, monitoring, alerting, Ansible, GitHub Actions CI/CD). See [PLAN.md](PLAN.md) for the full build plan and [docs/architecture.md](docs/architecture.md) for the detailed design.
