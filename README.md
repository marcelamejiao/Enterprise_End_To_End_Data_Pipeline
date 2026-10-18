# Enterprise End-to-End Data Pipeline

Portfolio project demonstrating secure batch and streaming data pipelines, data quality, governance, monitoring, and CI/CD on a retail orders use case.

See [PLAN.md](PLAN.md) for the full build plan.

## Architecture

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

```
pip install -r requirements.txt
python data/generate_sample_data.py          # synthetic customers/orders/events
```

- **Pipeline A (batch)**: run `snowflake/01_database_setup.sql` through `06_privacy_policies.sql` in order against your Snowflake account, then `aws/s3_setup.md` to automate ingestion via the Lambda file-tracker (`aws/lambda/file_tracker.py`) and Snowpipe, both provisioned by `infrastructure/provision_aws.yml`.
- **Pipeline B (streaming)**: see [streaming/README.md](streaming/README.md).
- **Pipeline C (governance/ops)**: `monitoring/freshness_checks.sql` + `monitoring/alerting.py`; infra via `infrastructure/provision_aws.yml`.

Tests: `pytest tests/` (Snowflake-backed tests self-skip without credentials; streaming dedup tests run locally against `local[2]` Spark).

## Status

All three pipelines scaffolded end-to-end: Pipeline A (Snowflake medallion architecture + Snowpipe), Pipeline B (Kafka + Spark Structured Streaming with checkpointing/dedup), and Pipeline C (masking, RBAC, monitoring, alerting, Ansible, GitHub Actions CI/CD). See [PLAN.md](PLAN.md) for the full build plan and [docs/architecture.md](docs/architecture.md) for the detailed design.
