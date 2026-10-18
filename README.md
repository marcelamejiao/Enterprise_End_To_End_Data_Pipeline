# Enterprise End-to-End Data Pipeline

Portfolio project demonstrating secure batch and streaming data pipelines, data quality, governance, monitoring, and CI/CD on a retail orders use case.

See [PLAN.md](PLAN.md) for the full build plan and [Project.md](Project.md) for the original project brief.

## Architecture

- **Pipeline A — Batch ingestion**: S3 → Snowpipe → Snowflake Bronze/Silver/Gold (medallion architecture) using SQL transformations, Dynamic Tables, and Streams/Tasks for CDC.
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

## Status

Scaffolding in progress — see [PLAN.md](PLAN.md) for build order and current phase.
