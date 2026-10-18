# Data Dictionary

## RETAIL_DWH.BRONZE.RAW_CUSTOMERS

| Column | Type | Sensitivity | Notes |
|---|---|---|---|
| CUSTOMER_ID | NUMBER | Internal | Natural key |
| FIRST_NAME | VARCHAR | Internal | |
| LAST_NAME | VARCHAR | Internal | |
| EMAIL | VARCHAR | **PII** | Masked by `EMAIL_MASK`; unmasked only for `PII_ACCESS_ROLE` |
| PHONE | VARCHAR | **PII** | Masked by `PHONE_MASK`; unmasked only for `PII_ACCESS_ROLE` |
| SIGNUP_TS | TIMESTAMP_NTZ | Internal | |
| INGESTED_AT | TIMESTAMP_NTZ | Internal | Load timestamp, used for dedup ordering |

Owner: Data Engineering. Retention: indefinite (synthetic data; in production, follow legal retention + deletion policy — see "Data deletion" below).

## RETAIL_DWH.BRONZE.RAW_ORDERS

| Column | Type | Sensitivity | Notes |
|---|---|---|---|
| ORDER_ID | NUMBER | Internal | Natural key |
| CUSTOMER_ID | NUMBER | Internal | FK to RAW_CUSTOMERS |
| ORDER_TS | TIMESTAMP_NTZ | Internal | |
| STATUS | VARCHAR | Internal | One of: pending, paid, shipped, delivered, cancelled, refunded |
| AMOUNT | NUMBER(12,2) | Internal | |
| SOURCE_FILE | VARCHAR | Internal | Lineage: originating S3 object |
| INGESTED_AT | TIMESTAMP_NTZ | Internal | |

Owner: Data Engineering.

## RETAIL_DWH.SILVER.CLEAN_ORDERS / CUSTOMERS

Deduplicated, validated views over the Bronze tables above. Same sensitivity
as their Bronze sources — masking is inherited from the base table.

## RETAIL_DWH.GOLD.DAILY_SALES / CUSTOMER_SEGMENTS

Aggregated business metrics. No PII. Owner: Analytics/BI. Business
definitions:
- `TOTAL_REVENUE` = `SUM(AMOUNT)` over non-rejected orders for a given sales date + status.
- `SEGMENT` (`CUSTOMER_SEGMENTS`) = lifetime value bucket: high_value (≥$1000), mid_value (≥$200), low_value (otherwise).

## RETAIL_DWH.SILVER.PIPELINE_RUN_LOG

Operational metadata (not business data): pipeline name, run window, rows
processed/rejected, status. Owner: Data Engineering. No PII.

## Data deletion workflow

A customer deletion request should:
1. Delete/redact the row in `RAW_CUSTOMERS` (email/phone/name).
2. Re-run the Silver views (they're views, so no separate deletion needed there).
3. Confirm no copies exist in `monitoring/` exports or ad hoc extracts — this
   is a process/audit step, not an automated one in this portfolio project,
   and should be logged in `docs/incident_runbook.md`-style records in a
   real deployment.
