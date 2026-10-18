"""Poll the freshness/volume/run-log checks defined in freshness_checks.sql
and send a webhook alert (Slack-compatible) when something is wrong.

Intended to run on a schedule (cron, GitHub Actions `schedule` trigger, or a
Snowflake Task calling out via external function) separate from the pipeline
runs themselves, so an alert still fires even if a pipeline silently stops
running at all.

Usage:
    python monitoring/alerting.py
Requires env vars SNOWFLAKE_ACCOUNT/USER/PASSWORD/WAREHOUSE and ALERT_WEBHOOK_URL.
"""

import json
import os
import urllib.request

import snowflake.connector

CHECKS = {
    "freshness": "SELECT * FROM RETAIL_DWH.GOLD.FRESHNESS_CHECK WHERE IS_STALE",
    "volume_anomaly": (
        "SELECT * FROM RETAIL_DWH.SILVER.VOLUME_ANOMALY_CHECK "
        "WHERE IS_ANOMALY AND LOAD_DATE = CURRENT_DATE()"
    ),
    "failed_runs": (
        "SELECT * FROM RETAIL_DWH.SILVER.FAILED_RUNS "
        "WHERE RUN_STARTED_AT >= DATEADD('hour', -24, CURRENT_TIMESTAMP())"
    ),
}


def connect() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )


def send_alert(webhook_url: str, text: str) -> None:
    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url, data=payload, headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req, timeout=10)  # noqa: S310 - trusted internal webhook URL


def main() -> None:
    webhook_url = os.environ["ALERT_WEBHOOK_URL"]
    conn = connect()
    triggered = []

    try:
        cur = conn.cursor()
        for name, query in CHECKS.items():
            rows = cur.execute(query).fetchall()
            if rows:
                triggered.append((name, len(rows)))
    finally:
        conn.close()

    if triggered:
        lines = [f"- {name}: {count} row(s)" for name, count in triggered]
        send_alert(webhook_url, "Pipeline alert triggered:\n" + "\n".join(lines))
        print("Alert sent:", triggered)
    else:
        print("All checks passed, no alert sent.")


if __name__ == "__main__":
    main()
