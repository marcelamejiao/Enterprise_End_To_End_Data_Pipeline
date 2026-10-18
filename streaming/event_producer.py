"""Publish simulated purchase/payment events to Kafka topic `order_events`
for Pipeline B. Reuses the order events generated in data/sample_order_events.csv
(including the intentional ~5% duplicate deliveries), or generates fresh ones
on the fly, to exercise dedup/watermarking in streaming/spark_consumer.py.

Usage:
    python streaming/event_producer.py --rate 10 --duration 60
"""

import argparse
import csv
import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

TOPIC = "order_events"
SAMPLE_EVENTS_PATH = Path(__file__).parent.parent / "data" / "sample_order_events.csv"


def load_sample_events() -> list[dict]:
    if not SAMPLE_EVENTS_PATH.exists():
        return []
    with SAMPLE_EVENTS_PATH.open() as f:
        return list(csv.DictReader(f))


def build_event(sample: dict | None) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    if sample:
        return {
            "event_id": sample["event_id"],
            "order_id": int(sample["order_id"]),
            "event_type": sample["event_type"],
            "event_ts": now,  # overwrite so it streams "live" regardless of original ts
        }
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": random.randint(1, 1000),
        "event_type": random.choice(["purchase", "payment"]),
        "event_ts": now,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=5.0, help="events per second")
    parser.add_argument("--duration", type=float, default=0, help="seconds to run, 0 = forever")
    parser.add_argument("--duplicate-rate", type=float, default=0.05)
    parser.add_argument("--late-rate", type=float, default=0.03, help="fraction of events delayed")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sample_events = load_sample_events()
    start = time.monotonic()
    sent = 0

    while args.duration == 0 or (time.monotonic() - start) < args.duration:
        sample = random.choice(sample_events) if sample_events else None
        event = build_event(sample)

        if random.random() < args.late_rate:
            # Simulate a late-arriving event: timestamp lags behind send time.
            event["event_ts"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            time.sleep(random.uniform(1, 5))

        producer.send(TOPIC, value=event)
        sent += 1

        if random.random() < args.duplicate_rate:
            producer.send(TOPIC, value=dict(event))  # exact duplicate delivery
            sent += 1

        time.sleep(1 / args.rate)

    producer.flush()
    print(f"Sent {sent} events to topic '{TOPIC}'")


if __name__ == "__main__":
    main()
