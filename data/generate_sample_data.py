"""Generate synthetic customers, orders, and order-event data for the retail
orders pipeline. Output lands in this directory as CSV, ready for a manual
COPY INTO (Phase 1) or upload to S3 incoming/ (Phase 1 automation).

Usage:
    python data/generate_sample_data.py --customers 200 --orders 1000
"""

import argparse
import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

OUTPUT_DIR = Path(__file__).parent
ORDER_STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled", "refunded"]


def generate_customers(fake: Faker, count: int) -> list[dict]:
    customers = []
    start = datetime.now() - timedelta(days=365)
    for customer_id in range(1, count + 1):
        customers.append(
            {
                "customer_id": customer_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "signup_ts": (start + timedelta(days=random.randint(0, 365))).isoformat(),
            }
        )
    return customers


def generate_orders(fake: Faker, customers: list[dict], count: int) -> list[dict]:
    orders = []
    start = datetime.now() - timedelta(days=90)
    for order_id in range(1, count + 1):
        customer = random.choice(customers)
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_ts": (start + timedelta(minutes=random.randint(0, 90 * 24 * 60))).isoformat(),
                "status": random.choice(ORDER_STATUSES),
                "amount": round(random.uniform(5, 500), 2),
            }
        )
    return orders


def generate_order_events(orders: list[dict]) -> list[dict]:
    """Simulated streaming events for Pipeline B, including a few intentional
    duplicates and late events to exercise dedup/watermarking downstream."""
    events = []
    for order in orders:
        event = {
            "event_id": str(uuid.uuid4()),
            "order_id": order["order_id"],
            "event_type": "purchase",
            "event_ts": order["order_ts"],
        }
        events.append(event)
        if random.random() < 0.05:  # ~5% duplicate delivery
            events.append(dict(event))
    return events


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=200)
    parser.add_argument("--orders", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    fake = Faker()
    Faker.seed(args.seed)

    customers = generate_customers(fake, args.customers)
    orders = generate_orders(fake, customers, args.orders)
    events = generate_order_events(orders)

    write_csv(customers, OUTPUT_DIR / "sample_customers.csv")
    write_csv(orders, OUTPUT_DIR / "sample_orders.csv")
    write_csv(events, OUTPUT_DIR / "sample_order_events.csv")

    print(f"Wrote {len(customers)} customers, {len(orders)} orders, {len(events)} events to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
