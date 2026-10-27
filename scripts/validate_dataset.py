#!/usr/bin/env python3
import argparse
import datetime as dt
import json
from collections import Counter

FIELDS = [
    "order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
    "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
    "expected_delivery_timestamp", "delivered_timestamp", "distance_km",
    "order_amount", "delivery_fee", "status",
]
TIMESTAMPS = [
    "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
    "expected_delivery_timestamp", "delivered_timestamp",
]


def valid_record(r):
    if any(r.get(k) is None for k in FIELDS):
        return False
    try:
        for k in TIMESTAMPS:
            dt.datetime.strptime(r[k], "%Y-%m-%d %H:%M:%S")
        distance = float(r["distance_km"])
        amount = float(r["order_amount"])
    except (TypeError, ValueError):
        return False
    return distance > 0 and amount > 0 and r["status"] == "DELIVERED"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("input")
    args = p.parse_args()

    rows = [json.loads(line) for line in open(args.input) if line.strip()]
    unique = []
    seen = set()
    for row in rows:
        key = json.dumps(row, sort_keys=True, separators=(",", ":"))
        if key not in seen:
            seen.add(key)
            unique.append(row)

    valid = [r for r in unique if valid_record(r)]
    print(f"Input records: {len(rows)}")
    print(f"Exact duplicates removed: {len(rows) - len(unique)}")
    print(f"Valid records after assignment rules: {len(valid)}")
    print(f"Statuses: {dict(Counter(r.get('status') for r in rows))}")


if __name__ == "__main__":
    main()
