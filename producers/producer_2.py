#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

from kafka import KafkaProducer


def parse_args():
    p = argparse.ArgumentParser(description="Publish enriched metrics to Kafka.")
    p.add_argument("--input", required=True)
    p.add_argument("--topic", default=os.getenv("KAFKA_ORDER_METRICS_TOPIC", "order_metrics"))
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    return p.parse_args()


def main():
    args = parse_args()
    path = Path(args.input)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        acks=os.getenv("KAFKA_ACKS", "all"),
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
    )

    sent = 0
    try:
        with path.open() as handle:
            for line in handle:
                if not line.strip():
                    continue
                record = json.loads(line)
                key = str(record.get("order_id", f"line-{sent}"))
                producer.send(args.topic, key=key, value=record).get(timeout=30)
                sent += 1
                print(f"Published {sent}: {key}")
        producer.flush()
        print(f"Published {sent} records to {args.topic}.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
