#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

from kafka import KafkaConsumer


def parse_args():
    p = argparse.ArgumentParser(description="Land orders from Kafka into JSONL.")
    p.add_argument("--output", required=True)
    p.add_argument("--max-messages", type=int, default=None)
    p.add_argument("--group-id", default="consumer1-group")
    p.add_argument("--topic", default=os.getenv("KAFKA_ORDERS_TOPIC", "orders"))
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    return p.parse_args()


def main():
    args = parse_args()
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers.split(","),
        api_version=(3, 6),
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    output = Path(args.output)
    count = 0
    try:
        with output.open("a") as handle:
            for message in consumer:
                handle.write(json.dumps(message.value, separators=(",", ":")) + "\n")
                handle.flush()
                consumer.commit()
                count += 1
                print(f"Landed {count}: {message.value.get('order_id')}")
                if args.max_messages is not None and count >= args.max_messages:
                    break
    finally:
        consumer.close()
    print(f"Landed {count} records to {output}.")


if __name__ == "__main__":
    main()
