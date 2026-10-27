#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?Set KAFKA_BOOTSTRAP_SERVERS first}"
python3 producers/producer_1.py --input data/order.jsonl
python3 consumers/consumer_1.py --output orders_landed.jsonl --max-messages "${ORDER_COUNT:-62}" --group-id "${CONSUMER1_GROUP_ID:-consumer1-demo}"
spark-submit spark/spark_job_1.py --input orders_landed.jsonl --output order_metrics.jsonl
python3 producers/producer_2.py --input order_metrics.jsonl
