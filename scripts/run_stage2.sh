#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?Set KAFKA_BOOTSTRAP_SERVERS first}"
python3 consumers/consumer_2.py --output order_metrics_landed.jsonl --max-messages "${METRICS_COUNT:-44}" --group-id "${CONSUMER2_GROUP_ID:-consumer2-demo}"
spark-submit spark/spark_job_2.py --input order_metrics_landed.jsonl --output-dir output
