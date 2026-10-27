#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?Set KAFKA_BOOTSTRAP_SERVERS first}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-kafka-topics.sh}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-2}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-2}"
ORDERS="${KAFKA_ORDERS_TOPIC:-orders}"
METRICS="${KAFKA_ORDER_METRICS_TOPIC:-order_metrics}"

for topic in "$ORDERS" "$METRICS"; do
  "$KAFKA_TOPICS_BIN" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create --if-not-exists --topic "$topic" \
    --partitions "$PARTITIONS" --replication-factor "$RF"
done
