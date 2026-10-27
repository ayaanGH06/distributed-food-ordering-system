#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?Set KAFKA_BOOTSTRAP_SERVERS first}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-kafka-topics.sh}"
for topic in "${KAFKA_ORDERS_TOPIC:-orders}" "${KAFKA_ORDER_METRICS_TOPIC:-order_metrics}"; do
  echo "=== $topic ==="
  "$KAFKA_TOPICS_BIN" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$topic"
done
