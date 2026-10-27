#!/usr/bin/env bash
set -euo pipefail
rm -f orders_landed.jsonl order_metrics.jsonl order_metrics_landed.jsonl
rm -f output/restaurant_summary.csv output/city_summary.csv
rm -rf output/_spark_* *.spark_tmp
