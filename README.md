# Assignment 2 — Distributed Food Ordering System

Team 53 implementation for Big Data 2026 (UE24CS343AB2).

## Architecture

- **VM1:** Producer 1, Spark Job 2, Consumer 2
- **VM2:** KRaft Controller + Kafka Broker 1
- **VM3:** Kafka Broker 2
- **VM4:** Consumer 1, Spark Job 1, Producer 2

Pipeline:

```text
Team dataset
   |
   v
VM1 Producer 1 --orders--> Kafka cluster (2 partitions, RF=2)
                                |
                                v
                         VM4 Consumer 1
                                |
                                v
                      orders_landed.jsonl
                                |
                                v
                         VM4 Spark Job 1
                                |
                                v
                       order_metrics.jsonl
                                |
                                v
                       VM4 Producer 2
                                |
                                v
                   Kafka topic: order_metrics
                                |
                                v
                         VM1 Consumer 2
                                |
                                v
                  order_metrics_landed.jsonl
                                |
                                v
                         VM1 Spark Job 2
                           /           \
                          v             v
             restaurant_summary.csv  city_summary.csv
```

The assignment requires Spark to read local landing files, not Kafka directly, and does not use HDFS.

## Requirements

- Java 11
- Apache Kafka 3.9.x
- Apache Spark with PySpark
- Python 3.12 recommended for Spark jobs
- ZeroTier networking between the four VMs
- Kafka topics `orders` and `order_metrics`, each with 2 partitions and replication factor 2

## Repository layout

```text
config/
  config.example.env

data/
  order.jsonl
producers/
  producer_1.py
  producer_2.py
consumers/
  consumer_1.py
  consumer_2.py
spark/
  spark_job_1.py
  spark_job_2.py
scripts/
  create_topics.sh
  check_topics.sh
  run_stage1.sh
  run_stage2.sh
  clean_outputs.sh
requirements.txt
```

## 1. Configure each VM

Copy the example configuration:

```bash
cp config/config.example.env config/.env
```

Set `KAFKA_BOOTSTRAP_SERVERS` to the ZeroTier IP and port of the Kafka brokers. The expected form is:

```text
VM2_ZEROTIER_IP:9092,VM3_ZEROTIER_IP:9092
```

Do not commit `config/.env`.

## 2. Install Python dependencies

For machines running producers/consumers/Spark jobs:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

VM3 does not need the Python dependencies to run its Kafka broker role.

## 3. Create the Kafka topics

Run from a machine that can reach the Kafka cluster:

```bash
source config/.env
bash scripts/create_topics.sh
```

Verify:

```bash
source config/.env
bash scripts/check_topics.sh
```

Expected configuration for both topics:

```text
partitions = 2
replication factor = 2
```

## 4. Stage 1 — order enrichment

### VM1 — Producer 1

```bash
source config/.env
python3 producers/producer_1.py --input data/order.jsonl
```

### VM4 — Consumer 1

```bash
source config/.env
python3 consumers/consumer_1.py --output orders_landed.jsonl --max-messages 62
```

### VM4 — Spark Job 1

```bash
spark-submit spark/spark_job_1.py \
  --input orders_landed.jsonl \
  --output order_metrics.jsonl
```

### VM4 — Producer 2

```bash
source config/.env
python3 producers/producer_2.py --input order_metrics.jsonl
```

## 5. Stage 2 — operations analytics

### VM1 — Consumer 2

```bash
source config/.env
python3 consumers/consumer_2.py --output order_metrics_landed.jsonl --max-messages 44
```

### VM1 — Spark Job 2

```bash
spark-submit spark/spark_job_2.py \
  --input order_metrics_landed.jsonl \
  --output-dir output
```

Final files:

```text
output/restaurant_summary.csv
output/city_summary.csv
```

## Cleaning and rerunning

Before a fresh end-to-end run, remove local outputs:

```bash
bash scripts/clean_outputs.sh
```

If a consumer group has already committed offsets, use a new group id when rerunning, for example:

```bash
python3 consumers/consumer_1.py \
  --output orders_landed.jsonl \
  --max-messages 62 \
  --group-id consumer1-demo-2
```

## Required broker failure demo

The assignment requires stopping **VM3 (Kafka Broker 2)** and demonstrating that replicated Kafka data remains available through the surviving broker. Controller handover is out of scope.

During the demo:

1. Start both brokers.
2. Create/verify both topics with RF=2.
3. Produce/consume data normally.
4. Stop Kafka on VM3.
5. Show that the replicated partitions remain available through VM2.
6. Restart VM3 and verify the broker rejoins the cluster.

Do not change the business rules or hardcode final outputs.
