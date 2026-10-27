#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

EXPECTED_FIELDS = [
    "order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
    "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
    "expected_delivery_timestamp", "delivered_timestamp", "distance_km",
    "order_amount", "delivery_fee", "status",
]
TIMESTAMP_FIELDS = [
    "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
    "expected_delivery_timestamp", "delivered_timestamp",
]


def parse_args():
    p = argparse.ArgumentParser(description="Spark Job 1: enrich valid delivered orders.")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("A2-Spark-Job-1").getOrCreate()
    try:
        raw = spark.read.text(args.input)
        parsed = raw.select(F.from_json(F.col("value"), T.MapType(T.StringType(), T.StringType())).alias("r"))

        # Parse JSON strictly enough to reject malformed records while retaining numeric values as strings.
        for field in EXPECTED_FIELDS:
            parsed = parsed.withColumn(field, F.col("r").getItem(field))
        parsed = parsed.drop("r")

        for field in TIMESTAMP_FIELDS:
            parsed = parsed.withColumn(field, F.to_timestamp(F.col(field), "yyyy-MM-dd HH:mm:ss"))
        parsed = parsed.withColumn("distance_km", F.col("distance_km").cast("double"))
        parsed = parsed.withColumn("order_amount", F.col("order_amount").cast("double"))
        parsed = parsed.withColumn("delivery_fee", F.col("delivery_fee").cast("double"))

        valid_condition = F.lit(True)
        for field in EXPECTED_FIELDS:
            valid_condition = valid_condition & F.col(field).isNotNull()
        valid = parsed.where(valid_condition)
        valid = valid.where((F.col("distance_km") > 0) & (F.col("order_amount") > 0))
        valid = valid.where(F.col("status") == F.lit("DELIVERED"))
        valid = valid.dropDuplicates()

        valid = (
            valid
            .withColumn("preparation_time_min", (F.col("picked_up_timestamp").cast("long") - F.col("accepted_timestamp").cast("long")) / 60.0)
            .withColumn("delivery_time_min", (F.col("delivered_timestamp").cast("long") - F.col("picked_up_timestamp").cast("long")) / 60.0)
            .withColumn("fulfillment_time_min", (F.col("delivered_timestamp").cast("long") - F.col("order_timestamp").cast("long")) / 60.0)
            .withColumn("delivery_speed_kmph", F.col("distance_km") / (F.col("delivery_time_min") / 60.0))
            .withColumn("delay_min", F.greatest(F.lit(0.0), (F.col("delivered_timestamp").cast("long") - F.col("expected_delivery_timestamp").cast("long")) / 60.0))
            .withColumn("delivery_status", F.when(F.col("delay_min") == 0, "ON_TIME").otherwise("DELAYED"))
            .withColumn(
                "delay_category",
                F.when(F.col("delay_min") == 0, "ON_TIME")
                .when(F.col("delay_min") <= 15, "SLIGHT_DELAY")
                .when(F.col("delay_min") <= 30, "MODERATE_DELAY")
                .otherwise("SEVERE_DELAY"),
            )
        )

        output_columns = [
            "order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
            "order_amount", "delivery_fee", "distance_km", "preparation_time_min",
            "delivery_time_min", "fulfillment_time_min", "delivery_speed_kmph",
            "delay_min", "delivery_status", "delay_category",
        ]
        result = valid.select(*output_columns)
        result = result.select(
            *[F.round(F.col(c), 2).alias(c) if c not in {"order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city", "delivery_status", "delay_category"} else F.col(c) for c in output_columns]
        )

        # Spark JSONL output is normally a directory. Coalesce to one part so the required artifact is easy to consume.
        temp_dir = args.output + ".spark_tmp"
        result.coalesce(1).write.mode("overwrite").json(temp_dir)
        import glob, os, shutil
        part = glob.glob(os.path.join(temp_dir, "part-*.json"))[0]
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        shutil.copyfile(part, args.output)
        shutil.rmtree(temp_dir)
        print(f"Wrote {result.count()} enriched records to {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
