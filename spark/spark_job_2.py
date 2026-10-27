#!/usr/bin/env python3
import argparse
import os
import shutil
import glob
from pyspark.sql import SparkSession, functions as F


def parse_args():
    p = argparse.ArgumentParser(description="Spark Job 2: restaurant and city analytics.")
    p.add_argument("--input", required=True)
    p.add_argument("--output-dir", required=True)
    return p.parse_args()


def write_single_csv(df, path):
    temp_dir = path + ".spark_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_dir)
    part = glob.glob(os.path.join(temp_dir, "part-*.csv"))[0]
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    shutil.copyfile(part, path)
    shutil.rmtree(temp_dir)


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("A2-Spark-Job-2").getOrCreate()
    try:
        df = spark.read.json(args.input).dropDuplicates()

        restaurant = (
            df.groupBy("restaurant_id")
            .agg(
                F.count("order_id").alias("order_count"),
                F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
                F.round(F.avg("preparation_time_min"), 2).alias("avg_preparation_time_min"),
                F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_time_min"),
                F.sum(F.when(F.col("delivery_status") == "DELAYED", 1).otherwise(0)).alias("delayed_order_count"),
            )
            .withColumn("delay_rate_pct", F.round(100.0 * F.col("delayed_order_count") / F.col("order_count"), 2))
            .select(
                "restaurant_id", "order_count", "avg_order_amount",
                "avg_preparation_time_min", "avg_delivery_time_min",
                "delayed_order_count", "delay_rate_pct",
            )
            .orderBy("restaurant_id")
        )

        city = (
            df.groupBy("city")
            .agg(
                F.count("order_id").alias("order_count"),
                F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
                F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_time_min"),
                F.sum(F.when(F.col("delivery_status") == "DELAYED", 1).otherwise(0)).alias("delayed_order_count"),
            )
            .withColumn("delay_rate_pct", F.round(100.0 * F.col("delayed_order_count") / F.col("order_count"), 2))
            .select("city", "order_count", "avg_order_amount", "avg_delivery_time_min", "delayed_order_count", "delay_rate_pct")
            .orderBy("city")
        )

        write_single_csv(restaurant, os.path.join(args.output_dir, "restaurant_summary.csv"))
        write_single_csv(city, os.path.join(args.output_dir, "city_summary.csv"))
        print(f"Wrote analytics to {args.output_dir}/")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
