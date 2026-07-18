"""Spark Structured Streaming — Kafka → Spark → alerts.

The distributed, horizontally-scalable counterpart to the FastAPI engine: it
consumes the same ``trades`` Kafka topic, applies the statistical detectors as
stateful stream operations, and writes anomalies to the console (swap the sink
for Parquet, Delta, another Kafka topic, or a database).

Run (Spark fetches the Kafka connector via --packages):
    pip install -e ".[spark]"
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      spark/streaming_job.py --bootstrap localhost:9092 --topic trades

Design note: this uses event-time tumbling windows with a watermark for the
z-score/volume baselines, which is the idiomatic Structured Streaming way to
bound state. It intentionally mirrors the rules in ``detection_sql.py`` so the
batch backtest and the stream agree.
"""

from __future__ import annotations

import argparse

from detection_sql import TRADE_SCHEMA
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    p = argparse.ArgumentParser(description="Spark Structured Streaming anomaly detection over Kafka")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="trades")
    p.add_argument("--starting-offsets", default="latest")
    p.add_argument("--window", default="30 seconds", help="baseline aggregation window")
    p.add_argument("--slide", default="5 seconds")
    p.add_argument("--price-z", type=float, default=3.0)
    p.add_argument("--max-trades", type=int, default=200, help="velocity cap per window")
    args = p.parse_args()

    spark = SparkSession.builder.appName("tradewatch-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    trades = (
        raw.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"))
        .select("t.*")
        .withWatermark("timestamp", "1 minute")
    )

    # Per-symbol sliding-window baselines (mean/std price, mean size, count).
    baselines = trades.groupBy(
        F.window("timestamp", args.window, args.slide), F.col("symbol")
    ).agg(
        F.avg("price").alias("price_mean"),
        F.stddev("price").alias("price_std"),
        F.avg("quantity").alias("vol_mean"),
        F.max("price").alias("price_max"),
        F.count("*").alias("trade_count"),
    )

    # Flag windows whose extreme print breaches the z-score / volume thresholds.
    flagged = (
        baselines.withColumn(
            "price_z_max",
            F.when(
                F.col("price_std") > 0, (F.col("price_max") - F.col("price_mean")) / F.col("price_std")
            ).otherwise(F.lit(0.0)),
        )
        .filter(
            (F.abs(F.col("price_z_max")) >= args.price_z) | (F.col("trade_count") >= args.max_trades)
        )
        .withColumn(
            "detector",
            F.when(F.abs(F.col("price_z_max")) >= args.price_z, F.lit("zscore")).otherwise(F.lit("velocity")),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "detector",
            F.round("price_z_max", 2).alias("price_z_max"),
            "trade_count",
            F.round("price_mean", 4).alias("price_mean"),
        )
    )

    query = (
        flagged.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
