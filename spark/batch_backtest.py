"""PySpark batch backtest — the scale layer over historical trade data.

Reads a historical trade dataset (Parquet or newline-delimited JSON), applies
the Spark-native statistical detectors across the whole history at once, writes
the flagged anomalies back out, and prints a summary. Use it to backtest
thresholds on months of data, or to bootstrap per-symbol baselines that seed the
real-time engine.

Run locally:
    pip install -e ".[spark]"
    python examples/generate_history.py --out data/trades.parquet --trades 100000
    python spark/batch_backtest.py --input data/trades.parquet --output data/anomalies.parquet
"""

from __future__ import annotations

import argparse

from detection_sql import add_statistical_features, flag_anomalies
from pyspark.sql import SparkSession


def main() -> None:
    p = argparse.ArgumentParser(description="Batch backtest of statistical detectors with Spark")
    p.add_argument("--input", required=True, help="Parquet or JSON path of historical trades")
    p.add_argument("--output", default=None, help="Where to write flagged anomalies (Parquet)")
    p.add_argument("--format", default="parquet", choices=["parquet", "json"])
    p.add_argument("--lookback", type=int, default=100)
    p.add_argument("--price-z", type=float, default=3.0)
    p.add_argument("--vol-ratio", type=float, default=6.0)
    p.add_argument("--velocity", type=int, default=25)
    args = p.parse_args()

    spark = (
        SparkSession.builder.appName("tradewatch-batch-backtest")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    trades = spark.read.format(args.format).load(args.input)
    total = trades.count()

    featured = add_statistical_features(trades, lookback=args.lookback)
    flagged = flag_anomalies(
        featured,
        price_z_threshold=args.price_z,
        vol_ratio_threshold=args.vol_ratio,
        velocity_threshold=args.velocity,
    )
    anomalies = flagged.filter("anomalous").select(
        "timestamp", "symbol", "price", "quantity", "detector", "severity", "reason"
    )

    n_anom = anomalies.count()
    print("=" * 60)
    print("  TradeWatch — Spark batch backtest")
    print("=" * 60)
    print(f"  trades scanned   : {total:,}")
    print(f"  anomalies flagged: {n_anom:,}  ({(100 * n_anom / total if total else 0):.2f}%)")
    print("-" * 60)
    print("  by detector:")
    anomalies.groupBy("detector").count().orderBy("count", ascending=False).show(truncate=False)
    print("  sample anomalies:")
    anomalies.orderBy("timestamp").show(10, truncate=False)

    if args.output:
        anomalies.write.mode("overwrite").parquet(args.output)
        print(f"  wrote flagged anomalies → {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
