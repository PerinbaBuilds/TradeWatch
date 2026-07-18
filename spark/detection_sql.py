"""Spark-native detection logic, shared by the batch and streaming jobs.

The FastAPI engine is the low-latency *speed layer*. This module is the
*batch/scale layer*: it expresses the statistical detectors (price z-score,
volume ratio, trade velocity) as Spark SQL window operations so they run over
huge historical datasets or high-throughput streams on a cluster.

Keeping the logic in one place means the offline backtest and the Structured
Streaming job apply exactly the same rules.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

# JSON schema of a trade message (matches tradewatch.models.Trade).
TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", DoubleType()),
        StructField("side", StringType()),
        StructField("account_id", StringType()),
        StructField("counterparty_id", StringType()),
        StructField("venue", StringType()),
        StructField("currency", StringType()),
    ]
)


def add_statistical_features(
    df: DataFrame,
    lookback: int = 100,
    velocity_seconds: int = 5,
) -> DataFrame:
    """Add per-symbol rolling features using event-time window functions.

    * ``price_z``   — z-score of price over the ``lookback`` preceding trades
    * ``vol_ratio`` — quantity / rolling-mean quantity
    * ``velocity``  — trades for the symbol within the last ``velocity_seconds``
    """
    df = df.withColumn("ts_seconds", F.col("timestamp").cast("long"))

    # Row-based window over the preceding N trades for the symbol.
    prior = Window.partitionBy("symbol").orderBy("ts_seconds").rowsBetween(-lookback, -1)
    # Time-range window (seconds) for velocity.
    recent = Window.partitionBy("symbol").orderBy("ts_seconds").rangeBetween(-velocity_seconds, 0)

    price_mean = F.avg("price").over(prior)
    price_std = F.stddev("price").over(prior)
    vol_mean = F.avg("quantity").over(prior)

    return (
        df.withColumn("price_mean", price_mean)
        .withColumn("price_std", price_std)
        .withColumn(
            "price_z",
            F.when(F.col("price_std") > 0, (F.col("price") - F.col("price_mean")) / F.col("price_std")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn("vol_ratio", F.when(vol_mean > 0, F.col("quantity") / vol_mean).otherwise(F.lit(0.0)))
        .withColumn("velocity", F.count("*").over(recent))
    )


def flag_anomalies(
    df: DataFrame,
    price_z_threshold: float = 3.0,
    vol_ratio_threshold: float = 6.0,
    velocity_threshold: int = 25,
) -> DataFrame:
    """Reduce the feature columns to a single anomaly verdict + reason + severity."""
    is_price = F.abs(F.col("price_z")) >= price_z_threshold
    is_vol = F.col("vol_ratio") >= vol_ratio_threshold
    is_velocity = F.col("velocity") >= velocity_threshold
    anomalous = is_price | is_vol | is_velocity

    detector = (
        F.when(is_price, F.lit("zscore"))
        .when(is_vol, F.lit("volume_spike"))
        .when(is_velocity, F.lit("velocity"))
        .otherwise(F.lit(None))
    )
    severity = (
        F.when(F.abs(F.col("price_z")) >= price_z_threshold * 1.6, F.lit("critical"))
        .when(anomalous, F.lit("high"))
        .otherwise(F.lit(None))
    )
    reason = (
        F.when(is_price, F.concat(F.lit("price z-score "), F.round("price_z", 2).cast(StringType())))
        .when(is_vol, F.concat(F.lit("volume "), F.round("vol_ratio", 1).cast(StringType()), F.lit("x avg")))
        .when(is_velocity, F.concat(F.col("velocity").cast(StringType()), F.lit(" trades in window")))
        .otherwise(F.lit(None))
    )

    return (
        df.withColumn("anomalous", anomalous)
        .withColumn("detector", detector)
        .withColumn("severity", severity)
        .withColumn("reason", reason)
    )
