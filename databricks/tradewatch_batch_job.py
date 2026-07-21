# Databricks notebook source
# MAGIC %md
# MAGIC # TradeWatch — Databricks batch anomaly detection
# MAGIC
# MAGIC Runs the same statistical detectors as the real-time engine on a
# MAGIC **Databricks** managed-Spark cluster over trade history in cloud storage
# MAGIC (S3 / ADLS / DBFS) and writes flagged anomalies back as Delta/Parquet.
# MAGIC Schedule it as a Databricks Job (see `databricks/job.json`).

# COMMAND ----------
dbutils.widgets.text("input_path", "dbfs:/tradewatch/trades")          # noqa: F821
dbutils.widgets.text("output_path", "dbfs:/tradewatch/anomalies")      # noqa: F821
dbutils.widgets.text("price_z", "3.0")                                 # noqa: F821
dbutils.widgets.text("vol_ratio", "6.0")                               # noqa: F821

INPUT = dbutils.widgets.get("input_path")    # noqa: F821
OUTPUT = dbutils.widgets.get("output_path")  # noqa: F821
PRICE_Z = float(dbutils.widgets.get("price_z"))    # noqa: F821
VOL_RATIO = float(dbutils.widgets.get("vol_ratio"))  # noqa: F821

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F

# `spark` is provided by the Databricks runtime.
trades = spark.read.parquet(INPUT)  # noqa: F821

prior = Window.partitionBy("symbol").orderBy(F.col("timestamp").cast("long")).rowsBetween(-100, -1)
recent = Window.partitionBy("symbol").orderBy(F.col("timestamp").cast("long")).rangeBetween(-5, 0)

feat = (
    trades
    .withColumn("price_mean", F.avg("price").over(prior))
    .withColumn("price_std", F.stddev("price").over(prior))
    .withColumn("vol_mean", F.avg("quantity").over(prior))
    .withColumn("velocity", F.count("*").over(recent))
    .withColumn(
        "price_z",
        F.when(F.col("price_std") > 0, (F.col("price") - F.col("price_mean")) / F.col("price_std")).otherwise(
            F.lit(0.0)
        ),
    )
    .withColumn("vol_ratio", F.when(F.col("vol_mean") > 0, F.col("quantity") / F.col("vol_mean")).otherwise(F.lit(0.0)))
)

anomalies = (
    feat
    .withColumn(
        "detector",
        F.when(F.abs("price_z") >= PRICE_Z, F.lit("zscore"))
         .when(F.col("vol_ratio") >= VOL_RATIO, F.lit("volume_spike"))
         .when(F.col("velocity") >= 25, F.lit("velocity")),
    )
    .withColumn(
        "severity",
        F.when(F.abs("price_z") >= PRICE_Z * 1.6, F.lit("critical")).when(F.col("detector").isNotNull(), F.lit("high")),
    )
    .filter(F.col("detector").isNotNull())
    .select("timestamp", "symbol", "detector", "severity", "price", "quantity", "price_z", "vol_ratio")
)

# COMMAND ----------
display(anomalies.groupBy("detector").count().orderBy(F.desc("count")))  # noqa: F821

# COMMAND ----------
(anomalies.write.mode("overwrite").format("delta").save(OUTPUT))
print(f"wrote {anomalies.count()} anomalies to {OUTPUT}")
