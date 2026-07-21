-- TradeWatch — Apache Hive schema over the HDFS data lake.
--
-- Hive gives the lake a SQL warehouse surface: analysts query trades and
-- anomalies with plain SQL, and Spark reads/writes the same tables via
-- `SparkSession.builder.enableHiveSupport()`. Tables are EXTERNAL so the data
-- lifecycle is owned by the lake (HDFS), not the metastore.
--
-- Apply with:  hive -f warehouse/hive/schema.hql
--          or  spark-sql -f warehouse/hive/schema.hql

CREATE DATABASE IF NOT EXISTS tradewatch
  COMMENT 'Trade surveillance lakehouse'
  LOCATION 'hdfs:///tradewatch/warehouse';

USE tradewatch;

-- Raw trade tape landed from Kafka (one JSON object per line).
CREATE EXTERNAL TABLE IF NOT EXISTS trades_raw (
  trade_id        STRING,
  `timestamp`     TIMESTAMP,
  symbol          STRING,
  price           DOUBLE,
  quantity        DOUBLE,
  side            STRING,
  account_id      STRING,
  counterparty_id STRING,
  venue           STRING,
  currency        STRING
)
PARTITIONED BY (dt STRING)                    -- ingestion date, e.g. 2026-07-21
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs:///tradewatch/trades';

-- Curated, columnar trades (Parquet) written by the Spark batch job.
CREATE EXTERNAL TABLE IF NOT EXISTS trades (
  trade_id   STRING,
  `timestamp` TIMESTAMP,
  symbol     STRING,
  price      DOUBLE,
  quantity   DOUBLE,
  side       STRING,
  venue      STRING,
  notional   DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs:///tradewatch/curated/trades';

-- Anomalies flagged by the Spark backtest / MapReduce jobs.
CREATE EXTERNAL TABLE IF NOT EXISTS anomalies (
  `timestamp` TIMESTAMP,
  symbol     STRING,
  detector   STRING,
  severity   STRING,
  price      DOUBLE,
  quantity   DOUBLE,
  score      DOUBLE,
  reason     STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs:///tradewatch/anomalies';

-- Re-scan partitions after new data lands.
MSCK REPAIR TABLE trades_raw;
MSCK REPAIR TABLE anomalies;

-- ---------------------------------------------------------------------------
-- Example analytics served straight from Hive
-- ---------------------------------------------------------------------------

-- Daily anomaly counts by detector.
-- SELECT dt, detector, COUNT(*) AS n
-- FROM anomalies GROUP BY dt, detector ORDER BY dt, n DESC;

-- Top symbols by critical alerts in the last day.
-- SELECT symbol, COUNT(*) AS critical
-- FROM anomalies WHERE severity = 'critical' AND dt = '2026-07-21'
-- GROUP BY symbol ORDER BY critical DESC LIMIT 20;
