# Hadoop layer — HDFS data lake + MapReduce batch detection

This directory is the **Hadoop** part of the big-data stack. It provides:

- a **Hadoop MapReduce** job (via **Hadoop Streaming**, Python mapper/reducer)
  that performs per-symbol statistical anomaly detection over the trade history;
- the pattern for using **HDFS** as the durable data lake that the Spark jobs and
  this MapReduce job read from and write to.

Where each engine fits:

| Engine | Role | Latency |
|---|---|---|
| FastAPI detection engine | real-time scoring of the live tape | sub-10ms/event |
| Apache Spark | distributed batch + Structured Streaming | seconds–minutes |
| **Hadoop MapReduce** | **massive batch scans of the HDFS data lake** | minutes+ |

## Files

- `mapper.py` — emits `symbol \t price \t quantity` per trade (key = symbol).
- `reducer.py` — per-symbol price z-score + volume-ratio detection; emits
  `symbol \t detector \t price \t quantity \t score`.
- `run_local.sh` — runs the job with Unix pipes (no cluster needed).
- `run_streaming.sh` — runs it on a real cluster via `hadoop-streaming.jar` over HDFS.

## Try it locally (no Hadoop required)

`mapper | sort | reducer` is exactly what Hadoop Streaming executes, so the same
code runs in a plain pipe:

```bash
python examples/generate_history.py --out data/trades.jsonl --format json --trades 100000
hadoop/run_local.sh data/trades.jsonl
```

## Run on a Hadoop cluster (HDFS + YARN)

```bash
# stage the data lake
hdfs dfs -mkdir -p /tradewatch/trades
hdfs dfs -put data/trades.jsonl /tradewatch/trades/

# MapReduce over HDFS
hadoop/run_streaming.sh /tradewatch/trades /tradewatch/anomalies

# ...and Spark can read/write the same HDFS paths:
spark-submit spark/batch_backtest.py \
  --input hdfs://namenode:9000/tradewatch/trades \
  --output hdfs://namenode:9000/tradewatch/spark-anomalies
```

Spin up a local HDFS with the compose profile:

```bash
docker compose --profile hadoop up   # NameNode UI on http://localhost:9870
```

Thresholds are configurable via env: `PRICE_Z_THRESHOLD`, `VOL_RATIO_THRESHOLD`,
`MIN_TRADES`.
