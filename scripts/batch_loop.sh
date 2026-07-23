#!/usr/bin/env bash
# Continuous batch/scale layer for the integrated stack.
#
# Every INTERVAL seconds: generate a fresh history slice, run the Spark batch
# backtest and the Hadoop MapReduce job over it, and write anomalies to the
# shared data volume. This makes the batch layer *visibly running alongside*
# the real-time engine — the whole platform live at once.
set -u
INTERVAL="${BATCH_INTERVAL:-300}"
DATA="${DATA_DIR:-/data}"
TRADES="${BATCH_TRADES:-20000}"
mkdir -p "$DATA"

echo "[batch] starting — interval ${INTERVAL}s, ${TRADES} trades/cycle, out=${DATA}"
cycle=0
while true; do
  cycle=$((cycle + 1))
  seed=$((RANDOM))
  echo "[batch] cycle ${cycle} (seed ${seed}) $(date -u +%H:%M:%S)"

  python examples/generate_history.py --out "$DATA/trades.parquet" --trades "$TRADES" --seed "$seed" || true
  python examples/generate_history.py --out "$DATA/trades.jsonl" --format json --trades "$TRADES" --seed "$seed" || true

  echo "[batch] spark backtest…"
  python spark/batch_backtest.py --input "$DATA/trades.parquet" --output "$DATA/anomalies.parquet" || true

  echo "[batch] hadoop mapreduce…"
  bash hadoop/run_local.sh "$DATA/trades.jsonl" > "$DATA/mr_anomalies.tsv" 2>/dev/null || true
  mr=$(wc -l < "$DATA/mr_anomalies.tsv" 2>/dev/null || echo 0)
  echo "[batch] mapreduce flagged ${mr} anomalies"

  # Heartbeat for the dashboard Platform health board.
  printf '{"epoch": %s, "last_cycle": "%s", "cycle": %s, "seed": %s, "trades": %s, "mr_anomalies": %s}\n' \
    "$(date -u +%s)" "$(date -u +%FT%TZ)" "$cycle" "$seed" "$TRADES" "$mr" > "$DATA/batch_status.json" || true

  # Optional: load the gold layer into Snowflake when credentials are provided.
  if [ -n "${SNOWFLAKE_ACCOUNT:-}" ]; then
    echo "[batch] loading Snowflake gold layer…"
    python warehouse/snowflake/load_snowflake.py --input "$DATA/anomalies.parquet" --dt "$(date -u +%F)" || true
  fi

  echo "[batch] cycle ${cycle} done; sleeping ${INTERVAL}s"
  sleep "$INTERVAL"
done
