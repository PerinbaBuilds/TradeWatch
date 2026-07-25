#!/usr/bin/env bash
# Run TradeWatch locally WITHOUT Docker (macOS / Linux).
# Starts the batch runner + the dashboard together, sharing a data dir so the
# Platform page shows the API up and the batch layer executing.
set -euo pipefail
cd "$(dirname "$0")/.."

export TRADEWATCH_DATA_DIR="$(pwd)/data"
mkdir -p "$TRADEWATCH_DATA_DIR"

echo "Starting the batch runner (Spark backtest + Hadoop MapReduce) in the background…"
python scripts/batch_runner.py --interval 180 &
BATCH_PID=$!
trap 'kill "$BATCH_PID" 2>/dev/null || true' EXIT

echo "Starting the dashboard on http://localhost:8000 …"
echo "Open the 'Platform' page to watch health + batch executions."
tradewatch serve
