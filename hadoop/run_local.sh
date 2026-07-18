#!/usr/bin/env bash
# Run the MapReduce job locally with Unix pipes.
#
# Hadoop Streaming executes exactly this: mapper -> (shuffle/sort by key) ->
# reducer. `sort -k1,1` stands in for the shuffle, so this runs the *same*
# mapper/reducer code the cluster would, with no Hadoop install required.
#
#   python examples/generate_history.py --out data/trades.jsonl --format json --trades 100000
#   hadoop/run_local.sh data/trades.jsonl
set -euo pipefail

INPUT="${1:-data/trades.jsonl}"
HERE="$(cd "$(dirname "$0")" && pwd)"

if [[ ! -f "$INPUT" ]]; then
  echo "input not found: $INPUT" >&2
  echo "generate one with: python examples/generate_history.py --out data/trades.jsonl --format json" >&2
  exit 1
fi

cat "$INPUT" \
  | python3 "$HERE/mapper.py" \
  | sort -k1,1 \
  | python3 "$HERE/reducer.py"
