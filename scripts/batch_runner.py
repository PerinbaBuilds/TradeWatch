"""Cross-platform batch/scale runner (no Docker, no bash required).

Runs the batch layer as a plain Python process so it works on Windows, macOS and
Linux: each cycle it generates a history slice, runs the Spark backtest **if**
Spark is usable (skips gracefully otherwise), runs the Hadoop MapReduce job via
pure-Python pipes, and writes the heartbeat the dashboard's Platform page reads.

    python scripts/batch_runner.py                 # loop forever
    python scripts/batch_runner.py --once           # single cycle (CI/tests)
    python scripts/batch_runner.py --interval 120    # custom cadence

Data dir defaults to ./data (override with TRADEWATCH_DATA_DIR) — point the API
at the same dir so the Platform page shows the batch layer executing:
    set TRADEWATCH_DATA_DIR=./data    (Windows)
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = Path(os.environ.get("TRADEWATCH_DATA_DIR", str(ROOT / "data")))
PY = sys.executable


def _gen(out: Path, fmt: str, trades: int, seed: int) -> bool:
    cmd = [PY, str(ROOT / "examples" / "generate_history.py"), "--out", str(out),
           "--trades", str(trades), "--seed", str(seed)]
    if fmt == "json":
        cmd += ["--format", "json"]
    return subprocess.run(cmd, cwd=ROOT).returncode == 0


def _spark_backtest(parquet: Path) -> bool:
    """Run the Spark backtest; returns False (and is skipped) if Spark can't run."""
    try:
        r = subprocess.run(
            [PY, str(ROOT / "spark" / "batch_backtest.py"), "--input", str(parquet),
             "--output", str(DATA / "anomalies.parquet")],
            cwd=ROOT, timeout=300, capture_output=True,
        )
        return r.returncode == 0
    except Exception:
        return False


def _mapreduce(jsonl: Path) -> int:
    """Run mapper | (python sort) | reducer — the same code Hadoop Streaming runs."""
    with jsonl.open("rb") as fh:
        mapper = subprocess.run([PY, str(ROOT / "hadoop" / "mapper.py")], stdin=fh, capture_output=True)
    # Sorting whole lines groups records by their leading symbol key (the shuffle).
    ordered = "\n".join(sorted(mapper.stdout.decode().splitlines()))
    reducer = subprocess.run([PY, str(ROOT / "hadoop" / "reducer.py")], input=ordered.encode(), capture_output=True)
    return len([ln for ln in reducer.stdout.decode().splitlines() if ln])


def run_cycle(cycle: int, trades: int) -> dict:
    DATA.mkdir(parents=True, exist_ok=True)
    seed = random.randint(1, 1_000_000)
    print(f"[batch] cycle {cycle} (seed {seed})", flush=True)

    jsonl = DATA / "trades.jsonl"
    _gen(jsonl, "json", trades, seed)

    spark_ok = False
    parquet = DATA / "trades.parquet"
    if _gen(parquet, "parquet", trades, seed):   # needs pandas/pyarrow
        print("[batch] spark backtest…", flush=True)
        spark_ok = _spark_backtest(parquet)
    if not spark_ok:
        print("[batch] spark step skipped (pandas/Java/Spark not available) — continuing", flush=True)

    print("[batch] hadoop mapreduce…", flush=True)
    mr = _mapreduce(jsonl)
    print(f"[batch] mapreduce flagged {mr} anomalies", flush=True)

    hb = {
        "epoch": int(time.time()),
        "last_cycle": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cycle": cycle,
        "seed": seed,
        "trades": trades,
        "mr_anomalies": mr,
        "spark": spark_ok,
    }
    (DATA / "batch_status.json").write_text(json.dumps(hb))
    return hb


def main() -> None:
    p = argparse.ArgumentParser(description="Cross-platform batch/scale runner")
    p.add_argument("--interval", type=int, default=int(os.environ.get("BATCH_INTERVAL", "300")))
    p.add_argument("--trades", type=int, default=int(os.environ.get("BATCH_TRADES", "20000")))
    p.add_argument("--once", action="store_true", help="run a single cycle and exit")
    args = p.parse_args()

    print(f"[batch] runner started — data={DATA}, interval={args.interval}s, trades={args.trades}", flush=True)
    cycle = 0
    while True:
        cycle += 1
        try:
            run_cycle(cycle, args.trades)
        except Exception as exc:  # never let one cycle kill the runner
            print(f"[batch] cycle error: {exc}", flush=True)
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
