"""Verify the Hadoop Streaming mapper/reducer.

Hadoop Streaming runs `mapper | shuffle-sort | reducer`, so we reproduce that
with subprocess pipes (`sort` stands in for the shuffle). This exercises the
exact code the cluster runs, with no Hadoop install required — so the MapReduce
job is covered by CI like everything else.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

_HADOOP = Path(__file__).resolve().parent.parent / "hadoop"


def _run_pipeline(trades: list[dict]) -> list[list[str]]:
    payload = "\n".join(json.dumps(t) for t in trades).encode()

    mapper = subprocess.run(
        [sys.executable, str(_HADOOP / "mapper.py")], input=payload, capture_output=True, check=True
    )
    ordered = subprocess.run(["sort", "-k1,1"], input=mapper.stdout, capture_output=True, check=True)
    reducer = subprocess.run(
        [sys.executable, str(_HADOOP / "reducer.py")], input=ordered.stdout, capture_output=True, check=True
    )
    return [line.split("\t") for line in reducer.stdout.decode().splitlines() if line]


def test_mapper_emits_symbol_keyed_records():
    trades = [{"symbol": "AAPL", "price": 100.0, "quantity": 10.0}, {"symbol": "bad"}]  # 2nd is malformed
    out = subprocess.run(
        [sys.executable, str(_HADOOP / "mapper.py")],
        input=("\n".join(json.dumps(t) for t in trades)).encode(),
        capture_output=True,
        check=True,
    )
    lines = out.stdout.decode().splitlines()
    assert lines == ["AAPL\t100.0\t10.0"]  # malformed record skipped


def test_reducer_flags_price_and_volume_anomalies():
    # 40 normal trades around 100, then a clear price outlier and a volume block.
    trades = [{"symbol": "AAPL", "price": 100.0 + (i % 3 - 1) * 0.5, "quantity": 100.0} for i in range(40)]
    trades.append({"symbol": "AAPL", "price": 130.0, "quantity": 100.0})   # price z-score
    trades.append({"symbol": "AAPL", "price": 100.0, "quantity": 900.0})   # volume spike

    rows = _run_pipeline(trades)
    detectors = {r[1] for r in rows}
    assert "zscore" in detectors
    assert "volume_spike" in detectors
    assert all(r[0] == "AAPL" for r in rows)


def test_reducer_silent_below_min_trades():
    # Fewer than MIN_TRADES observations -> no verdict emitted.
    trades = [{"symbol": "XYZ", "price": 10.0, "quantity": 5.0} for _ in range(5)]
    trades.append({"symbol": "XYZ", "price": 99.0, "quantity": 5.0})
    assert _run_pipeline(trades) == []
