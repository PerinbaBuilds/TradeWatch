"""Test the cross-platform batch runner's MapReduce path (no Spark/Java needed)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("batch_runner", _ROOT / "scripts" / "batch_runner.py")
batch_runner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(batch_runner)


def test_mapreduce_pipeline_flags_outlier(tmp_path):
    lines = [json.dumps({"symbol": "AAPL", "price": 100 + (i % 3 - 1) * 0.5, "quantity": 100}) for i in range(40)]
    lines.append(json.dumps({"symbol": "AAPL", "price": 145.0, "quantity": 100}))   # price outlier
    lines.append(json.dumps({"symbol": "AAPL", "price": 100.0, "quantity": 900}))   # volume spike
    jsonl = tmp_path / "trades.jsonl"
    jsonl.write_text("\n".join(lines))

    flagged = batch_runner._mapreduce(jsonl)
    assert flagged >= 1


def test_mapreduce_empty_input(tmp_path):
    jsonl = tmp_path / "empty.jsonl"
    jsonl.write_text("")
    assert batch_runner._mapreduce(jsonl) == 0
