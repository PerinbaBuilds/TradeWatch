#!/usr/bin/env python3
"""Hadoop Streaming reducer — per-symbol statistical anomaly detection.

Input arrives grouped by symbol (Hadoop sorts the mapper output by key, so all
records for a symbol are contiguous). For each symbol we compute the price
mean/standard-deviation and mean quantity across its trades, then emit the
trades whose price z-score or volume ratio breaches the thresholds:

    symbol \\t detector \\t price \\t quantity \\t score

Thresholds are read from the environment so they can be set per job:
``PRICE_Z_THRESHOLD`` (default 3.0) and ``VOL_RATIO_THRESHOLD`` (default 6.0).

This is the batch counterpart to the real-time engine's z-score / volume-spike
detectors — the same idea, expressed as a MapReduce job over the HDFS data lake.
"""

from __future__ import annotations

import math
import os
import sys

PRICE_Z = float(os.environ.get("PRICE_Z_THRESHOLD", "3.0"))
VOL_RATIO = float(os.environ.get("VOL_RATIO_THRESHOLD", "6.0"))
MIN_TRADES = int(os.environ.get("MIN_TRADES", "20"))


def flush(symbol: str | None, rows: list[tuple[float, float]]) -> None:
    """Emit anomalies for one symbol's accumulated trades."""
    if symbol is None or len(rows) < MIN_TRADES:
        return
    prices = [p for p, _ in rows]
    n = len(prices)
    mean = sum(prices) / n
    var = sum((p - mean) ** 2 for p in prices) / (n - 1) if n > 1 else 0.0
    std = math.sqrt(var)
    qmean = sum(q for _, q in rows) / n

    for price, qty in rows:
        z = (price - mean) / std if std > 1e-12 else 0.0
        ratio = qty / qmean if qmean > 0 else 0.0
        if abs(z) >= PRICE_Z:
            sys.stdout.write(f"{symbol}\tzscore\t{price:.4f}\t{qty:.2f}\t{z:.2f}\n")
        elif ratio >= VOL_RATIO:
            sys.stdout.write(f"{symbol}\tvolume_spike\t{price:.4f}\t{qty:.2f}\t{ratio:.1f}\n")


def main() -> None:
    current: str | None = None
    rows: list[tuple[float, float]] = []

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        symbol, price_s, qty_s = parts
        try:
            price = float(price_s)
            qty = float(qty_s)
        except ValueError:
            continue
        if symbol != current:
            flush(current, rows)
            current = symbol
            rows = []
        rows.append((price, qty))

    flush(current, rows)


if __name__ == "__main__":
    main()
