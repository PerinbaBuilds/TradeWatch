#!/usr/bin/env python3
"""Hadoop Streaming mapper.

Reads newline-delimited JSON trades on stdin (as written by
``examples/generate_history.py --format json``, i.e. one Trade per line) and
emits a tab-separated ``symbol \\t price \\t quantity`` record.

The map output key is ``symbol`` (everything before the first tab), so Hadoop's
shuffle groups every trade for a symbol at the same reducer — exactly what the
per-symbol statistical detection in ``reducer.py`` needs.

Runs unchanged under `hadoop jar hadoop-streaming.jar -mapper "python3 mapper.py"`
or, for local testing, in a plain Unix pipe.
"""

from __future__ import annotations

import json
import sys


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            trade = json.loads(line)
            symbol = str(trade["symbol"])
            price = float(trade["price"])
            quantity = float(trade["quantity"])
        except (ValueError, KeyError, TypeError):
            # Skip malformed records rather than fail the whole task.
            continue
        sys.stdout.write(f"{symbol}\t{price}\t{quantity}\n")


if __name__ == "__main__":
    main()
