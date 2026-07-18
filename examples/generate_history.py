"""Generate a historical trade dataset for the Spark batch backtest.

Writes Parquet (or JSON) using the same simulator that drives the live engine,
so the batch layer and speed layer see identically-shaped data.

    python examples/generate_history.py --out data/trades.parquet --trades 100000
"""

from __future__ import annotations

import argparse
from pathlib import Path

from tradewatch.sources import MarketSimulator


def main() -> None:
    p = argparse.ArgumentParser(description="Generate historical trades")
    p.add_argument("--out", default="data/trades.parquet")
    p.add_argument("--trades", type=int, default=100_000)
    p.add_argument("--anomaly-rate", type=float, default=0.02)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--format", default="parquet", choices=["parquet", "json"])
    args = p.parse_args()

    sim = MarketSimulator(
        symbols=["AAPL", "MSFT", "BTC-USD", "ETH-USD", "TSLA"],
        anomaly_rate=args.anomaly_rate,
        seed=args.seed,
    )
    rows = [t.model_dump(mode="json") for t, _ in sim.labeled_batch(args.trades)]

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    try:
        import pandas as pd
    except ImportError:
        pd = None

    if args.format == "parquet":
        if pd is None:
            raise SystemExit("Parquet output needs pandas + pyarrow (pip install -e '.[spark]').")
        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.to_parquet(out, index=False)
    else:
        import json

        with out.open("w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")

    print(f"wrote {len(rows):,} trades → {out}")


if __name__ == "__main__":
    main()
