"""Load Spark-produced anomaly Parquet into the Snowflake gold layer.

The tail of the ETL: after Spark writes flagged anomalies to the lake, this
loads them into Snowflake for BI and retention. Credentials come from the
environment — never hard-code them.

    pip install -e ".[snowflake]"
    export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
    python warehouse/snowflake/load_snowflake.py --input data/anomalies.parquet --dt 2026-07-21
"""

from __future__ import annotations

import argparse
import os


def _connect():
    try:
        import snowflake.connector
    except ImportError as exc:  # pragma: no cover - optional dep
        raise SystemExit("snowflake-connector-python required: pip install -e '.[snowflake]'") from exc
    missing = [k for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD") if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"missing Snowflake env vars: {', '.join(missing)}")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRADEWATCH_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "TRADEWATCH"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "GOLD"),
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Load anomaly Parquet into Snowflake")
    p.add_argument("--input", required=True, help="Local Parquet file/dir of anomalies")
    p.add_argument("--dt", required=True, help="Partition date YYYY-MM-DD")
    p.add_argument("--table", default="ANOMALIES")
    args = p.parse_args()

    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("pandas + pyarrow required: pip install -e '.[snowflake]'") from exc
    from snowflake.connector.pandas_tools import write_pandas

    df = pd.read_parquet(args.input)
    df.columns = [c.upper() for c in df.columns]
    if "TIMESTAMP" in df.columns:
        df = df.rename(columns={"TIMESTAMP": "EVENT_TS"})
    df["DT"] = args.dt

    conn = _connect()
    try:
        ok, nchunks, nrows, _ = write_pandas(conn, df, args.table.upper(), quote_identifiers=False)
        print(f"loaded {nrows} rows into {args.table} (success={ok}, chunks={nchunks})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
