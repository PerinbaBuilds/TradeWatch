#!/usr/bin/env python3
"""Async load / stress test for the TradeWatch API.

Fires a configurable number of concurrent `POST /trades` requests and reports
achieved throughput and client-observed latency percentiles — the kind of number
you quote when someone asks "how much can it take?".

Usage
-----
    # in one terminal
    tradewatch serve --no-simulator

    # in another
    python scripts/loadtest.py --requests 5000 --concurrency 50 --url http://localhost:8000

Requires the `httpx` client (installed with the `[dev]` extra).
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time

import httpx

SYMBOLS = ["AAPL", "MSFT", "BTC-USD", "ETH-USD", "TSLA"]


def _rand_trade() -> dict:
    return {
        "symbol": random.choice(SYMBOLS),
        "price": round(random.uniform(50, 65000), 2),
        "quantity": round(random.uniform(1, 500), 2),
        "side": random.choice(["buy", "sell"]),
    }


async def _worker(client: httpx.AsyncClient, url: str, api_key: str | None,
                  queue: asyncio.Queue, latencies: list[float], errors: list[int]) -> None:
    headers = {"X-API-Key": api_key} if api_key else {}
    while True:
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        t0 = time.perf_counter()
        try:
            r = await client.post(f"{url}/trades", json=_rand_trade(), headers=headers, timeout=10.0)
            latencies.append((time.perf_counter() - t0) * 1000.0)  # ms
            if r.status_code >= 400:
                errors.append(r.status_code)
        except Exception:
            errors.append(-1)
        finally:
            queue.task_done()


def _pct(sorted_ms: list[float], p: float) -> float:
    if not sorted_ms:
        return 0.0
    return sorted_ms[min(len(sorted_ms) - 1, int(p * len(sorted_ms)))]


async def run(url: str, total: int, concurrency: int, api_key: str | None) -> int:
    queue: asyncio.Queue = asyncio.Queue()
    for _ in range(total):
        queue.put_nowait(1)
    latencies: list[float] = []
    errors: list[int] = []

    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    t0 = time.perf_counter()
    async with httpx.AsyncClient(limits=limits) as client:
        workers = [
            asyncio.create_task(_worker(client, url, api_key, queue, latencies, errors))
            for _ in range(concurrency)
        ]
        await asyncio.gather(*workers)
    wall = time.perf_counter() - t0

    ok = len(latencies)
    lat = sorted(latencies)
    print("=" * 56)
    print("  TradeWatch — load test")
    print("=" * 56)
    print(f"  target        : {url}")
    print(f"  requests      : {total}  (concurrency {concurrency})")
    print(f"  succeeded     : {ok}   failed: {len(errors)}")
    print(f"  wall time     : {wall:.2f}s")
    print(f"  throughput    : {ok / wall:,.0f} req/s")
    print("-" * 56)
    print(f"  latency p50   : {_pct(lat, 0.50):.2f} ms")
    print(f"  latency p95   : {_pct(lat, 0.95):.2f} ms")
    print(f"  latency p99   : {_pct(lat, 0.99):.2f} ms")
    print(f"  latency max   : {(lat[-1] if lat else 0):.2f} ms")
    print("=" * 56)
    return 0 if not errors else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load test the TradeWatch API")
    p.add_argument("--url", default="http://localhost:8000")
    p.add_argument("--requests", type=int, default=5000)
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--api-key", default=None)
    args = p.parse_args(argv)
    return asyncio.run(run(args.url, args.requests, args.concurrency, args.api_key))


if __name__ == "__main__":
    raise SystemExit(main())
