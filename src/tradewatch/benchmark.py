"""Per-event latency benchmark for the detection engine.

Measures wall-clock time of ``DetectionEngine.process`` per trade and reports
the latency distribution and single-core throughput. This is what backs the
"sub-200ms per-event flagging latency" performance target: the engine decides on
each trade in well under a millisecond (rule core) to a few milliseconds (with
the online Isolation Forest).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from .config import DetectionConfig
from .engine import DetectionEngine
from .sources import MarketSimulator


@dataclass
class BenchReport:
    label: str
    events: int
    p50_us: float
    p95_us: float
    p99_us: float
    max_us: float
    mean_us: float

    @property
    def throughput(self) -> float:
        return 1_000_000.0 / self.mean_us if self.mean_us else 0.0

    def print(self) -> None:
        print(f"  {self.label}")
        print(f"    events      : {self.events:,}")
        print(f"    p50 latency : {self.p50_us:8.1f} µs")
        print(f"    p95 latency : {self.p95_us:8.1f} µs")
        print(f"    p99 latency : {self.p99_us:8.1f} µs")
        print(f"    max latency : {self.max_us / 1000:8.2f} ms")
        print(f"    throughput  : {self.throughput:,.0f} events/sec (single core)")
        print(f"    sub-200ms   : {'PASS' if self.p99_us < 200_000 else 'FAIL'} (p99)")


def benchmark(trades: int = 40000, warmup: int = 8000, seed: int = 3, include_ml: bool = True) -> BenchReport:
    cfg = DetectionConfig()
    if not include_ml:
        cfg.isolation_forest.enabled = False
    engine = DetectionEngine(cfg)
    sim = MarketSimulator(
        symbols=["AAPL", "MSFT", "BTC-USD", "ETH-USD", "TSLA"],
        anomaly_rate=0.02,
        seed=seed,
    )
    batch = sim.labeled_batch(trades + warmup)

    for trade, _ in batch[:warmup]:
        engine.process(trade)

    latencies: list[float] = []
    for trade, _ in batch[warmup:]:
        start = time.perf_counter_ns()
        engine.process(trade)
        latencies.append((time.perf_counter_ns() - start) / 1000.0)  # µs

    latencies.sort()
    n = len(latencies)

    def pct(p: float) -> float:
        return latencies[min(n - 1, int(p * n))]

    return BenchReport(
        label="full engine (with online Isolation Forest)" if include_ml else "rule-core (statistical detectors only)",
        events=n,
        p50_us=pct(0.50),
        p95_us=pct(0.95),
        p99_us=pct(0.99),
        max_us=latencies[-1],
        mean_us=sum(latencies) / n,
    )


def run_all(trades: int = 40000) -> None:
    print("=" * 60)
    print("  TradeWatch — per-event latency benchmark")
    print("=" * 60)
    benchmark(trades=trades, include_ml=False).print()
    print("-" * 60)
    benchmark(trades=min(trades, 15000), include_ml=True).print()
    print("=" * 60)
