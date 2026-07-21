"""Live metrics collection for the analytics dashboard.

The engine emits alerts; this collector turns the trade + alert stream into the
time-series, per-symbol, latency and breakdown aggregates the dashboard renders.
It keeps bounded ring buffers so memory stays flat regardless of uptime, and all
work is O(1)/O(window) on the hot path.

Everything runs on the single asyncio event loop (background pipeline + the
`POST /trades` handler), so no locking is required.
"""

from __future__ import annotations

import time
from collections import Counter, deque
from typing import Any

from .models import Alert, Trade


class MetricsCollector:
    def __init__(
        self,
        ts_window_seconds: int = 300,
        price_points: int = 120,
        latency_reservoir: int = 4000,
        tape_size: int = 80,
    ) -> None:
        self.start_time = time.time()
        self.ts_window = ts_window_seconds
        self.price_points = price_points

        # Per-second buckets: epoch-second -> count.
        self._ts_trades: dict[int, int] = {}
        self._ts_alerts: dict[int, int] = {}
        self._ts_notional: dict[int, float] = {}

        self._symbols: dict[str, dict[str, Any]] = {}
        self._latency: deque[float] = deque(maxlen=latency_reservoir)  # microseconds
        self._tape: deque[dict[str, Any]] = deque(maxlen=tape_size)

        self.by_detector: Counter[str] = Counter()
        self.by_severity: Counter[str] = Counter()

        self.total_trades = 0
        self.total_alerts = 0
        self.total_notional = 0.0
        self._last_trim = int(self.start_time)

    # ------------------------------------------------------------------ record
    def record(self, trade: Trade, alerts: list[Alert], latency_us: float) -> None:
        now = time.time()
        sec = int(now)

        self._ts_trades[sec] = self._ts_trades.get(sec, 0) + 1
        self._ts_notional[sec] = self._ts_notional.get(sec, 0.0) + trade.notional
        self.total_trades += 1
        self.total_notional += trade.notional
        self._latency.append(latency_us)

        sym = self._symbols.get(trade.symbol)
        if sym is None:
            sym = {
                "symbol": trade.symbol,
                "prices": deque(maxlen=self.price_points),
                "first_price": trade.price,
                "last_price": trade.price,
                "high": trade.price,
                "low": trade.price,
                "trades": 0,
                "alerts": 0,
                "notional": 0.0,
                "buy": 0,
                "sell": 0,
                "last_seen": now,
            }
            self._symbols[trade.symbol] = sym
        sym["trades"] += 1
        sym["last_price"] = trade.price
        sym["high"] = max(sym["high"], trade.price)
        sym["low"] = min(sym["low"], trade.price)
        sym["notional"] += trade.notional
        sym["last_seen"] = now
        sym["prices"].append(round(trade.price, 6))
        if getattr(trade.side, "value", trade.side) == "buy":
            sym["buy"] += 1
        else:
            sym["sell"] += 1

        tape_row = {
            "trade_id": trade.trade_id,
            "symbol": trade.symbol,
            "price": trade.price,
            "quantity": trade.quantity,
            "side": getattr(trade.side, "value", trade.side),
            "notional": round(trade.notional, 2),
            "anomalous": bool(alerts),
            "ts": trade.timestamp.isoformat(),
        }
        self._tape.appendleft(tape_row)

        if alerts:
            self._ts_alerts[sec] = self._ts_alerts.get(sec, 0) + len(alerts)
            self.total_alerts += len(alerts)
            sym["alerts"] += len(alerts)
            for a in alerts:
                self.by_detector[a.detector] += 1
                self.by_severity[a.severity.value] += 1

        if sec != self._last_trim:
            self._trim(sec)
            self._last_trim = sec

    def _trim(self, sec: int) -> None:
        cutoff = sec - self.ts_window
        for d in (self._ts_trades, self._ts_alerts, self._ts_notional):
            for k in [k for k in d if k < cutoff]:
                del d[k]

    # ---------------------------------------------------------------- snapshots
    def timeseries(self, window: int | None = None) -> list[dict[str, Any]]:
        window = window or self.ts_window
        now = int(time.time())
        out = []
        for s in range(now - window + 1, now + 1):
            out.append(
                {
                    "t": s,
                    "trades": self._ts_trades.get(s, 0),
                    "alerts": self._ts_alerts.get(s, 0),
                    "notional": round(self._ts_notional.get(s, 0.0), 2),
                }
            )
        return out

    def latency_stats(self) -> dict[str, Any]:
        data = sorted(self._latency)
        n = len(data)
        if n == 0:
            return {"count": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0, "mean": 0, "histogram": []}

        def pct(p: float) -> float:
            return data[min(n - 1, int(p * n))]

        # Log-spaced histogram buckets (microseconds).
        edges = [0, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 1e12]
        labels = [
            "<50µs", "50-100", "100-200", "200-500", "500µs-1ms", "1-2ms",
            "2-5ms", "5-10ms", "10-20ms", "20-50ms", ">50ms",
        ]
        counts = [0] * (len(edges) - 1)
        for v in data:
            for i in range(len(edges) - 1):
                if edges[i] <= v < edges[i + 1]:
                    counts[i] += 1
                    break
        hist = [{"label": labels[i], "count": counts[i]} for i in range(len(counts))]
        return {
            "count": n,
            "p50": round(pct(0.50), 1),
            "p95": round(pct(0.95), 1),
            "p99": round(pct(0.99), 1),
            "max": round(data[-1], 1),
            "mean": round(sum(data) / n, 1),
            "histogram": hist,
        }

    def symbols(self) -> list[dict[str, Any]]:
        out = []
        for s in self._symbols.values():
            first = s["first_price"] or s["last_price"]
            change = (s["last_price"] - first) / first if first else 0.0
            out.append(
                {
                    "symbol": s["symbol"],
                    "last_price": round(s["last_price"], 4),
                    "change_pct": round(change * 100, 3),
                    "high": round(s["high"], 4),
                    "low": round(s["low"], 4),
                    "trades": s["trades"],
                    "alerts": s["alerts"],
                    "notional": round(s["notional"], 2),
                    "buy": s["buy"],
                    "sell": s["sell"],
                    "spark": list(s["prices"]),
                }
            )
        out.sort(key=lambda x: x["notional"], reverse=True)
        return out

    def rate(self, seconds: int = 5) -> dict[str, float]:
        now = int(time.time())
        # Use the last `seconds` fully-elapsed buckets (exclude current partial).
        t = a = 0
        for s in range(now - seconds, now):
            t += self._ts_trades.get(s, 0)
            a += self._ts_alerts.get(s, 0)
        return {"trades_per_sec": round(t / seconds, 2), "alerts_per_sec": round(a / seconds, 2)}

    def kpis(self) -> dict[str, Any]:
        rate = self.rate()
        return {
            "total_trades": self.total_trades,
            "total_alerts": self.total_alerts,
            "total_notional": round(self.total_notional, 2),
            "alert_rate_pct": round(100 * self.total_alerts / self.total_trades, 3) if self.total_trades else 0.0,
            "symbols_tracked": len(self._symbols),
            "uptime_seconds": int(time.time() - self.start_time),
            "trades_per_sec": rate["trades_per_sec"],
            "alerts_per_sec": rate["alerts_per_sec"],
        }

    def snapshot(self, ts_window: int = 120) -> dict[str, Any]:
        return {
            "kpis": self.kpis(),
            "timeseries": self.timeseries(ts_window),
            "latency": self.latency_stats(),
            "by_detector": dict(self.by_detector),
            "by_severity": dict(self.by_severity),
            "symbols": self.symbols(),
            "tape": list(self._tape),
        }
