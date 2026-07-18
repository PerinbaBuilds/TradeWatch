"""Per-symbol rolling statistics.

Each symbol keeps a bounded, time-aware window of recent trades. The window is
the shared state that most detectors read from: it exposes running mean/std of
price and volume, the last price, short-horizon trade counts, and the raw
recent trades for detectors that need to look across events (spoofing, wash
trades).

Everything here is intentionally O(1)/O(window) and dependency-free so the hot
path stays fast enough for real-time ingestion.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

from .models import Side, Trade


@dataclass
class SymbolWindow:
    """Bounded rolling window of trades for a single symbol."""

    symbol: str
    max_trades: int = 200
    horizon_seconds: float = 300.0
    trades: deque[Trade] = field(default_factory=deque)

    def add(self, trade: Trade) -> None:
        self.trades.append(trade)
        self._evict(now_ts=trade.timestamp.timestamp())

    def _evict(self, now_ts: float) -> None:
        # Trim by count.
        while len(self.trades) > self.max_trades:
            self.trades.popleft()
        # Trim by age.
        cutoff = now_ts - self.horizon_seconds
        while self.trades and self.trades[0].timestamp.timestamp() < cutoff:
            self.trades.popleft()

    # --- size / readiness -------------------------------------------------
    def __len__(self) -> int:
        return len(self.trades)

    def ready(self, min_trades: int) -> bool:
        # Need at least ``min_trades`` *prior* observations (exclude current).
        return len(self.trades) > min_trades

    # --- price statistics -------------------------------------------------
    def _prices(self, exclude_last: bool = True) -> list[float]:
        prices = [t.price for t in self.trades]
        if exclude_last and prices:
            prices = prices[:-1]
        return prices

    def price_mean_std(self, exclude_last: bool = True) -> tuple[float, float]:
        return _mean_std(self._prices(exclude_last))

    def volume_mean_std(self, exclude_last: bool = True) -> tuple[float, float]:
        vols = [t.quantity for t in self.trades]
        if exclude_last and vols:
            vols = vols[:-1]
        return _mean_std(vols)

    def volume_median(self, exclude_last: bool = True) -> float:
        vols = sorted(t.quantity for t in self.trades)
        if exclude_last and vols:
            vols = vols[:-1]
        return _median(vols)

    def previous_price(self) -> float | None:
        if len(self.trades) < 2:
            return None
        return self.trades[-2].price

    # --- velocity ---------------------------------------------------------
    def count_within(self, seconds: float, now_ts: float | None = None) -> int:
        if not self.trades:
            return 0
        ref = now_ts if now_ts is not None else self.trades[-1].timestamp.timestamp()
        cutoff = ref - seconds
        return sum(1 for t in self.trades if t.timestamp.timestamp() >= cutoff)

    def recent(self, seconds: float, now_ts: float | None = None) -> list[Trade]:
        if not self.trades:
            return []
        ref = now_ts if now_ts is not None else self.trades[-1].timestamp.timestamp()
        cutoff = ref - seconds
        return [t for t in self.trades if t.timestamp.timestamp() >= cutoff]

    def side_imbalance(self, seconds: float) -> tuple[int, int]:
        """Return (buy_count, sell_count) within the given horizon."""
        window = self.recent(seconds)
        buys = sum(1 for t in window if t.side is Side.BUY)
        return buys, len(window) - buys


class WindowStore:
    """Registry of per-symbol windows."""

    def __init__(self, max_trades: int = 200, horizon_seconds: float = 300.0) -> None:
        self._max_trades = max_trades
        self._horizon = horizon_seconds
        self._windows: dict[str, SymbolWindow] = {}

    def get(self, symbol: str) -> SymbolWindow:
        win = self._windows.get(symbol)
        if win is None:
            win = SymbolWindow(symbol=symbol, max_trades=self._max_trades, horizon_seconds=self._horizon)
            self._windows[symbol] = win
        return win

    def symbols(self) -> list[str]:
        return list(self._windows)


def _mean_std(values: list[float]) -> tuple[float, float]:
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mean = sum(values) / n
    if n == 1:
        return mean, 0.0
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return mean, math.sqrt(max(var, 0.0))


def _median(values: list[float]) -> float:
    n = len(values)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2 == 1:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0
