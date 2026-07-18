"""Volume-spike detector.

Uses the median (robust to the very outliers we're hunting) rather than the
mean as the baseline for "normal" size, and flags trades whose quantity is a
large multiple of it. Good at surfacing block trades, fat-finger sizes and
sudden liquidity events.
"""

from __future__ import annotations

from ..config import VolumeSpikeConfig
from ..models import Alert, Severity, Trade
from ..windows import SymbolWindow
from .base import Detector


class VolumeSpikeDetector(Detector):
    name = "volume_spike"

    def __init__(self, config: VolumeSpikeConfig, min_trades: int) -> None:
        self.cfg = config
        self.min_trades = min_trades

    def inspect(self, trade: Trade, window: SymbolWindow) -> Alert | None:
        if not window.ready(self.min_trades):
            return None

        median = window.volume_median()
        if median <= 0:
            return None

        ratio = trade.quantity / median
        if ratio < self.cfg.median_multiplier:
            return None

        critical = ratio >= self.cfg.critical_multiplier
        severity = Severity.CRITICAL if critical else Severity.HIGH
        return Alert.build(
            trade=trade,
            detector=self.name,
            severity=severity,
            score=min(1.0, ratio / max(self.cfg.critical_multiplier, 1e-9)),
            reason=f"quantity {trade.quantity:.2f} is {ratio:.1f}x median size {median:.2f}",
            details={
                "ratio": round(ratio, 2),
                "median_quantity": round(median, 4),
                "notional": round(trade.notional, 2),
            },
        )
