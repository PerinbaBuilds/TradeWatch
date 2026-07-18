"""Price-spike detector.

Catches abrupt tick-to-tick price jumps (gaps) that a slow-moving mean/std may
smooth over. Complements the z-score detector: this fires on the *speed* of a
move, z-score fires on the *distance* from the baseline.
"""

from __future__ import annotations

from ..config import PriceSpikeConfig
from ..models import Alert, Severity, Trade
from ..windows import SymbolWindow
from .base import Detector


class PriceSpikeDetector(Detector):
    name = "price_spike"

    def __init__(self, config: PriceSpikeConfig) -> None:
        self.cfg = config

    def inspect(self, trade: Trade, window: SymbolWindow) -> Alert | None:
        prev = window.previous_price()
        if prev is None or prev <= 0:
            return None

        pct = (trade.price - prev) / prev
        if abs(pct) < self.cfg.pct_threshold:
            return None

        critical = abs(pct) >= self.cfg.critical_pct
        severity = Severity.CRITICAL if critical else Severity.HIGH
        direction = "surge" if pct > 0 else "drop"
        return Alert.build(
            trade=trade,
            detector=self.name,
            severity=severity,
            score=min(1.0, abs(pct) / max(self.cfg.critical_pct, 1e-9)),
            reason=f"instantaneous price {direction} of {pct:+.2%} ({prev:.4f} -> {trade.price:.4f})",
            details={"pct_change": round(pct, 5), "previous_price": prev},
        )
