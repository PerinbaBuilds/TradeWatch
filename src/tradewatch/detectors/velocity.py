"""Trade-velocity detector.

Bursts of trades in a very short window are a classic signature of algorithmic
misbehaviour, momentum ignition and quote stuffing. This detector counts trades
per symbol inside a short horizon and fires when the rate spikes.
"""

from __future__ import annotations

from ..config import VelocityConfig
from ..models import Alert, Severity, Trade
from ..windows import SymbolWindow
from .base import Detector


class VelocityDetector(Detector):
    name = "velocity"

    def __init__(self, config: VelocityConfig) -> None:
        self.cfg = config

    def inspect(self, trade: Trade, window: SymbolWindow) -> Alert | None:
        count = window.count_within(self.cfg.window_seconds, now_ts=trade.timestamp.timestamp())
        if count < self.cfg.max_trades:
            return None

        critical = count >= self.cfg.critical_trades
        severity = Severity.CRITICAL if critical else Severity.MEDIUM
        return Alert.build(
            trade=trade,
            detector=self.name,
            severity=severity,
            score=min(1.0, count / max(self.cfg.critical_trades, 1)),
            reason=f"{count} trades in {self.cfg.window_seconds:.0f}s (limit {self.cfg.max_trades})",
            details={"count": count, "window_seconds": self.cfg.window_seconds},
        )
