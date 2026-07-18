"""Spoofing / order-imbalance detector.

Spoofing and layering show up as a heavily one-sided burst of activity intended
to create a false impression of supply or demand. Working purely from the trade
tape, we approximate this as a strong directional imbalance concentrated in a
very short window: many prints on one side and almost none on the other.
"""

from __future__ import annotations

from ..config import SpoofingConfig
from ..models import Alert, Severity, Side, Trade
from ..windows import SymbolWindow
from .base import Detector


class SpoofingDetector(Detector):
    name = "spoofing"

    def __init__(self, config: SpoofingConfig) -> None:
        self.cfg = config

    def inspect(self, trade: Trade, window: SymbolWindow) -> Alert | None:
        buys, sells = window.side_imbalance(self.cfg.window_seconds)
        total = buys + sells
        if total < self.cfg.min_events:
            return None

        heavy, light = (buys, sells) if buys >= sells else (sells, buys)
        # Ratio of dominant side to the other; +1 avoids div-by-zero and keeps
        # a fully one-sided burst from being infinite.
        ratio = heavy / (light + 1)
        if ratio < self.cfg.imbalance_ratio:
            return None

        dominant = Side.BUY if buys >= sells else Side.SELL
        return Alert.build(
            trade=trade,
            detector=self.name,
            severity=Severity.HIGH,
            score=min(1.0, ratio / (self.cfg.imbalance_ratio * 2)),
            reason=(
                f"one-sided burst: {heavy} {dominant.value} vs {light} opposite "
                f"in {self.cfg.window_seconds:.0f}s (ratio {ratio:.1f})"
            ),
            details={"buys": buys, "sells": sells, "dominant_side": dominant.value, "ratio": round(ratio, 2)},
        )
