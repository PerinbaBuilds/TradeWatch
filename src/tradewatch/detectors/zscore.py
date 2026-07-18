"""Price z-score detector.

Flags trades whose price deviates from the recent per-symbol mean by more than a
configurable number of standard deviations — the work-horse statistical detector
for "this print is far from where the market has been trading".

It deliberately does **not** z-score trade size: quantities are heavy-tailed
(log-normal), so a Gaussian z-score there is a false-positive factory. Size
anomalies are handled by the robust, median-based
:class:`~tradewatch.detectors.volume_spike.VolumeSpikeDetector` instead.
"""

from __future__ import annotations

from ..config import ZScoreConfig
from ..models import Alert, Severity, Trade
from ..windows import SymbolWindow
from .base import Detector


class ZScoreDetector(Detector):
    name = "zscore"

    def __init__(self, config: ZScoreConfig, min_trades: int) -> None:
        self.cfg = config
        self.min_trades = min_trades

    def inspect(self, trade: Trade, window: SymbolWindow) -> Alert | None:
        if not window.ready(self.min_trades):
            return None

        price_mean, price_std = window.price_mean_std()
        price_z = _zscore(trade.price, price_mean, price_std)
        if abs(price_z) < self.cfg.price_threshold:
            return None

        critical = abs(price_z) >= self.cfg.price_threshold * self.cfg.critical_multiplier
        severity = Severity.CRITICAL if critical else Severity.HIGH
        return Alert.build(
            trade=trade,
            detector=self.name,
            severity=severity,
            score=_z_to_score(abs(price_z), self.cfg.price_threshold),
            reason=(
                f"price {trade.price:.4f} is {price_z:+.2f}σ from mean "
                f"{price_mean:.4f} (σ={price_std:.4f})"
            ),
            details={
                "price_z": round(price_z, 3),
                "price_mean": round(price_mean, 4),
                "price_std": round(price_std, 4),
            },
        )


def _zscore(value: float, mean: float, std: float) -> float:
    if std <= 1e-12:
        return 0.0
    return (value - mean) / std


def _z_to_score(abs_z: float, threshold: float) -> float:
    # Map [threshold, 2*threshold] -> [0.6, 1.0], saturating above.
    if threshold <= 0:
        return 1.0
    return min(1.0, 0.6 + 0.4 * (abs_z - threshold) / threshold)
