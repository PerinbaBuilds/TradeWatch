"""Detection engine.

The engine is the synchronous core of TradeWatch. It owns the per-symbol
windows, runs every configured detector against each incoming trade, and
returns the alerts that fired. It is deliberately transport-agnostic: feed it
trades from anywhere (HTTP, Kafka, a CSV replay, the bundled simulator) and it
returns structured alerts you can route wherever you like.
"""

from __future__ import annotations

from collections import Counter

from .config import DetectionConfig
from .detectors import (
    Detector,
    IsolationForestDetector,
    PriceSpikeDetector,
    SpoofingDetector,
    VelocityDetector,
    VolumeSpikeDetector,
    WashTradeDetector,
    ZScoreDetector,
)
from .models import Alert, Trade
from .windows import WindowStore


class DetectionEngine:
    """Runs a configured set of detectors over a stream of trades."""

    def __init__(self, config: DetectionConfig | None = None) -> None:
        self.config = config or DetectionConfig()
        self.windows = WindowStore(
            max_trades=self.config.window.max_trades,
            horizon_seconds=self.config.window.horizon_seconds,
        )
        self.detectors: list[Detector] = self._build_detectors()
        self.cooldown = max(0.0, self.config.alert_cooldown_seconds)

        # Lightweight operational metrics.
        self.trades_processed = 0
        self.alerts_raised = 0
        self.alerts_suppressed = 0
        self.alerts_by_detector: Counter[str] = Counter()
        self.alerts_by_severity: Counter[str] = Counter()

        # Event-time of the last emitted alert per (symbol, detector), for dedup.
        self._last_alert_ts: dict[tuple[str, str], float] = {}

    def _build_detectors(self) -> list[Detector]:
        cfg = self.config
        min_trades = cfg.window.min_trades
        detectors: list[Detector] = []
        if cfg.zscore.enabled:
            detectors.append(ZScoreDetector(cfg.zscore, min_trades))
        if cfg.price_spike.enabled:
            detectors.append(PriceSpikeDetector(cfg.price_spike))
        if cfg.volume_spike.enabled:
            detectors.append(VolumeSpikeDetector(cfg.volume_spike, min_trades))
        if cfg.velocity.enabled:
            detectors.append(VelocityDetector(cfg.velocity))
        if cfg.spoofing.enabled:
            detectors.append(SpoofingDetector(cfg.spoofing))
        if cfg.wash_trade.enabled:
            detectors.append(WashTradeDetector(cfg.wash_trade))
        if cfg.isolation_forest.enabled:
            detectors.append(IsolationForestDetector(cfg.isolation_forest, min_trades))
        return detectors

    def process(self, trade: Trade) -> list[Alert]:
        """Ingest one trade and return any alerts it triggered."""
        window = self.windows.get(trade.symbol)
        window.add(trade)
        self.trades_processed += 1

        alerts: list[Alert] = []
        for detector in self.detectors:
            try:
                alert = detector.inspect(trade, window)
            except Exception as exc:  # a single detector must never take down ingestion
                # Fail open: log-and-continue keeps the pipeline resilient.
                alert = None
                self.alerts_by_detector[f"{detector.name}:error"] += 1
                _ = exc
            if alert is not None and not self._suppressed(trade, detector.name):
                alerts.append(alert)
                self.alerts_raised += 1
                self.alerts_by_detector[detector.name] += 1
                self.alerts_by_severity[alert.severity.value] += 1

        # Most severe first so consumers can prioritise.
        alerts.sort(key=lambda a: (a.severity.rank, a.score), reverse=True)
        return alerts

    def _suppressed(self, trade: Trade, detector_name: str) -> bool:
        """Return True if this alert should be deduplicated (within cooldown)."""
        if self.cooldown <= 0:
            return False
        key = (trade.symbol, detector_name)
        ts = trade.timestamp.timestamp()
        last = self._last_alert_ts.get(key)
        if last is not None and ts - last < self.cooldown:
            self.alerts_suppressed += 1
            return True
        self._last_alert_ts[key] = ts
        return False

    def stats(self) -> dict:
        """Snapshot of engine metrics for /stats endpoints and dashboards."""
        return {
            "trades_processed": self.trades_processed,
            "alerts_raised": self.alerts_raised,
            "alerts_suppressed": self.alerts_suppressed,
            "symbols_tracked": len(self.windows.symbols()),
            "detectors": [d.name for d in self.detectors],
            "alerts_by_detector": dict(self.alerts_by_detector),
            "alerts_by_severity": dict(self.alerts_by_severity),
        }
