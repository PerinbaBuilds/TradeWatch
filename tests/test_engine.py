from __future__ import annotations

from tradewatch.config import DetectionConfig
from tradewatch.engine import DetectionEngine
from tradewatch.models import Side

from .conftest import make_trade


def test_engine_processes_and_tracks_stats():
    engine = DetectionEngine()
    for i in range(30):
        engine.process(make_trade(price=100 + (i % 3 - 1) * 0.5, offset_seconds=i))
    stats = engine.stats()
    assert stats["trades_processed"] == 30
    assert stats["symbols_tracked"] == 1
    assert "zscore" in stats["detectors"]


def test_engine_raises_alert_on_obvious_anomaly():
    engine = DetectionEngine()
    for i in range(40):
        engine.process(make_trade(price=100 + (i % 3 - 1) * 0.5, quantity=100, offset_seconds=i))
    alerts = engine.process(make_trade(price=140, quantity=100, offset_seconds=40))
    assert alerts, "expected at least one alert for a large price outlier"
    # Alerts must be sorted most-severe first.
    ranks = [a.severity.rank for a in alerts]
    assert ranks == sorted(ranks, reverse=True)


def test_engine_wash_trade_end_to_end():
    engine = DetectionEngine()
    engine.process(make_trade(side=Side.BUY, price=50.0, account_id="acct_9", offset_seconds=0))
    alerts = engine.process(make_trade(side=Side.SELL, price=50.0, account_id="acct_9", offset_seconds=1))
    assert any(a.detector == "wash_trade" for a in alerts)


def test_disabled_detector_is_not_built():
    cfg = DetectionConfig()
    cfg.wash_trade.enabled = False
    cfg.isolation_forest.enabled = False
    engine = DetectionEngine(cfg)
    names = [d.name for d in engine.detectors]
    assert "wash_trade" not in names
    assert "isolation_forest" not in names


def test_faulty_detector_does_not_break_pipeline():
    engine = DetectionEngine()

    class Boom:
        name = "boom"

        def inspect(self, trade, window):
            raise RuntimeError("kaboom")

    engine.detectors.append(Boom())
    # Should not raise despite the faulty detector.
    engine.process(make_trade())
    assert engine.trades_processed == 1
