from __future__ import annotations

from tradewatch.metrics import MetricsCollector
from tradewatch.models import Alert, Severity

from .conftest import make_trade


def _alert(trade, detector="zscore", severity=Severity.HIGH):
    return Alert.build(trade=trade, detector=detector, severity=severity, score=0.9, reason="r")


def test_records_trades_and_alerts():
    m = MetricsCollector()
    t1 = make_trade(symbol="AAPL", price=100, quantity=10)
    m.record(t1, [], 120.0)
    t2 = make_trade(symbol="AAPL", price=140, quantity=10)
    m.record(t2, [_alert(t2)], 8000.0)

    k = m.kpis()
    assert k["total_trades"] == 2
    assert k["total_alerts"] == 1
    assert k["symbols_tracked"] == 1
    assert m.by_detector["zscore"] == 1
    assert m.by_severity["high"] == 1


def test_symbols_snapshot_has_sparkline_and_change():
    m = MetricsCollector()
    for p in (100, 101, 102, 108):
        m.record(make_trade(symbol="BTC-USD", price=p, quantity=1), [], 100.0)
    syms = m.symbols()
    assert len(syms) == 1
    s = syms[0]
    assert s["symbol"] == "BTC-USD"
    assert s["change_pct"] > 0
    assert len(s["spark"]) == 4
    assert s["high"] == 108


def test_latency_percentiles_and_histogram():
    m = MetricsCollector()
    for i in range(200):
        m.record(make_trade(price=100 + i * 0.01), [], float(100 + i))
    lat = m.latency_stats()
    assert lat["count"] == 200
    assert lat["p50"] <= lat["p95"] <= lat["p99"] <= lat["max"]
    assert sum(b["count"] for b in lat["histogram"]) == 200


def test_snapshot_shape():
    m = MetricsCollector()
    m.record(make_trade(), [], 100.0)
    snap = m.snapshot(ts_window=60)
    for key in ("kpis", "timeseries", "latency", "by_detector", "by_severity", "symbols", "tape"):
        assert key in snap
    assert len(snap["timeseries"]) == 60
