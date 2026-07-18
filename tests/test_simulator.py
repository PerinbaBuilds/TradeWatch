from __future__ import annotations

import pytest

from tradewatch.evaluate import evaluate
from tradewatch.models import Trade
from tradewatch.sources.simulator import ANOMALY_KINDS, MarketSimulator


def test_simulator_is_deterministic_with_seed():
    a = MarketSimulator(["AAPL", "MSFT"], seed=123).labeled_batch(200)
    b = MarketSimulator(["AAPL", "MSFT"], seed=123).labeled_batch(200)
    assert [t.price for t, _ in a] == [t.price for t, _ in b]


def test_labeled_batch_shapes_and_labels():
    batch = MarketSimulator(["AAPL"], anomaly_rate=0.1, seed=1).labeled_batch(500)
    assert len(batch) == 500
    for trade, label in batch:
        assert isinstance(trade, Trade)
        assert label is None or label in ANOMALY_KINDS
    assert any(label is not None for _, label in batch)


@pytest.mark.asyncio
async def test_stream_respects_max_trades():
    sim = MarketSimulator(["AAPL"], trades_per_second=1000, anomaly_rate=0.0, max_trades=50)
    seen = [t async for t in sim.stream()]
    assert len(seen) >= 50


def test_evaluation_meets_quality_gate():
    # Single event-based evaluation covering both aggregate metrics and
    # per-pattern recall (one ML run keeps the suite fast).
    report = evaluate(trades=8000, anomaly_rate=0.03, seed=7)
    assert report.total_events > 0

    # Catch the large majority of injected anomaly episodes...
    assert report.recall >= 0.9, f"recall too low: {report.recall}"
    # ...while keeping false alarms on normal flow low.
    assert report.false_positive_rate <= 0.03, f"FPR too high: {report.false_positive_rate}"
    assert report.precision >= 0.5, f"precision too low: {report.precision}"

    # Every injected pattern must be caught the large majority of the time.
    for label, (detected, total) in report.per_label_recall.items():
        assert total > 0
        assert detected / total >= 0.8, f"{label} recall {detected}/{total}"
