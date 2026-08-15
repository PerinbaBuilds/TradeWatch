"""Property-based tests (Hypothesis).

Unit tests check known cases; these check *invariants* that must hold for any
valid input the engine could ever see. Hypothesis searches the input space for a
counter-example and shrinks it to the minimal failing case.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from hypothesis import given, settings
from hypothesis import strategies as st

from tradewatch.engine import DetectionEngine
from tradewatch.models import Severity, Side, Trade
from tradewatch.windows import SymbolWindow

_BASE = datetime(2026, 1, 1, 15, 0, 0, tzinfo=timezone.utc)

# A strategy that builds any *valid* trade (price/qty strictly positive).
valid_trades = st.builds(
    Trade,
    symbol=st.sampled_from(["AAPL", "MSFT", "BTC-USD", "ETH-USD", "TSLA"]),
    price=st.floats(min_value=0.01, max_value=1_000_000, allow_nan=False, allow_infinity=False),
    quantity=st.floats(min_value=0.01, max_value=1_000_000, allow_nan=False, allow_infinity=False),
    side=st.sampled_from(list(Side)),
)


@given(trades=st.lists(valid_trades, min_size=1, max_size=200))
@settings(max_examples=150, deadline=None)
def test_engine_never_crashes_and_alerts_are_wellformed(trades):
    """For any sequence of valid trades the engine must not raise, and every
    alert it emits must be structurally valid and reference its trade."""
    engine = DetectionEngine()
    for i, t in enumerate(trades):
        # Give each trade a monotonic event-time so windows behave.
        t = t.model_copy(update={"timestamp": _BASE + timedelta(seconds=i)})
        alerts = engine.process(t)
        assert isinstance(alerts, list)
        for a in alerts:
            assert isinstance(a.severity, Severity)
            assert 0.0 <= a.score <= 1.0
            assert a.symbol == t.symbol
            assert a.reason


@given(trades=st.lists(valid_trades, min_size=1, max_size=200))
@settings(max_examples=150, deadline=None)
def test_alerts_sorted_most_severe_first(trades):
    """The engine contract: returned alerts are ordered by descending severity."""
    order = {Severity.CRITICAL: 3, Severity.HIGH: 2, Severity.MEDIUM: 1, Severity.LOW: 0}
    engine = DetectionEngine()
    for i, t in enumerate(trades):
        t = t.model_copy(update={"timestamp": _BASE + timedelta(seconds=i)})
        alerts = engine.process(t)
        ranks = [order[a.severity] for a in alerts]
        assert ranks == sorted(ranks, reverse=True)


@given(
    n=st.integers(min_value=1, max_value=500),
    max_trades=st.integers(min_value=5, max_value=100),
)
@settings(max_examples=100, deadline=None)
def test_symbol_window_is_bounded(n, max_trades):
    """A window must never exceed its count cap however many trades arrive."""
    w = SymbolWindow(symbol="AAPL", max_trades=max_trades, horizon_seconds=1e9)
    for i in range(n):
        w.add(Trade(symbol="AAPL", price=100.0, quantity=1.0, timestamp=_BASE + timedelta(seconds=i)))
    assert len(w) <= max_trades
