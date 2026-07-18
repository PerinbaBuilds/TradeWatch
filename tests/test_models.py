from __future__ import annotations

import pytest
from pydantic import ValidationError

from tradewatch.models import Alert, Severity, Side, Trade


def test_symbol_normalized_and_notional():
    t = Trade(symbol=" aapl ", price=10.0, quantity=5.0)
    assert t.symbol == "AAPL"
    assert t.notional == 50.0
    assert t.side is Side.BUY


def test_price_and_quantity_must_be_positive():
    with pytest.raises(ValidationError):
        Trade(symbol="X", price=0, quantity=1)
    with pytest.raises(ValidationError):
        Trade(symbol="X", price=1, quantity=-3)


def test_severity_ordering():
    assert Severity.CRITICAL.rank > Severity.HIGH.rank > Severity.MEDIUM.rank > Severity.LOW.rank


def test_alert_score_is_clamped():
    t = Trade(symbol="X", price=1, quantity=1)
    high = Alert.build(trade=t, detector="d", severity=Severity.HIGH, score=5.0, reason="r")
    low = Alert.build(trade=t, detector="d", severity=Severity.LOW, score=-2.0, reason="r")
    assert high.score == 1.0
    assert low.score == 0.0
    assert high.trade_id == t.trade_id
