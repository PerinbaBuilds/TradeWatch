"""Shared test fixtures and helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tradewatch.models import Side, Trade


@pytest.fixture
def base_time() -> datetime:
    return datetime(2026, 1, 1, 15, 0, 0, tzinfo=timezone.utc)


def make_trade(
    symbol: str = "AAPL",
    price: float = 100.0,
    quantity: float = 100.0,
    side: Side = Side.BUY,
    ts: datetime | None = None,
    account_id: str | None = None,
    offset_seconds: float = 0.0,
) -> Trade:
    base = ts or datetime(2026, 1, 1, 15, 0, 0, tzinfo=timezone.utc)
    return Trade(
        symbol=symbol,
        price=price,
        quantity=quantity,
        side=side,
        account_id=account_id,
        timestamp=base + timedelta(seconds=offset_seconds),
    )
