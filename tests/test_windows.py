from __future__ import annotations

from tradewatch.windows import SymbolWindow, WindowStore, _mean_std, _median

from .conftest import make_trade


def test_mean_std_and_median_helpers():
    mean, std = _mean_std([2.0, 4.0, 6.0])
    assert mean == 4.0
    assert round(std, 4) == 2.0
    assert _median([1, 2, 3, 4]) == 2.5
    assert _median([5]) == 5
    assert _median([]) == 0.0


def test_window_evicts_by_count():
    win = SymbolWindow(symbol="AAPL", max_trades=5, horizon_seconds=1e9)
    for i in range(10):
        win.add(make_trade(price=100 + i, offset_seconds=i))
    assert len(win) == 5
    # Oldest kept trade should be the 6th (index 5).
    assert win.trades[0].price == 105


def test_window_evicts_by_age():
    win = SymbolWindow(symbol="AAPL", max_trades=1000, horizon_seconds=10)
    for i in range(30):
        win.add(make_trade(offset_seconds=i))
    # Only trades within the last 10s of the newest (t=29) survive: t>=19.
    assert all(t.timestamp.timestamp() >= win.trades[-1].timestamp.timestamp() - 10 for t in win.trades)
    assert len(win) == 11


def test_count_within_and_imbalance():
    from tradewatch.models import Side

    win = SymbolWindow(symbol="AAPL", max_trades=1000, horizon_seconds=1e9)
    for i in range(10):
        win.add(make_trade(side=Side.BUY, offset_seconds=i * 0.1))
    assert win.count_within(1.0) == 10
    buys, sells = win.side_imbalance(1.0)
    assert buys == 10 and sells == 0


def test_window_store_creates_per_symbol():
    store = WindowStore(max_trades=10)
    a = store.get("AAPL")
    b = store.get("MSFT")
    assert a is not b
    assert store.get("AAPL") is a
    assert set(store.symbols()) == {"AAPL", "MSFT"}
