"""Feature engineering: trade stream -> model-ready feature matrix.

The live detectors each look at *one* facet of a trade (price z-score, size vs
median, trades-in-window, …). A model wants all of those facets as a single
numeric vector per trade, computed from the same event-time rolling window the
engine uses — so the offline model sees exactly what the online engine sees.

`FeatureBuilder` replays trades through a per-symbol `SymbolWindow` and emits one
row of `FEATURE_NAMES` per trade. It is deliberately leakage-safe: every feature
for trade *t* is computed from trades strictly *before* t (the window excludes
the current trade), so a model cannot cheat by peeking at the label-bearing
event itself.
"""

from __future__ import annotations

import numpy as np

from ..models import Side, Trade
from ..windows import SymbolWindow

FEATURE_NAMES: list[str] = [
    "price_z",          # |z-score| of price vs rolling mean
    "price_pct_move",   # abs tick-to-tick % move
    "size_vs_median",   # quantity / rolling median quantity
    "trades_in_5s",     # trade count for the symbol in the last 5s
    "side_imbalance",   # signed buy/sell imbalance in the window [-1, 1]
    "notional_log",     # log1p(price * quantity)
    "spread_from_last", # (price - last_price) / last_price
    "qty_z",            # z-score of quantity vs rolling mean
]


class FeatureBuilder:
    """Replays trades through per-symbol windows to build a feature matrix.

    Stateful by symbol, exactly like the engine, so features respect event-time.
    Call :meth:`transform_one` on a live trade, or :meth:`transform` on a batch.
    """

    def __init__(self, max_trades: int = 200, horizon_seconds: float = 300.0) -> None:
        self.max_trades = max_trades
        self.horizon_seconds = horizon_seconds
        self._windows: dict[str, SymbolWindow] = {}

    def _window(self, symbol: str) -> SymbolWindow:
        w = self._windows.get(symbol)
        if w is None:
            w = SymbolWindow(symbol=symbol, max_trades=self.max_trades, horizon_seconds=self.horizon_seconds)
            self._windows[symbol] = w
        return w

    def transform_one(self, trade: Trade) -> np.ndarray:
        """Return the feature row for ``trade`` given history seen so far, then
        fold the trade into its window."""
        w = self._window(trade.symbol)
        now = trade.timestamp.timestamp()

        prices = [t.price for t in w.trades]  # strictly-prior trades
        quantities = [t.quantity for t in w.trades]

        price_mean = float(np.mean(prices)) if prices else trade.price
        price_std = float(np.std(prices)) if len(prices) > 1 else 0.0
        qty_mean = float(np.mean(quantities)) if quantities else trade.quantity
        qty_std = float(np.std(quantities)) if len(quantities) > 1 else 0.0
        qty_median = float(np.median(quantities)) if quantities else trade.quantity
        last_price = prices[-1] if prices else trade.price

        price_z = abs(trade.price - price_mean) / price_std if price_std > 1e-9 else 0.0
        qty_z = abs(trade.quantity - qty_mean) / qty_std if qty_std > 1e-9 else 0.0
        pct_move = abs(trade.price - last_price) / last_price if last_price > 1e-9 else 0.0
        spread = (trade.price - last_price) / last_price if last_price > 1e-9 else 0.0
        size_vs_median = trade.quantity / qty_median if qty_median > 1e-9 else 1.0

        cutoff = now - 5.0
        recent = [t for t in w.trades if t.timestamp.timestamp() >= cutoff]
        trades_in_5s = float(len(recent))
        buys = sum(1 for t in recent if _side(t) == "buy")
        sells = len(recent) - buys
        imbalance = (buys - sells) / len(recent) if recent else 0.0

        row = np.array(
            [
                price_z,
                pct_move,
                size_vs_median,
                trades_in_5s,
                imbalance,
                float(np.log1p(trade.price * trade.quantity)),
                spread,
                qty_z,
            ],
            dtype=np.float64,
        )
        w.add(trade)
        return row

    def transform(self, trades: list[Trade]) -> np.ndarray:
        """Build an (n_trades, n_features) matrix for a batch, in order."""
        return np.vstack([self.transform_one(t) for t in trades]) if trades else np.empty((0, len(FEATURE_NAMES)))


def _side(trade: Trade) -> str:
    return getattr(trade.side, "value", trade.side) if trade.side else Side.BUY.value
