"""Market simulator.

Generates a realistic-looking multi-symbol trade tape using geometric Brownian
motion for prices and log-normal trade sizes, then deliberately injects known
anomaly patterns at a configurable rate. This makes the engine demonstrable and
testable end-to-end without needing a live market-data subscription — and,
through :meth:`labeled_batch`, it gives every injected anomaly a ground-truth
label for evaluation (precision/recall).

Trades carry a synthetic **event-time** clock that advances per trade: normal
trades are spaced out, injected bursts are tightly clustered. Detection keys off
this event-time (not wall-clock arrival time), which is the correct design for a
streaming engine and lets short-horizon detectors age their windows correctly.

Injected patterns: price spike, volume spike, velocity burst, one-sided
spoofing burst, and a wash-trade pair.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timedelta, timezone

from ..models import Side, Trade
from .base import TradeSource

ANOMALY_KINDS = ("price_spike", "volume_spike", "velocity_burst", "spoofing", "wash_trade")

# Synthetic event-time spacing (seconds).
_NORMAL_INTERVAL_MEAN = 0.25   # gap between ordinary trades
_NORMAL_INTERVAL_STD = 0.08
_BURST_INTERVAL = 0.02         # gap between trades inside a velocity/spoofing burst
_EPOCH = datetime(2025, 1, 1, tzinfo=timezone.utc)


class _SymbolState:
    def __init__(self, symbol: str, price: float, rng: random.Random) -> None:
        self.symbol = symbol
        self.price = price
        self.drift = rng.uniform(-0.00002, 0.00002)
        self.vol = rng.uniform(0.0008, 0.0025)
        self.base_size = rng.uniform(50, 500)

    def next_price(self, rng: random.Random) -> float:
        shock = rng.gauss(self.drift, self.vol)
        self.price = max(0.01, self.price * (1.0 + shock))
        return self.price


class MarketSimulator(TradeSource):
    """Async trade generator with injectable, labelled anomalies."""

    _SEED_PRICES = {"BTC-USD": 62000.0, "ETH-USD": 3400.0, "AAPL": 195.0, "MSFT": 420.0, "TSLA": 250.0}

    def __init__(
        self,
        symbols: list[str],
        trades_per_second: float = 20.0,
        anomaly_rate: float = 0.015,
        seed: int | None = None,
        max_trades: int | None = None,
    ) -> None:
        self.rng = random.Random(seed)
        self.trades_per_second = max(0.1, trades_per_second)
        self.anomaly_rate = min(max(anomaly_rate, 0.0), 1.0)
        self.max_trades = max_trades
        self.states = {
            s: _SymbolState(s, self._SEED_PRICES.get(s, self.rng.uniform(20, 800)), self.rng)
            for s in symbols
        }
        self.accounts = [f"acct_{i:03d}" for i in range(40)]
        self.venues = ["XNAS", "XNYS", "ARCX", "BATS"]
        self._elapsed = 0.0  # synthetic event-time cursor (seconds since epoch)

    # --- event-time clock -------------------------------------------------
    def _advance(self, dt: float) -> datetime:
        self._elapsed += max(dt, 0.0)
        return _EPOCH + timedelta(seconds=self._elapsed)

    def _normal_gap(self) -> float:
        return max(0.01, self.rng.gauss(_NORMAL_INTERVAL_MEAN, _NORMAL_INTERVAL_STD))

    # --- streaming --------------------------------------------------------
    async def stream(self):
        interval = 1.0 / self.trades_per_second
        emitted = 0
        while self.max_trades is None or emitted < self.max_trades:
            for trade, _label in self._next_batch():
                yield trade
                emitted += 1
            await asyncio.sleep(interval)

    def labeled_batch(self, count: int) -> list[tuple[Trade, str | None]]:
        """Return ``count`` trades paired with their ground-truth label.

        A label of ``None`` means a normal trade; otherwise it is one of
        :data:`ANOMALY_KINDS`. Used by the evaluation harness and tests.
        """
        out: list[tuple[Trade, str | None]] = []
        while len(out) < count:
            out.extend(self._next_batch())
        return out[:count]

    # ------------------------------------------------------------------
    def _next_batch(self) -> list[tuple[Trade, str | None]]:
        symbol = self.rng.choice(list(self.states))
        state = self.states[symbol]
        if self.rng.random() < self.anomaly_rate:
            return self._anomalous(state)
        return [(self._normal(state, gap=self._normal_gap()), None)]

    def _normal(self, state: _SymbolState, *, gap: float, **overrides) -> Trade:
        price = overrides.pop("price", None)
        if price is None:
            price = round(state.next_price(self.rng), 4)
        size = overrides.pop("quantity", round(max(1.0, self.rng.lognormvariate(0, 0.5) * state.base_size), 2))
        account = self.rng.choice(self.accounts)
        counterparty = self.rng.choice([a for a in self.accounts if a != account])
        data = dict(
            symbol=state.symbol,
            price=price,
            quantity=size,
            side=self.rng.choice([Side.BUY, Side.SELL]),
            account_id=account,
            counterparty_id=counterparty,
            venue=self.rng.choice(self.venues),
            timestamp=self._advance(gap),
        )
        data.update(overrides)
        return Trade(**data)

    def _anomalous(self, state: _SymbolState) -> list[tuple[Trade, str | None]]:
        kind = self.rng.choice(ANOMALY_KINDS)

        if kind == "price_spike":
            direction = self.rng.choice([-1, 1])
            spiked = round(state.price * (1 + direction * self.rng.uniform(0.05, 0.15)), 4)
            state.price = spiked
            return [(self._normal(state, gap=self._normal_gap(), price=spiked), "price_spike")]

        if kind == "volume_spike":
            size = round(state.base_size * self.rng.uniform(20, 60), 2)
            return [(self._normal(state, gap=self._normal_gap(), quantity=size), "volume_spike")]

        if kind == "velocity_burst":
            n = self.rng.randint(30, 70)
            return [(self._normal(state, gap=_BURST_INTERVAL), "velocity_burst") for _ in range(n)]

        if kind == "spoofing":
            side = self.rng.choice([Side.BUY, Side.SELL])
            n = self.rng.randint(10, 20)
            return [(self._normal(state, gap=_BURST_INTERVAL, side=side), "spoofing") for _ in range(n)]

        # wash_trade: same beneficial owner on both sides (self-cross), matched
        # price and size, in tight succession.
        account = self.rng.choice(self.accounts)
        price = round(state.price, 4)
        qty = round(max(1.0, self.rng.lognormvariate(0, 0.5) * state.base_size), 2)
        common = dict(price=price, quantity=qty, account_id=account, counterparty_id=account)
        buy = self._normal(state, gap=self._normal_gap(), side=Side.BUY, **common)
        sell = self._normal(state, gap=_BURST_INTERVAL, side=Side.SELL, **common)
        return [(buy, "wash_trade"), (sell, "wash_trade")]
