"""Wash-trade / self-trade detector.

A wash trade is a buy and a sell in the same instrument by the same beneficial
owner, with no genuine change in ownership — used to inflate volume or paint the
tape. Two complementary signals:

1. **Direct self-trade** — the trade's ``account_id`` equals its
   ``counterparty_id`` (the same entity is on both sides). This is unambiguous
   and fires immediately.
2. **Matched reciprocal cross** — the same ``account_id`` appears on the
   *opposite* side of a recent trade in the same symbol at a near-identical
   price *and* near-identical quantity. Requiring the size to match too is what
   separates a real self-cross from two unrelated trades that a busy account
   happened to do around the same price.
"""

from __future__ import annotations

from ..config import WashTradeConfig
from ..models import Alert, Severity, Side, Trade
from ..windows import SymbolWindow
from .base import Detector


class WashTradeDetector(Detector):
    name = "wash_trade"

    def __init__(self, config: WashTradeConfig) -> None:
        self.cfg = config

    def inspect(self, trade: Trade, window: SymbolWindow) -> Alert | None:
        # Signal 1: the initiator is its own counterparty.
        if trade.account_id and trade.counterparty_id and trade.account_id == trade.counterparty_id:
            return Alert.build(
                trade=trade,
                detector=self.name,
                severity=Severity.CRITICAL,
                score=0.98,
                reason=f"self-trade: account {trade.account_id} is both buyer and seller of {trade.symbol}",
                details={"account_id": trade.account_id, "signal": "self_counterparty", "price": trade.price},
            )

        if not trade.account_id or trade.price <= 0:
            return None

        # Signal 2: matched reciprocal cross by the same account.
        opposite = Side.SELL if trade.side is Side.BUY else Side.BUY
        recent = window.recent(self.cfg.window_seconds, now_ts=trade.timestamp.timestamp())
        for other in reversed(recent):
            if other.trade_id == trade.trade_id or other.account_id != trade.account_id:
                continue
            if other.side is not opposite:
                continue
            price_match = abs(other.price - trade.price) / trade.price <= self.cfg.price_tolerance
            qty_match = _rel_close(other.quantity, trade.quantity, self.cfg.quantity_tolerance)
            if price_match and qty_match:
                return Alert.build(
                    trade=trade,
                    detector=self.name,
                    severity=Severity.CRITICAL,
                    score=0.95,
                    reason=(
                        f"account {trade.account_id} crossed both sides of {trade.symbol} "
                        f"at ~{trade.price:.4f} x {trade.quantity:.2f} within {self.cfg.window_seconds:.0f}s"
                    ),
                    details={
                        "account_id": trade.account_id,
                        "signal": "matched_cross",
                        "matched_trade_id": other.trade_id,
                        "price": trade.price,
                        "quantity": trade.quantity,
                    },
                )
        return None


def _rel_close(a: float, b: float, tol: float) -> bool:
    denom = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / denom <= tol
