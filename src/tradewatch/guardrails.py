"""Input guardrails for ingested trades.

Pydantic validates *shape* (types, required fields, positivity). Guardrails add
a *safety/sanity* layer on top: reject economically-absurd or abusive payloads
before they reach the engine, so a bad or hostile producer can't poison the
detectors, blow up notional aggregates, or exhaust memory with unbounded symbol
cardinality.

This is deliberately conservative and configurable — it fails closed on clearly
invalid input and passes everything plausible. Every rejection carries a reason
so it can be logged and audited.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

from .models import Trade

_SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9._\-]{0,31}$")


@dataclass(frozen=True)
class GuardrailConfig:
    max_price: float = 1e9          # reject prints above $1B/unit
    max_quantity: float = 1e12      # reject absurd sizes
    max_notional: float = 1e13      # reject > $10T single-trade notional
    max_symbol_cardinality: int = 10_000  # cap distinct symbols tracked
    require_symbol_pattern: bool = True


@dataclass(frozen=True)
class GuardrailResult:
    ok: bool
    reason: str | None = None
    code: str | None = None


class Guardrails:
    """Validates trades against safety bounds and tracks symbol cardinality."""

    def __init__(self, config: GuardrailConfig | None = None) -> None:
        self.cfg = config or GuardrailConfig()
        self._symbols: set[str] = set()

    def check(self, trade: Trade) -> GuardrailResult:
        # Finiteness (NaN/inf slip past ``gt=0`` in some paths).
        if not (math.isfinite(trade.price) and math.isfinite(trade.quantity)):
            return GuardrailResult(False, "non-finite price or quantity", "non_finite")

        if self.cfg.require_symbol_pattern and not _SYMBOL_RE.match(trade.symbol):
            return GuardrailResult(False, f"symbol '{trade.symbol}' fails format policy", "bad_symbol")

        if trade.price > self.cfg.max_price:
            return GuardrailResult(False, f"price {trade.price} exceeds max {self.cfg.max_price}", "price_bound")

        if trade.quantity > self.cfg.max_quantity:
            return GuardrailResult(False, f"quantity {trade.quantity} exceeds max {self.cfg.max_quantity}", "qty_bound")

        if trade.notional > self.cfg.max_notional:
            return GuardrailResult(False, f"notional {trade.notional:.0f} exceeds max", "notional_bound")

        if trade.symbol not in self._symbols:
            if len(self._symbols) >= self.cfg.max_symbol_cardinality:
                return GuardrailResult(False, "symbol cardinality limit reached", "cardinality")
            self._symbols.add(trade.symbol)

        return GuardrailResult(True)
