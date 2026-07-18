"""Core domain models for TradeWatch.

These models define the wire format for trades coming into the engine and the
alerts flowing out of it. They are plain Pydantic models so they validate
cleanly at the API boundary and serialize straight to JSON for any downstream
sink (WebSocket, Kafka, a database, a SIEM, ...).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


class Side(str, Enum):
    """Direction of an executed trade."""

    BUY = "buy"
    SELL = "sell"


class Severity(str, Enum):
    """Alert severity, ordered from least to most urgent."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return _SEVERITY_RANK[self]


_SEVERITY_RANK: dict[Severity, int] = {
    Severity.LOW: 0,
    Severity.MEDIUM: 1,
    Severity.HIGH: 2,
    Severity.CRITICAL: 3,
}


class Trade(BaseModel):
    """A single executed trade.

    The engine only requires ``symbol``, ``price`` and ``quantity``; every
    other field is optional but enables richer detection (e.g. ``account_id``
    powers wash-trade detection, ``venue`` lets you segment cross-venue flow).
    """

    trade_id: str = Field(default_factory=lambda: _new_id("trd"))
    timestamp: datetime = Field(default_factory=_utcnow)
    symbol: str = Field(..., min_length=1, max_length=32)
    price: float = Field(..., gt=0)
    quantity: float = Field(..., gt=0)
    side: Side = Side.BUY
    account_id: str | None = None
    counterparty_id: str | None = None
    venue: str | None = None
    currency: str = "USD"

    @field_validator("symbol")
    @classmethod
    def _upper_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @property
    def notional(self) -> float:
        """Traded value = price x quantity."""
        return self.price * self.quantity


class Alert(BaseModel):
    """An anomaly raised against a specific trade by a detector."""

    alert_id: str = Field(default_factory=lambda: _new_id("alrt"))
    timestamp: datetime = Field(default_factory=_utcnow)
    trade_id: str
    symbol: str
    detector: str
    severity: Severity
    score: float = Field(..., description="Normalized anomaly score in [0, 1].")
    reason: str
    details: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def build(
        cls,
        *,
        trade: Trade,
        detector: str,
        severity: Severity,
        score: float,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> Alert:
        return cls(
            trade_id=trade.trade_id,
            symbol=trade.symbol,
            detector=detector,
            severity=severity,
            score=max(0.0, min(1.0, score)),
            reason=reason,
            details=details or {},
        )
