"""Trade sources: where the engine reads trades from."""

from __future__ import annotations

from .base import TradeSource
from .simulator import MarketSimulator

__all__ = ["MarketSimulator", "TradeSource"]
