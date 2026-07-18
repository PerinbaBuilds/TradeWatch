"""Trade source interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from ..models import Trade


class TradeSource(ABC):
    """An async source of trades.

    Implementations yield :class:`Trade` objects. Anything that can produce
    trades — a market-data feed, a Kafka topic, a file replay, a simulator —
    can back the pipeline by implementing :meth:`stream`.
    """

    @abstractmethod
    def stream(self) -> AsyncIterator[Trade]:
        """Yield trades until the source is exhausted or cancelled."""

    async def close(self) -> None:  # pragma: no cover - default no-op
        """Release any resources held by the source."""
        return None
