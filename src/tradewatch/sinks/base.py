"""Alert sink interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import Alert, Trade


class AlertSink(ABC):
    """Destination for alerts produced by the engine.

    A sink receives the offending trade alongside each alert so it has full
    context. Implementations can print, persist, forward to a WebSocket, push
    to Kafka/Slack/a SIEM, etc.
    """

    @abstractmethod
    async def emit(self, trade: Trade, alert: Alert) -> None:
        """Deliver a single alert."""

    async def close(self) -> None:  # pragma: no cover - default no-op
        return None
