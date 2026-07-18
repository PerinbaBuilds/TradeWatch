"""In-process pub/sub broadcaster and WebSocket sink.

The :class:`Broadcaster` is a fan-out hub the FastAPI app shares between the
pipeline and every connected WebSocket client. It keeps a bounded ring buffer of
recent alerts (so a dashboard that connects late still has context) and pushes
new alerts to all live subscribers.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

from ..models import Alert, Trade
from .base import AlertSink


class Broadcaster:
    """Fan-out hub for alerts and (optionally) the raw trade tape."""

    def __init__(self, history: int = 500) -> None:
        self._alert_subs: set[asyncio.Queue] = set()
        self._trade_subs: set[asyncio.Queue] = set()
        self._recent: deque[dict[str, Any]] = deque(maxlen=history)

    # --- subscription management -----------------------------------------
    def subscribe_alerts(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._alert_subs.add(q)
        return q

    def unsubscribe_alerts(self, q: asyncio.Queue) -> None:
        self._alert_subs.discard(q)

    def subscribe_trades(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._trade_subs.add(q)
        return q

    def unsubscribe_trades(self, q: asyncio.Queue) -> None:
        self._trade_subs.discard(q)

    # --- publishing -------------------------------------------------------
    def publish_alert(self, alert: Alert) -> None:
        payload = {"type": "alert", "data": alert.model_dump(mode="json")}
        self._recent.append(payload)
        self._fan_out(self._alert_subs, payload)

    def publish_trade(self, trade: Trade) -> None:
        payload = {"type": "trade", "data": trade.model_dump(mode="json")}
        self._fan_out(self._trade_subs, payload)

    @staticmethod
    def _fan_out(subs: set[asyncio.Queue], payload: dict[str, Any]) -> None:
        for q in list(subs):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                # Drop for slow consumers rather than block the hot path.
                pass

    def recent_alerts(self, limit: int = 100) -> list[dict[str, Any]]:
        items = list(self._recent)[-limit:]
        return [i["data"] for i in items]


class WebSocketSink(AlertSink):
    """Adapts the engine's sink interface onto a :class:`Broadcaster`."""

    def __init__(self, broadcaster: Broadcaster) -> None:
        self.broadcaster = broadcaster

    async def emit(self, trade: Trade, alert: Alert) -> None:
        self.broadcaster.publish_alert(alert)
