"""Kafka trade source (optional).

Consumes JSON-encoded trades from a Kafka topic. This is the production path for
plugging TradeWatch into an existing event backbone. ``aiokafka`` is an optional
dependency (``pip install tradewatch[kafka]``); importing this module without it
raises a clear, actionable error only when you actually try to use it.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

from ..models import Trade
from .base import TradeSource


class KafkaTradeSource(TradeSource):
    """Stream trades from a Kafka topic of JSON messages."""

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "tradewatch",
        auto_offset_reset: str = "latest",
    ) -> None:
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self._consumer = None

    async def _ensure_consumer(self):
        if self._consumer is not None:
            return self._consumer
        try:
            from aiokafka import AIOKafkaConsumer
        except ImportError as exc:  # pragma: no cover - optional dep
            raise RuntimeError(
                "aiokafka is required for the Kafka source. Install it with "
                "`pip install tradewatch[kafka]`."
            ) from exc
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        await self._consumer.start()
        return self._consumer

    async def stream(self) -> AsyncIterator[Trade]:
        consumer = await self._ensure_consumer()
        async for message in consumer:
            try:
                yield Trade.model_validate(message.value)
            except Exception:
                # Skip malformed messages rather than crash the consumer loop.
                continue

    async def close(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
