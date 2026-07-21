"""Streaming pipeline.

Glue that pulls trades from a :class:`~tradewatch.sources.base.TradeSource`,
runs them through the :class:`~tradewatch.engine.DetectionEngine`, and fans the
resulting alerts out to one or more
:class:`~tradewatch.sinks.base.AlertSink` s. Cancellation-safe and sink-failure
tolerant so one bad consumer can't stall ingestion.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from .engine import DetectionEngine
from .models import Alert, Trade
from .sinks.base import AlertSink
from .sources.base import TradeSource

logger = logging.getLogger("tradewatch.pipeline")


class Pipeline:
    def __init__(
        self,
        source: TradeSource,
        engine: DetectionEngine,
        sinks: list[AlertSink] | None = None,
        on_trade: Callable[[Trade], None] | Callable[[Trade], Awaitable[None]] | None = None,
        on_processed: Callable[[Trade, list[Alert], float], None] | None = None,
    ) -> None:
        self.source = source
        self.engine = engine
        self.sinks = sinks or []
        self.on_trade = on_trade
        # Called after each trade with (trade, alerts, latency_us) — powers metrics.
        self.on_processed = on_processed
        self._running = False

    async def run(self) -> None:
        """Consume the source until it ends or the task is cancelled."""
        self._running = True
        logger.info("pipeline started with %d detector(s)", len(self.engine.detectors))
        try:
            async for trade in self.source.stream():
                await self._handle_trade(trade)
        except asyncio.CancelledError:
            logger.info("pipeline cancelled")
            raise
        finally:
            self._running = False
            await self.source.close()

    async def _handle_trade(self, trade: Trade) -> None:
        if self.on_trade is not None:
            result = self.on_trade(trade)
            if asyncio.iscoroutine(result):
                await result

        start = time.perf_counter_ns()
        alerts = self.engine.process(trade)
        latency_us = (time.perf_counter_ns() - start) / 1000.0

        if self.on_processed is not None:
            try:
                self.on_processed(trade, alerts, latency_us)
            except Exception:  # metrics must never stall ingestion
                logger.exception("on_processed hook failed")

        for alert in alerts:
            await self._dispatch(trade, alert)

    async def _dispatch(self, trade: Trade, alert: Alert) -> None:
        for sink in self.sinks:
            try:
                await sink.emit(trade, alert)
            except Exception:  # a failing sink must not stall the pipeline
                logger.exception("sink %s failed to emit alert", type(sink).__name__)

    @property
    def running(self) -> bool:
        return self._running

    async def close(self) -> None:
        for sink in self.sinks:
            await sink.close()
