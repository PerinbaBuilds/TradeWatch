"""FastAPI application exposing the engine over HTTP + WebSocket.

Endpoints
---------
GET  /health          liveness/readiness probe
GET  /stats           engine + pipeline metrics
GET  /config          effective detection ruleset
GET  /alerts          recent alerts (ring buffer)
POST /trades          ingest one trade and get alerts back (synchronous)
WS   /ws/alerts       live alert stream (for dashboards / SIEM bridges)
WS   /ws/trades       live trade tape
GET  /                bundled real-time dashboard

The app can run the built-in simulator as a background pipeline (default) so a
fresh deployment is immediately alive with data. Point ``TRADEWATCH_SIMULATOR_
ENABLED=false`` at it and feed trades via POST /trades or a Kafka source when
wiring it to a real feed.
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from ..config import DetectionConfig, Settings
from ..engine import DetectionEngine
from ..guardrails import Guardrails
from ..metrics import MetricsCollector
from ..models import Alert, Trade
from ..observability import AuditLogger, configure_logging
from ..pipeline import Pipeline
from ..sinks import Broadcaster, WebSocketSink
from ..sources import MarketSimulator
from .security import (
    RateLimiter,
    RequestContextMiddleware,
    SecurityHeadersMiddleware,
    make_api_key_dependency,
)

logger = logging.getLogger("tradewatch.api")
_DASHBOARD = Path(__file__).resolve().parent / "dashboard.html"


class AppState:
    """Holds long-lived singletons shared across requests."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.config = DetectionConfig.load(settings.rules_path)
        self.engine = DetectionEngine(self.config)
        self.broadcaster = Broadcaster(history=settings.alert_buffer_size)
        self.metrics = MetricsCollector()
        self.guardrails = Guardrails() if settings.guardrails_enabled else None
        self.audit = AuditLogger(settings.audit_log_path)
        self.rate_limiter = RateLimiter(settings.rate_limit_per_min)
        self.pipeline: Pipeline | None = None
        self._task: asyncio.Task | None = None

    async def start_source(self) -> None:
        """Start the background pipeline for the configured trade source."""
        source_name = (self.settings.source or "simulator").lower()

        if source_name == "kafka":
            source = self._build_kafka_source()
            label = f"kafka topic '{self.settings.kafka_topic}'"
        else:
            if not self.settings.simulator_enabled:
                return
            source = MarketSimulator(
                symbols=self.settings.symbol_list(),
                trades_per_second=self.settings.simulator_trades_per_second,
                anomaly_rate=self.settings.simulator_anomaly_rate,
                seed=self.settings.simulator_seed,
            )
            label = "built-in simulator"

        self.pipeline = Pipeline(
            source=source,
            engine=self.engine,
            sinks=[WebSocketSink(self.broadcaster)],
            on_trade=self.broadcaster.publish_trade,
            on_processed=self.metrics.record,
        )
        self._task = asyncio.create_task(self.pipeline.run())
        logger.info("background pipeline started from %s", label)

    def _build_kafka_source(self):
        from ..sources.kafka_source import KafkaTradeSource

        return KafkaTradeSource(
            topic=self.settings.kafka_topic,
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            group_id=self.settings.kafka_group_id,
            auto_offset_reset=self.settings.kafka_auto_offset_reset,
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self.pipeline is not None:
            await self.pipeline.close()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(level=settings.log_level, json_logs=settings.log_json)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        state: AppState = app.state.tw
        await state.start_source()
        try:
            yield
        finally:
            await state.stop()

    app = FastAPI(
        title="TradeWatch",
        description="Real-Time Trade Anomaly Detection Engine",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.tw = AppState(settings)

    # --- security & observability middleware (outermost first) ---
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(RequestContextMiddleware, logger=logger)
    if settings.cors_list():
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_list(),
            allow_methods=["GET", "POST"],
            allow_headers=["*"],
        )

    require_api_key = make_api_key_dependency(settings.api_key, app.state.tw.audit)

    @app.get("/health")
    async def health() -> dict:
        state: AppState = app.state.tw
        return {
            "status": "ok",
            "source": state.settings.source,
            "pipeline_running": bool(state.pipeline and state.pipeline.running),
            "trades_processed": state.engine.trades_processed,
        }

    @app.get("/stats")
    async def stats() -> dict:
        return app.state.tw.engine.stats()

    @app.get("/config")
    async def config() -> dict:
        return app.state.tw.config.model_dump()

    @app.get("/alerts")
    async def alerts(limit: int = 100) -> JSONResponse:
        data = app.state.tw.broadcaster.recent_alerts(limit=limit)
        return JSONResponse(data)

    @app.get("/api/metrics")
    async def api_metrics(window: int = 120) -> JSONResponse:
        """Consolidated snapshot powering the analytics dashboard."""
        state: AppState = app.state.tw
        snap = state.metrics.snapshot(ts_window=window)
        snap["engine"] = state.engine.stats()
        snap["source"] = state.settings.source
        snap["pipeline_running"] = bool(state.pipeline and state.pipeline.running)
        return JSONResponse(snap)

    @app.post("/trades", dependencies=[Depends(require_api_key)])
    async def ingest_trade(trade: Trade, request: Request) -> dict:
        """Ingest a single trade synchronously and return any alerts.

        The integration entry point: any system can POST trades here and receive
        real-time anomaly decisions. Guarded by optional API key, a per-client
        rate limit, and input guardrails; every ingest/rejection is audited.
        """
        state: AppState = app.state.tw
        client = request.client.host if request.client else "?"

        if not state.rate_limiter.allow(client):
            state.audit.record("rate_limited", outcome="denied", client=client)
            raise HTTPException(status_code=429, detail="rate limit exceeded")

        if state.guardrails is not None:
            verdict = state.guardrails.check(trade)
            if not verdict.ok:
                state.audit.record(
                    "trade_rejected", outcome="rejected", client=client,
                    symbol=trade.symbol, reason=verdict.reason, code=verdict.code,
                )
                raise HTTPException(status_code=422, detail=f"guardrail: {verdict.reason}")

        state.broadcaster.publish_trade(trade)
        start = time.perf_counter_ns()
        raised: list[Alert] = state.engine.process(trade)
        latency_us = (time.perf_counter_ns() - start) / 1000.0
        state.metrics.record(trade, raised, latency_us)
        for alert in raised:
            state.broadcaster.publish_alert(alert)

        state.audit.record(
            "trade_ingested", trade_id=trade.trade_id, symbol=trade.symbol,
            anomalous=bool(raised), alerts=len(raised), client=client,
        )
        return {
            "trade_id": trade.trade_id,
            "alerts": [a.model_dump(mode="json") for a in raised],
            "anomalous": bool(raised),
        }

    @app.websocket("/ws/alerts")
    async def ws_alerts(ws: WebSocket) -> None:
        await _pump(ws, app.state.tw.broadcaster, kind="alerts")

    @app.websocket("/ws/trades")
    async def ws_trades(ws: WebSocket) -> None:
        await _pump(ws, app.state.tw.broadcaster, kind="trades")

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> HTMLResponse:
        if _DASHBOARD.exists():
            return HTMLResponse(_DASHBOARD.read_text(encoding="utf-8"))
        return HTMLResponse("<h1>TradeWatch</h1><p>Dashboard asset missing.</p>")

    return app


async def _pump(ws: WebSocket, broadcaster: Broadcaster, kind: str) -> None:
    await ws.accept()
    if kind == "alerts":
        queue = broadcaster.subscribe_alerts()
        unsubscribe = broadcaster.unsubscribe_alerts
        # Replay recent context to a freshly-connected client.
        for item in broadcaster.recent_alerts(limit=50):
            await ws.send_json({"type": "alert", "data": item})
    else:
        queue = broadcaster.subscribe_trades()
        unsubscribe = broadcaster.unsubscribe_trades
    try:
        while True:
            payload = await queue.get()
            await ws.send_json(payload)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.debug("websocket pump ended", exc_info=True)
    finally:
        unsubscribe(queue)


# Module-level app for `uvicorn tradewatch.api.app:app`.
app = create_app()
