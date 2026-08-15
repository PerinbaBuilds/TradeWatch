"""Tests for the Prometheus /metrics exposition endpoint."""

from __future__ import annotations

from fastapi.testclient import TestClient

from tradewatch.api.app import create_app
from tradewatch.config import Settings


def _client() -> TestClient:
    return TestClient(create_app(Settings(simulator_enabled=False)))


def test_metrics_endpoint_exposition_format():
    with _client() as client:
        for i in range(25):
            client.post("/trades", json={"symbol": "AAPL", "price": 100 + (i % 5), "quantity": 10})
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/plain")
        body = resp.text

        # Well-formed exposition: HELP/TYPE lines and the core series present.
        assert "# HELP tradewatch_trades_total" in body
        assert "# TYPE tradewatch_trades_total counter" in body
        assert "tradewatch_trades_total 25" in body
        assert "tradewatch_latency_microseconds{quantile=\"0.99\"}" in body
        assert "tradewatch_pipeline_running" in body

        # Every non-comment, non-blank line must parse as `name value` or
        # `name{labels} value`.
        for line in body.splitlines():
            if not line or line.startswith("#"):
                continue
            assert line.rsplit(" ", 1)[-1].replace(".", "", 1).replace("-", "", 1).replace("e", "", 1).isdigit() \
                or _is_float(line.rsplit(" ", 1)[-1])


def _is_float(tok: str) -> bool:
    try:
        float(tok)
        return True
    except ValueError:
        return False


def test_metrics_labels_by_detector_and_severity():
    with _client() as client:
        # Force at least one alert with a huge off-market print.
        client.post("/trades", json={"symbol": "AAPL", "price": 100, "quantity": 10})
        for _ in range(30):
            client.post("/trades", json={"symbol": "AAPL", "price": 100, "quantity": 10})
        client.post("/trades", json={"symbol": "AAPL", "price": 9999, "quantity": 10})
        body = client.get("/metrics").text
        assert "tradewatch_alerts_by_detector_total" in body
        assert "tradewatch_alerts_by_severity_total" in body
