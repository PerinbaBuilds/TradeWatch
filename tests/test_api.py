from __future__ import annotations

from fastapi.testclient import TestClient

from tradewatch.api.app import create_app
from tradewatch.config import Settings


def _client() -> TestClient:
    # Disable the background simulator so tests are deterministic and fast.
    settings = Settings(simulator_enabled=False)
    return TestClient(create_app(settings))


def test_health_and_stats():
    with _client() as client:
        assert client.get("/health").json()["status"] == "ok"
        stats = client.get("/stats").json()
        assert stats["trades_processed"] == 0
        assert "zscore" in stats["detectors"]


def test_config_endpoint_exposes_rules():
    with _client() as client:
        cfg = client.get("/config").json()
        assert cfg["zscore"]["price_threshold"] > 0


def test_post_trade_returns_decision():
    with _client() as client:
        resp = client.post("/trades", json={"symbol": "AAPL", "price": 100.0, "quantity": 10.0})
        body = resp.json()
        assert resp.status_code == 200
        assert "trade_id" in body
        assert body["anomalous"] is False  # first-ever trade can't be anomalous


def test_post_trade_flags_wash_trade():
    with _client() as client:
        buy = {"symbol": "XYZ", "price": 10.0, "quantity": 5.0, "side": "buy", "account_id": "a1"}
        sell = {"symbol": "XYZ", "price": 10.0, "quantity": 5.0, "side": "sell", "account_id": "a1"}
        client.post("/trades", json=buy)
        resp = client.post("/trades", json=sell)
        body = resp.json()
        assert body["anomalous"] is True
        assert any(a["detector"] == "wash_trade" for a in body["alerts"])


def test_dashboard_served():
    with _client() as client:
        resp = client.get("/")
        assert resp.status_code == 200
        assert "TradeWatch" in resp.text


def test_ws_alerts_receives_pushed_alert():
    with _client() as client:
        with client.websocket_connect("/ws/alerts") as ws:
            # Trigger a wash trade to force an alert to be broadcast.
            buy = {"symbol": "WS", "price": 5.0, "quantity": 2.0, "side": "buy", "account_id": "z"}
            sell = {"symbol": "WS", "price": 5.0, "quantity": 2.0, "side": "sell", "account_id": "z"}
            client.post("/trades", json=buy)
            client.post("/trades", json=sell)
            msg = ws.receive_json()
            assert msg["type"] == "alert"
