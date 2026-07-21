from __future__ import annotations

from fastapi.testclient import TestClient

from tradewatch.api.app import create_app
from tradewatch.api.security import RateLimiter
from tradewatch.config import Settings
from tradewatch.guardrails import GuardrailConfig, Guardrails

from .conftest import make_trade


# ---- guardrails --------------------------------------------------------------
def test_guardrails_accept_normal_trade():
    g = Guardrails()
    assert g.check(make_trade(price=100, quantity=10)).ok


def test_guardrails_reject_absurd_values():
    g = Guardrails(GuardrailConfig(max_price=1000, max_quantity=1000, max_notional=1e6))
    assert not g.check(make_trade(price=5000, quantity=1)).ok
    assert g.check(make_trade(price=5000, quantity=1)).code == "price_bound"
    assert not g.check(make_trade(price=1, quantity=5000)).ok


def test_guardrails_symbol_cardinality_cap():
    g = Guardrails(GuardrailConfig(max_symbol_cardinality=2))
    assert g.check(make_trade(symbol="AAA", price=1, quantity=1)).ok
    assert g.check(make_trade(symbol="BBB", price=1, quantity=1)).ok
    res = g.check(make_trade(symbol="CCC", price=1, quantity=1))
    assert not res.ok and res.code == "cardinality"


def test_rate_limiter():
    rl = RateLimiter(per_minute=3)
    assert all(rl.allow("client") for _ in range(3))
    assert not rl.allow("client")
    assert rl.allow("other")  # per-key


# ---- api security ------------------------------------------------------------
def _client(**kw) -> TestClient:
    return TestClient(create_app(Settings(simulator_enabled=False, **kw)))


def test_security_headers_present():
    with _client() as client:
        r = client.get("/health")
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        assert r.headers["X-Frame-Options"] == "DENY"
        assert "Content-Security-Policy" in r.headers
        assert "X-Request-ID" in r.headers


def test_api_key_enforced_when_set():
    with _client(api_key="secret") as client:
        payload = {"symbol": "AAPL", "price": 100.0, "quantity": 5.0}
        assert client.post("/trades", json=payload).status_code == 401
        ok = client.post("/trades", json=payload, headers={"X-API-Key": "secret"})
        assert ok.status_code == 200


def test_api_key_disabled_by_default():
    with _client() as client:
        r = client.post("/trades", json={"symbol": "AAPL", "price": 100.0, "quantity": 5.0})
        assert r.status_code == 200


def test_guardrail_rejects_bad_trade_via_api():
    with _client() as client:
        # symbol with illegal characters -> guardrail rejection (422)
        r = client.post("/trades", json={"symbol": "AA PL$", "price": 100.0, "quantity": 5.0})
        assert r.status_code == 422
        assert "guardrail" in r.json()["detail"]


def test_rate_limit_via_api():
    with _client(rate_limit_per_min=5) as client:
        payload = {"symbol": "AAPL", "price": 100.0, "quantity": 5.0}
        codes = [client.post("/trades", json=payload).status_code for _ in range(7)]
        assert codes.count(200) == 5
        assert codes.count(429) == 2
