"""End-to-end smoke test: boot the app in-process and exercise every endpoint.

Uses FastAPI's TestClient (no external server needed). Exits non-zero on any
failure so it can double as a CI/deploy gate.
"""

from __future__ import annotations

import sys

from fastapi.testclient import TestClient

from tradewatch.api.app import create_app
from tradewatch.config import Settings


def main() -> int:
    app = create_app(Settings(simulator_enabled=False))
    with TestClient(app) as client:
        assert client.get("/health").json()["status"] == "ok"
        assert "zscore" in client.get("/stats").json()["detectors"]
        assert client.get("/config").json()["zscore"]["price_threshold"] > 0
        assert "TradeWatch" in client.get("/").text

        # Warm up, then send a clear anomaly.
        for i in range(60):
            client.post("/trades", json={"symbol": "AAPL", "price": 195 + (i % 4) * 0.05, "quantity": 100})
        decision = client.post("/trades", json={"symbol": "AAPL", "price": 260.0, "quantity": 100}).json()
        assert decision["anomalous"], "expected anomaly on a large price outlier"

        recent = client.get("/alerts").json()
        assert isinstance(recent, list) and recent, "recent alerts should be populated"

    print("smoke test OK — all endpoints healthy, anomaly path verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
