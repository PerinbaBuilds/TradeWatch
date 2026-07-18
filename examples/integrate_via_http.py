"""Integrate a running TradeWatch service over HTTP.

Start the server first:  tradewatch serve --no-simulator
Then run:                python examples/integrate_via_http.py

This shows the integration contract: POST a trade, get back a real-time
anomaly decision you can act on (block, hold, escalate to a case, ...).
"""

from __future__ import annotations

import json
import urllib.request

BASE = "http://localhost:8000"


def post_trade(trade: dict) -> dict:
    req = urllib.request.Request(
        f"{BASE}/trades",
        data=json.dumps(trade).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


if __name__ == "__main__":
    # Warm up with normal flow.
    for i in range(60):
        post_trade({"symbol": "EURUSD", "price": 1.0850 + (i % 4) * 0.0001, "quantity": 1_000_000})

    # A clearly anomalous print.
    decision = post_trade({"symbol": "EURUSD", "price": 1.2200, "quantity": 1_000_000})
    print("anomalous:", decision["anomalous"])
    for a in decision["alerts"]:
        print(f"  {a['severity'].upper()} {a['detector']}: {a['reason']}")
