"""Embed the engine directly in your own code — no server required.

Run: python examples/quickstart.py
"""

from __future__ import annotations

from tradewatch import DetectionEngine, Trade

# 1. Create an engine (uses config/detection_rules.yaml, or built-in defaults).
engine = DetectionEngine()

# 2. Warm it up with some normal market activity for one symbol.
for i in range(60):
    engine.process(Trade(symbol="AAPL", price=195.0 + (i % 5) * 0.05, quantity=100))

# 3. Feed a suspicious print and inspect the alerts it triggers.
suspect = Trade(symbol="AAPL", price=245.0, quantity=100)  # ~26% above the recent mean
alerts = engine.process(suspect)

print(f"Ingested trade {suspect.trade_id} @ {suspect.price}")
if not alerts:
    print("  no anomalies detected")
for alert in alerts:
    print(f"  [{alert.severity.value.upper():>8}] {alert.detector:<14} score={alert.score:.2f} :: {alert.reason}")

# 4. A wash trade (same account both sides, matched size/price) is CRITICAL.
engine.process(Trade(symbol="AAPL", price=200.0, quantity=50, side="buy", account_id="acct_42"))
wash = engine.process(Trade(symbol="AAPL", price=200.0, quantity=50, side="sell", account_id="acct_42"))
for alert in wash:
    if alert.detector == "wash_trade":
        print(f"\nWash trade caught: {alert.reason}")

print("\nEngine stats:", engine.stats())
