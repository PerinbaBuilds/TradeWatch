"""TradeWatch — Real-Time Trade Anomaly Detection Engine.

A streaming engine that ingests trade events and flags anomalous market
activity (price/volume spikes, spoofing, wash trades, trade-velocity bursts
and multivariate outliers) in real time.

The public surface is intentionally small so the engine can be embedded in an
existing service, run behind the bundled FastAPI app, or driven from the CLI.
"""

from __future__ import annotations

from .engine import DetectionEngine
from .models import Alert, Severity, Side, Trade
from .pipeline import Pipeline

__all__ = [
    "Alert",
    "DetectionEngine",
    "Pipeline",
    "Severity",
    "Side",
    "Trade",
]

__version__ = "1.0.0"
