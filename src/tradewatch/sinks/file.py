"""File sink — append alerts as newline-delimited JSON (JSONL).

JSONL is trivially ingestible by log shippers, data warehouses and pandas, so
this doubles as a durable audit trail of everything the engine flagged.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from ..models import Alert, Trade
from .base import AlertSink


class JsonlFileSink(AlertSink):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    async def emit(self, trade: Trade, alert: Alert) -> None:
        line = alert.model_dump_json()
        async with self._lock:
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
