"""Console sink — human-readable coloured alerts for the CLI."""

from __future__ import annotations

import sys

from ..models import Alert, Severity, Trade
from .base import AlertSink

_COLOR = {
    Severity.LOW: "\033[36m",       # cyan
    Severity.MEDIUM: "\033[33m",    # yellow
    Severity.HIGH: "\033[35m",      # magenta
    Severity.CRITICAL: "\033[31m",  # red
}
_RESET = "\033[0m"


class ConsoleSink(AlertSink):
    def __init__(self, use_color: bool | None = None) -> None:
        self.use_color = sys.stdout.isatty() if use_color is None else use_color

    async def emit(self, trade: Trade, alert: Alert) -> None:
        ts = alert.timestamp.strftime("%H:%M:%S")
        tag = f"[{alert.severity.value.upper():>8}]"
        if self.use_color:
            tag = f"{_COLOR[alert.severity]}{tag}{_RESET}"
        print(
            f"{ts} {tag} {alert.symbol:<8} {alert.detector:<16} "
            f"score={alert.score:.2f} :: {alert.reason}",
            flush=True,
        )
