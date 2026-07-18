"""Detector interface.

A detector inspects a single incoming trade against the current per-symbol
window state and optionally returns an :class:`Alert`. Detectors are pure with
respect to the window (they read it; the engine owns mutation), which keeps
them independently testable and cheap to compose.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import Alert, Trade
from ..windows import SymbolWindow


class Detector(ABC):
    """Base class for all anomaly detectors."""

    #: Stable identifier used in alerts, metrics and config.
    name: str = "detector"

    @abstractmethod
    def inspect(self, trade: Trade, window: SymbolWindow) -> Alert | None:
        """Return an :class:`Alert` if ``trade`` looks anomalous, else ``None``.

        ``window`` already includes ``trade`` as its most recent element.
        """

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"<Detector {self.name}>"
