"""Detector implementations."""

from __future__ import annotations

from .base import Detector
from .ml_isolation import IsolationForestDetector
from .price_spike import PriceSpikeDetector
from .spoofing import SpoofingDetector
from .velocity import VelocityDetector
from .volume_spike import VolumeSpikeDetector
from .wash_trade import WashTradeDetector
from .zscore import ZScoreDetector

__all__ = [
    "Detector",
    "IsolationForestDetector",
    "PriceSpikeDetector",
    "SpoofingDetector",
    "VelocityDetector",
    "VolumeSpikeDetector",
    "WashTradeDetector",
    "ZScoreDetector",
]
