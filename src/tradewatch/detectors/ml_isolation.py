"""Multivariate anomaly detector (Isolation Forest).

The rule-based detectors each look at one dimension. Real anomalies are often
only visible in the *combination* of features — a moderately large trade, at a
slightly-off price, during a mild velocity bump. This detector learns the joint
distribution of per-symbol trade features online and scores each new trade by
how isolated it is.

It trains lazily once enough history exists and periodically retrains on the
most recent buffer, so it adapts to regime changes without a separate pipeline.
scikit-learn is an optional dependency: if it is unavailable the detector
disables itself cleanly instead of breaking the engine.
"""

from __future__ import annotations

from collections import defaultdict, deque

from ..config import IsolationForestConfig
from ..models import Alert, Severity, Trade
from ..windows import SymbolWindow
from .base import Detector

try:  # scikit-learn is optional; degrade gracefully if missing.
    import numpy as np
    from sklearn.ensemble import IsolationForest

    _SKLEARN = True
except Exception:  # pragma: no cover - only hit when extra not installed
    _SKLEARN = False


class IsolationForestDetector(Detector):
    name = "isolation_forest"

    def __init__(self, config: IsolationForestConfig, min_trades: int) -> None:
        self.cfg = config
        self.min_trades = min_trades
        self.available = _SKLEARN and config.enabled
        self._buffers: dict[str, deque[list[float]]] = defaultdict(lambda: deque(maxlen=self.cfg.train_size))
        self._models: dict[str, object] = {}
        self._since_fit: dict[str, int] = defaultdict(int)

    def inspect(self, trade: Trade, window: SymbolWindow) -> Alert | None:
        if not self.available:
            return None

        features = self._features(trade, window)
        if features is None:
            return None

        buf = self._buffers[trade.symbol]
        buf.append(features)
        self._since_fit[trade.symbol] += 1

        model = self._maybe_fit(trade.symbol)
        if model is None:
            return None

        # decision_function: higher == more normal. Convert to [0, 1] anomaly score.
        raw = float(model.decision_function(np.array([features]))[0])
        score = 1.0 / (1.0 + np.exp(6.0 * raw))  # logistic squashing around the boundary
        if score < self.cfg.score_threshold:
            return None

        severity = Severity.HIGH if score >= (self.cfg.score_threshold + 1.0) / 2 else Severity.MEDIUM
        return Alert.build(
            trade=trade,
            detector=self.name,
            severity=severity,
            score=score,
            reason=f"multivariate outlier (model score {score:.2f}) across price/size/velocity features",
            details={"model_score": round(score, 3), "features": [round(f, 4) for f in features]},
        )

    # ------------------------------------------------------------------
    def _features(self, trade: Trade, window: SymbolWindow) -> list[float] | None:
        if not window.ready(self.min_trades):
            return None
        price_mean, price_std = window.price_mean_std()
        vol_mean, vol_std = window.volume_mean_std()
        prev = window.previous_price() or trade.price
        price_z = (trade.price - price_mean) / price_std if price_std > 1e-9 else 0.0
        vol_z = (trade.quantity - vol_mean) / vol_std if vol_std > 1e-9 else 0.0
        ret = (trade.price - prev) / prev if prev > 0 else 0.0
        velocity = window.count_within(5.0, now_ts=trade.timestamp.timestamp())
        return [price_z, vol_z, ret, float(velocity)]

    def _maybe_fit(self, symbol: str) -> object | None:
        buf = self._buffers[symbol]
        if len(buf) < self.cfg.train_size:
            return self._models.get(symbol)
        if symbol not in self._models or self._since_fit[symbol] >= self.cfg.retrain_every:
            model = IsolationForest(
                n_estimators=100,
                contamination=self.cfg.contamination,
                random_state=42,
            )
            model.fit(np.array(list(buf)))
            self._models[symbol] = model
            self._since_fit[symbol] = 0
        return self._models.get(symbol)
