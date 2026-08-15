"""Offline machine-learning layer for TradeWatch.

The real-time engine (`tradewatch.engine`) makes fast, explainable, rule-based
decisions on the hot path. This package is the *data-science* counterpart: it
turns the trade tape into an engineered feature matrix, trains and evaluates
supervised and unsupervised models offline, and records everything (metrics,
model artifact, model card) so a model can be reviewed, versioned and — when it
beats the rules — promoted.

Modules
-------
features  vectorize a stream of trades into a per-trade feature matrix
train     train + evaluate models, persist artifacts, log to MLflow
"""

from .features import FEATURE_NAMES, FeatureBuilder

__all__ = ["FeatureBuilder", "FEATURE_NAMES"]
