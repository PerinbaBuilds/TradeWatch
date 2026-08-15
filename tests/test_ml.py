"""Tests for the offline ML layer: feature engineering + training."""

from __future__ import annotations

import numpy as np

from tradewatch.ml.features import FEATURE_NAMES, FeatureBuilder
from tradewatch.ml.train import train

from .conftest import make_trade


def test_feature_row_shape_and_names():
    fb = FeatureBuilder()
    row = fb.transform_one(make_trade())
    assert row.shape == (len(FEATURE_NAMES),)
    assert row.dtype == np.float64


def test_features_are_finite_and_leakage_safe():
    fb = FeatureBuilder()
    # First trade has no prior history -> features must still be finite (no NaN/inf).
    first = fb.transform_one(make_trade(price=100.0, quantity=10.0))
    assert np.all(np.isfinite(first))
    # A wildly off-market print should push the price z-score up once history exists.
    for i in range(40):
        fb.transform_one(make_trade(price=100.0 + (i % 2) * 0.01, quantity=10.0, offset_seconds=i))
    normal = fb.transform_one(make_trade(price=100.0, quantity=10.0, offset_seconds=41))
    spike = fb.transform_one(make_trade(price=180.0, quantity=10.0, offset_seconds=42))
    price_z_idx = FEATURE_NAMES.index("price_z")
    assert spike[price_z_idx] > normal[price_z_idx]


def test_batch_transform_matrix_shape():
    fb = FeatureBuilder()
    trades = [make_trade(price=100 + i * 0.1, offset_seconds=i) for i in range(50)]
    X = fb.transform(trades)
    assert X.shape == (50, len(FEATURE_NAMES))
    assert np.all(np.isfinite(X))


def test_training_smoke_produces_artifacts(tmp_path):
    result = train(trades=4000, seed=7, out_dir=str(tmp_path))
    # Both models trained and evaluated.
    assert set(result["models"]) == {"gradient_boosting", "isolation_forest"}
    gb = result["models"]["gradient_boosting"]
    # The supervised model should be a real classifier, not a coin flip.
    assert gb["roc_auc"] > 0.7
    assert 0.0 <= gb["precision"] <= 1.0
    # Artifacts persisted.
    assert (tmp_path / "champion_model.joblib").exists()
    assert (tmp_path / "metrics.json").exists()
    assert (tmp_path / "MODEL_CARD.md").exists()
    # Champion is whichever won on F1.
    assert result["champion"] in ("gradient_boosting", "isolation_forest")
