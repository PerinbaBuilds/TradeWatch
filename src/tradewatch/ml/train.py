"""Train, evaluate and version anomaly-detection models offline.

This is the MLOps entry point. It:

1. generates a labelled trade tape from the simulator (normal flow + injected
   market-abuse episodes),
2. engineers a leakage-safe feature matrix (`features.FeatureBuilder`),
3. trains two complementary models on a time-ordered split:
     * a **supervised** gradient-boosted classifier (uses the labels), and
     * an **unsupervised** Isolation Forest (label-free, like the online detector),
4. evaluates both with the metrics a reviewer expects — precision, recall, F1,
   ROC-AUC and PR-AUC — plus a confusion matrix and permutation feature
   importances,
5. persists the winning model (`joblib`), a machine-readable `metrics.json`, and
   a human-readable `MODEL_CARD.md`,
6. logs params, metrics and artifacts to **MLflow** when it is installed, so runs
   are tracked and comparable (falls back to local files otherwise).

Run it with ``tradewatch train`` or ``python -m tradewatch.ml.train``.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier, IsolationForest
from sklearn.inspection import permutation_importance
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from ..sources import MarketSimulator
from .features import FEATURE_NAMES, FeatureBuilder


@dataclass
class ModelMetrics:
    model: str
    precision: float
    recall: float
    f1: float
    roc_auc: float
    pr_auc: float
    tn: int
    fp: int
    fn: int
    tp: int


def _build_dataset(trades: int, anomaly_rate: float, seed: int) -> tuple[np.ndarray, np.ndarray]:
    sim = MarketSimulator(
        symbols=["AAPL", "MSFT", "BTC-USD", "ETH-USD", "TSLA"],
        anomaly_rate=anomaly_rate,
        seed=seed,
    )
    builder = FeatureBuilder()
    X, y = [], []
    for trade, label in sim.labeled_batch(trades):
        X.append(builder.transform_one(trade))
        y.append(0 if label is None else 1)
    return np.vstack(X), np.asarray(y, dtype=int)


def _evaluate(name: str, y_true: np.ndarray, y_pred: np.ndarray, y_score: np.ndarray) -> ModelMetrics:
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    return ModelMetrics(
        model=name,
        precision=float(precision_score(y_true, y_pred, zero_division=0)),
        recall=float(recall_score(y_true, y_pred, zero_division=0)),
        f1=float(f1_score(y_true, y_pred, zero_division=0)),
        roc_auc=float(roc_auc_score(y_true, y_score)) if len(set(y_true)) > 1 else 0.0,
        pr_auc=float(average_precision_score(y_true, y_score)) if len(set(y_true)) > 1 else 0.0,
        tn=int(tn), fp=int(fp), fn=int(fn), tp=int(tp),
    )


def train(
    trades: int = 40000,
    anomaly_rate: float = 0.02,
    seed: int = 7,
    test_fraction: float = 0.3,
    out_dir: str = "models",
) -> dict:
    """Train, evaluate, persist. Returns the metrics dict."""
    t0 = time.perf_counter()
    X, y = _build_dataset(trades, anomaly_rate, seed)

    # Time-ordered split — no shuffling, because this is a stream.
    split = int(len(X) * (1 - test_fraction))
    X_tr, X_te = X[:split], X[split:]
    y_tr, y_te = y[:split], y[split:]

    # --- supervised: gradient-boosted trees ---
    clf = GradientBoostingClassifier(random_state=seed)
    clf.fit(X_tr, y_tr)
    p_score = clf.predict_proba(X_te)[:, 1]
    p_pred = (p_score >= 0.5).astype(int)
    sup = _evaluate("gradient_boosting", y_te, p_pred, p_score)

    # --- unsupervised: Isolation Forest (label-free) ---
    iso = IsolationForest(contamination=anomaly_rate, random_state=seed, n_estimators=200)
    iso.fit(X_tr)
    # decision_function: higher = more normal; invert so higher = more anomalous.
    iso_score = -iso.decision_function(X_te)
    iso_pred = (iso.predict(X_te) == -1).astype(int)
    unsup = _evaluate("isolation_forest", y_te, iso_pred, iso_score)

    # Champion = best F1.
    champion = clf if sup.f1 >= unsup.f1 else iso
    champion_name = sup.model if sup.f1 >= unsup.f1 else unsup.model

    # Permutation importance on the champion (uses labels for scoring).
    try:
        imp = permutation_importance(champion, X_te, y_te, n_repeats=5, random_state=seed, scoring="f1")
        importances = {FEATURE_NAMES[i]: float(imp.importances_mean[i]) for i in range(len(FEATURE_NAMES))}
        importances = dict(sorted(importances.items(), key=lambda kv: kv[1], reverse=True))
    except Exception:
        importances = {}

    elapsed = time.perf_counter() - t0
    result = {
        "dataset": {"trades": int(len(X)), "anomaly_rate": anomaly_rate, "seed": seed,
                    "positives": int(y.sum()), "train": int(len(X_tr)), "test": int(len(X_te))},
        "features": FEATURE_NAMES,
        "models": {sup.model: asdict(sup), unsup.model: asdict(unsup)},
        "champion": champion_name,
        "feature_importance": importances,
        "train_seconds": round(elapsed, 2),
    }

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    joblib.dump(champion, out / "champion_model.joblib")
    (out / "metrics.json").write_text(json.dumps(result, indent=2))
    _write_model_card(out / "MODEL_CARD.md", result)
    _try_mlflow_log(result, champion, champion_name, out)

    _print_report(result)
    return result


def _write_model_card(path: Path, r: dict) -> None:
    champ = r["champion"]
    cm = r["models"][champ]
    imp = r["feature_importance"]
    top = "\n".join(f"| {k} | {v:+.4f} |" for k, v in list(imp.items())[:8]) or "| n/a | n/a |"
    rows = "\n".join(
        f"| {m['model']} | {m['precision']:.3f} | {m['recall']:.3f} | {m['f1']:.3f} "
        f"| {m['roc_auc']:.3f} | {m['pr_auc']:.3f} |"
        for m in r["models"].values()
    )
    path.write_text(f"""# Model Card — TradeWatch anomaly classifier

*Auto-generated by `tradewatch train`. Do not edit by hand.*

## Overview
- **Task:** binary classification of market-abuse / bad-data trades on the
  engineered per-trade feature vector.
- **Champion model:** `{champ}` (selected by highest test F1).
- **Training data:** {r['dataset']['trades']:,} simulated trades
  ({r['dataset']['positives']:,} labelled anomalies, {r['dataset']['anomaly_rate']:.1%} rate),
  time-ordered {r['dataset']['train']:,}/{r['dataset']['test']:,} train/test split.

## Performance (held-out test set)
| Model | Precision | Recall | F1 | ROC-AUC | PR-AUC |
|---|---|---|---|---|---|
{rows}

**Champion confusion matrix:** TP={cm['tp']} · FP={cm['fp']} · FN={cm['fn']} · TN={cm['tn']}

## Top feature importances (permutation, champion)
| Feature | Δ F1 when shuffled |
|---|---|
{top}

## Intended use & limitations
- Complements — does not replace — the real-time rule engine. The rules are
  explainable and sub-millisecond; this model captures multivariate patterns for
  backtesting and threshold tuning.
- Trained on **simulated** data. Re-train and re-validate on real venue data
  before any production reliance.
- Features are leakage-safe (computed from strictly-prior trades), but the
  simulator is not real market microstructure.

## Reproduce
```bash
tradewatch train --trades {r['dataset']['trades']} --seed {r['dataset']['seed']}
```
""")


def _try_mlflow_log(result: dict, model, model_name: str, out: Path) -> None:
    try:
        import os

        import mlflow
        import mlflow.sklearn
    except Exception:
        print("  (mlflow not installed — skipped experiment tracking; metrics written to models/)")
        return
    try:
        # MLflow 3.x deprecated the bare-file store; default to a local SQLite
        # backend so `mlflow ui` works out of the box, unless the operator has
        # pointed MLFLOW_TRACKING_URI at a real tracking server.
        if not os.environ.get("MLFLOW_TRACKING_URI"):
            mlflow.set_tracking_uri(f"sqlite:///{(out / 'mlflow.db').resolve()}")
        mlflow.set_experiment("tradewatch-anomaly")
        with mlflow.start_run(run_name=f"{model_name}-{result['dataset']['seed']}"):
            mlflow.log_params({
                "trades": result["dataset"]["trades"],
                "anomaly_rate": result["dataset"]["anomaly_rate"],
                "seed": result["dataset"]["seed"],
                "champion": model_name,
            })
            for m in result["models"].values():
                for k in ("precision", "recall", "f1", "roc_auc", "pr_auc"):
                    mlflow.log_metric(f"{m['model']}_{k}", m[k])
            mlflow.log_artifact(str(out / "metrics.json"))
            mlflow.log_artifact(str(out / "MODEL_CARD.md"))
            mlflow.sklearn.log_model(model, name="model")
        print(f"  mlflow: logged run to {mlflow.get_tracking_uri()}")
    except Exception as exc:  # never let tracking break training
        print(f"  (mlflow logging skipped: {exc})")


def _print_report(r: dict) -> None:
    print("=" * 60)
    print("  TradeWatch — model training report")
    print("=" * 60)
    d = r["dataset"]
    print(f"  dataset      : {d['trades']:,} trades · {d['positives']:,} anomalies "
          f"· {d['train']:,}/{d['test']:,} split")
    print("-" * 60)
    print(f"  {'model':<20}{'P':>7}{'R':>7}{'F1':>7}{'ROC':>7}{'PR':>7}")
    for m in r["models"].values():
        print(f"  {m['model']:<20}{m['precision']:>7.3f}{m['recall']:>7.3f}"
              f"{m['f1']:>7.3f}{m['roc_auc']:>7.3f}{m['pr_auc']:>7.3f}")
    print("-" * 60)
    print(f"  champion     : {r['champion']}  (persisted to models/champion_model.joblib)")
    if r["feature_importance"]:
        top = list(r["feature_importance"].items())[:3]
        print("  top features : " + ", ".join(f"{k} ({v:+.3f})" for k, v in top))
    print(f"  train time   : {r['train_seconds']}s")
    print("=" * 60)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tradewatch train", description="Train + evaluate anomaly models")
    p.add_argument("--trades", type=int, default=40000)
    p.add_argument("--anomaly-rate", type=float, default=0.02)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--test-fraction", type=float, default=0.3)
    p.add_argument("--out", default="models")
    args = p.parse_args(argv)
    train(trades=args.trades, anomaly_rate=args.anomaly_rate, seed=args.seed,
          test_fraction=args.test_fraction, out_dir=args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
