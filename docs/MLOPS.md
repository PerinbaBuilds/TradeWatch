# MLOps & Observability

TradeWatch pairs a fast, explainable **rule engine** on the hot path with an
offline **data-science layer** for modelling, experiment tracking and monitoring.
This document covers that layer: feature engineering, model training and
evaluation, experiment tracking with MLflow, the analysis notebook, and
Prometheus/Grafana monitoring.

---

## 1. Feature engineering

`tradewatch.ml.features.FeatureBuilder` replays trades through the same
per-symbol event-time window the engine uses and emits one numeric row per trade:

| Feature | Meaning |
|---|---|
| `price_z` | \|z-score\| of price vs the rolling mean |
| `price_pct_move` | absolute tick-to-tick % move |
| `size_vs_median` | quantity ÷ rolling median quantity (robust) |
| `trades_in_5s` | trade count for the symbol in the last 5 s |
| `side_imbalance` | signed buy/sell imbalance in the window `[-1, 1]` |
| `notional_log` | `log1p(price × quantity)` |
| `spread_from_last` | signed % move from the previous print |
| `qty_z` | z-score of quantity vs the rolling mean |

**Leakage-safe by construction:** every feature for trade *t* is computed from
trades strictly *before* t (the window excludes the current trade), so a model
cannot cheat by looking at the event it is trying to classify.

```python
from tradewatch.ml.features import FeatureBuilder
fb = FeatureBuilder()
row = fb.transform_one(trade)      # np.ndarray, one row of FEATURE_NAMES
X   = fb.transform(list_of_trades) # (n, n_features) matrix, in order
```

---

## 2. Training & evaluation

`tradewatch train` (or `python -m tradewatch.ml.train`) runs the full pipeline:

1. generate a labelled trade tape from the simulator,
2. engineer the feature matrix,
3. train two complementary models on a **time-ordered** split (train on the
   past, test on the future — no shuffling, because this is a stream):
   - **Gradient-Boosted Trees** — supervised, uses the labels,
   - **Isolation Forest** — unsupervised, label-free (like the online detector),
4. evaluate both with **precision, recall, F1, ROC-AUC and PR-AUC**, a confusion
   matrix, and permutation feature importances,
5. select the **champion** (highest test F1) and persist artifacts.

```bash
tradewatch train --trades 40000 --seed 7
```

```
  model                     P      R     F1    ROC     PR
  gradient_boosting     0.886  0.771  0.825  0.936  0.830
  isolation_forest      0.600  0.066  0.119  0.688  0.390
  champion     : gradient_boosting  (persisted to models/champion_model.joblib)
  top features : trades_in_5s (+0.575), price_pct_move (+0.076), side_imbalance (+0.041)
```

### Artifacts (written to `models/`, git-ignored)

| File | Contents |
|---|---|
| `champion_model.joblib` | the serialized winning model (load with `joblib.load`) |
| `metrics.json` | machine-readable metrics, dataset stats, feature importances |
| `MODEL_CARD.md` | auto-generated model card — task, data, performance, limits |

> Why PR-AUC matters here: surveillance data is highly imbalanced (~2% positive),
> so accuracy is misleading. Precision-Recall AUC is the honest headline metric.

---

## 3. Experiment tracking with MLflow

When the `[ml]` extra is installed, every `tradewatch train` run is logged to
**MLflow** — parameters, metrics, the model card and the serialized model — so
runs are versioned and comparable.

```bash
pip install -e ".[ml]"
tradewatch train --trades 40000        # logs a run
mlflow ui --backend-store-uri sqlite:///models/mlflow.db   # → http://localhost:5000
```

By default runs go to a local SQLite store (`models/mlflow.db`). Point
`MLFLOW_TRACKING_URI` at a remote MLflow server to centralize tracking across a
team. If MLflow is not installed, training still runs and writes the local
metrics/model-card files — tracking simply no-ops.

---

## 4. Analysis notebook

`notebooks/anomaly_analysis.ipynb` is the data-scientist's walkthrough: dataset
construction, class balance, feature separability and correlation, model
training, **ROC / PR curves**, a confusion matrix, and permutation feature
importances — all rendered from the same feature pipeline the engine uses.

```bash
pip install -e ".[ml]" jupyter
jupyter lab notebooks/anomaly_analysis.ipynb
```

---

## 5. Monitoring — Prometheus & Grafana

The service exposes a Prometheus scrape endpoint at **`GET /metrics`** (standard
text exposition format, no client library required):

| Metric | Type | Meaning |
|---|---|---|
| `tradewatch_trades_total` | counter | trades processed |
| `tradewatch_alerts_total` | counter | alerts raised |
| `tradewatch_alerts_by_detector_total` | counter | alerts labelled by `detector` |
| `tradewatch_alerts_by_severity_total` | counter | alerts labelled by `severity` |
| `tradewatch_latency_microseconds` | summary | per-event latency p50/p95/p99 |
| `tradewatch_trades_per_second` | gauge | recent ingestion rate |
| `tradewatch_pipeline_running` | gauge | 1 if the background pipeline is live |

Bring up a monitoring stack:

```bash
# scrape config + dashboard ship in deploy/
docker run -p 9090:9090 \
  -v $PWD/deploy/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
# then import deploy/grafana/tradewatch-dashboard.json into Grafana
```

The Grafana dashboard (`deploy/grafana/tradewatch-dashboard.json`) has throughput,
latency-quantile, alerts-by-detector and alerts-by-severity panels out of the box.

---

## 6. Testing the ML/observability layer

The whole layer is covered by the test suite and CI quality gates:

- `tests/test_ml.py` — feature shape/finiteness/leakage-safety + a training smoke
  test that asserts ROC-AUC > 0.7 and that all artifacts are written.
- `tests/test_properties.py` — **Hypothesis** property-based tests: the engine
  never crashes on any valid trade, alerts are always well-formed and
  severity-ordered, and windows stay bounded.
- `tests/test_prometheus.py` — validates the `/metrics` exposition format.
- `scripts/loadtest.py` — async load/stress test reporting throughput and client
  latency percentiles.

See [TESTING.md](TESTING.md) for the full testing strategy.
