# Testing Strategy

TradeWatch is tested at every level a reviewer would expect from production
software — not just unit tests, but property-based, statistical, performance,
load and integration testing, each wired into CI as a gate.

| Layer | What it proves | Where | In CI |
|---|---|---|---|
| **Unit** | Each detector, window, model, guardrail behaves in isolation | `tests/test_*.py` | ✅ every push |
| **Integration** | The FastAPI app, security middleware and `/metrics` work end-to-end | `tests/test_api.py`, `test_security.py`, `test_prometheus.py` | ✅ |
| **Property-based** | Invariants hold for *any* valid input (fuzzed by Hypothesis) | `tests/test_properties.py` | ✅ |
| **Statistical / model** | Detectors hit precision/recall targets; the ML model beats chance | `tests/test_ml.py`, `tradewatch evaluate` | ✅ quality gate |
| **A/B** | The shipped ruleset beats challenger configs on identical data | `tradewatch evaluate` across seeds | on demand |
| **Performance** | Per-event latency stays within the sub-200 ms SLA | `tradewatch bench` | ✅ latency gate |
| **Load / stress** | The API sustains concurrent traffic without errors | `scripts/loadtest.py` | on demand |
| **Coverage** | ~80% line coverage, tracked | `pytest --cov` | ✅ |

---

## Unit & integration

```bash
pytest -q                      # full suite
pytest --cov=tradewatch --cov-report=term-missing
```

Every detector is unit-tested against hand-built windows with known-anomalous and
known-normal inputs. The API is tested through FastAPI's `TestClient`, including
auth, rate limiting, guardrail rejection and the audit trail.

## Property-based (Hypothesis)

Unit tests check known cases; property tests check *invariants* over a fuzzed
input space, and shrink any failure to a minimal counter-example:

- the engine never raises on any valid trade sequence;
- every alert is well-formed (`0 ≤ score ≤ 1`, references its trade, has a reason);
- returned alerts are always ordered most-severe-first;
- a `SymbolWindow` never exceeds its count cap however many trades arrive.

```bash
pytest tests/test_properties.py -q
```

## Statistical / detection quality

`tradewatch evaluate` streams labelled data through the engine and scores it with
**event-based** precision / recall / F1 — the standard for surveillance systems.
CI fails the build if detection quality regresses.

```bash
tradewatch evaluate --trades 15000 --seed 7
```

Typical result: **F1 ≈ 0.81, recall ≈ 0.96, <1% false-alarm rate**, with per-pattern
recall (price spike, spoofing, velocity, volume, wash trade) all ≥ 92%.

## A/B testing

The shipped ruleset is validated against challenger configurations on *identical*
seeded data across multiple seeds, comparing mean F1 with variance — so tuning
choices are evidence-based, not guesses. (The production config wins by
ΔF1 ≈ +0.12 with the lowest run-to-run variance.)

## Model evaluation

`tradewatch train` evaluates supervised + unsupervised models with ROC-AUC,
PR-AUC, a confusion matrix and permutation importances, writing a model card.
`tests/test_ml.py` asserts the trained classifier clears ROC-AUC > 0.7 and that
all artifacts are produced.

## Performance (latency)

```bash
tradewatch bench --trades 40000
```

Reports p50/p95/p99 per-event latency for the rule core and the full engine. CI
enforces the sub-200 ms SLA; the rule core runs with ~600× headroom (p99 ≈ 0.3 ms).

## Load / stress

```bash
tradewatch serve --no-simulator        # terminal 1
python scripts/loadtest.py --requests 8000 --concurrency 32   # terminal 2
```

Fires concurrent `POST /trades` and reports achieved throughput and client-side
latency percentiles. Use it to size workers/replicas for your traffic.

---

## Continuous integration

`.github/workflows/ci.yml` runs on every push across Python 3.10–3.12:

1. **lint** — `ruff` across all source trees;
2. **test + coverage** — the full `pytest` suite (unit, integration, property, ML);
3. **detection quality gate** — `tradewatch evaluate`;
4. **latency gate** — `tradewatch bench`;
5. **big-data smoke** — Spark backtest + Hadoop MapReduce on sample data;
6. **docker** — build the API + batch images and validate every compose stack.

A change that breaks a test, drops detection quality, or blows the latency budget
fails the build before it can merge.
