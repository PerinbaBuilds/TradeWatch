# Software Design Specification (SDS)

**Project:** TradeWatch — Real-Time Trade Anomaly Detection Engine
**Version:** 1.0
**Companion to:** `docs/SRS.md`

---

## 1. Introduction

### 1.1 Purpose
This document describes the design that realises the requirements in the SRS:
the architecture, components, data model, key algorithms, interfaces, and the
design decisions behind them.

### 1.2 Design goals
- **Low latency** on the per-event hot path (NFR-1).
- **Separation of concerns** — source, engine, sink are independent contracts.
- **Same rules everywhere** — real-time, Spark, and MapReduce agree.
- **Fail-open resilience** and **explainability** by construction.

---

## 2. System architecture

TradeWatch follows a **Lambda-style architecture** with a real-time speed layer
and a distributed batch/scale layer over a shared data lake.

```
Producers ─▶ Apache Kafka ─▶ Speed layer:  FastAPI + DetectionEngine ─▶ Sinks/Dashboard
                        └──▶ Data lake (HDFS) ─▶ Scale layer: Spark (batch+stream) + Hadoop MapReduce
```

### 2.1 Layered view
| Layer | Responsibility | Modules |
|---|---|---|
| Ingress | Produce trades | `sources/` (simulator, kafka), REST `POST /trades` |
| Core | Decide per trade | `engine.py`, `windows.py`, `detectors/` |
| Egress | Deliver alerts | `sinks/` (console, file, websocket) |
| Service | HTTP/WS + dashboard | `api/` |
| Scale | Distributed detection | `spark/`, `hadoop/` |
| Tooling | Quality & perf | `evaluate.py`, `benchmark.py`, `cli.py` |

### 2.2 Design decisions (rationale)
- **Synchronous engine core.** The hot path is CPU-bound (window update + O(window)
  detector checks); a synchronous core avoids per-trade coroutine overhead and is
  trivially embeddable/testable. Concurrency lives at the I/O edges.
- **Event-time windows.** Detectors keyed on trade timestamps (not arrival time)
  are correct under replay/back-pressure and make offline evaluation faithful.
- **Alert dedup/cooldown.** Collapses multi-tick episodes into one actionable
  alert — the primary lever on the precision/noise trade-off.
- **Rules duplicated in Spark SQL / MapReduce, not imported.** The batch layer
  expresses the same statistical detectors natively so it scales without shipping
  Python objects to executors; a shared schema keeps them aligned.

---

## 3. Component design

### 3.1 Data model (`models.py`)
- `Trade` — validated input (symbol normalized upper-case; price/quantity > 0;
  optional account/counterparty/venue; event `timestamp`).
- `Alert` — output verdict with `severity`, `score∈[0,1]`, `reason`, `details`.
- `Severity` — ordered enum `low < medium < high < critical`.

### 3.2 Windows (`windows.py`)
- `SymbolWindow` — bounded deque of recent trades, evicted by **count and age**;
  provides rolling mean/std/median of price & quantity, previous price,
  time-bounded counts, and side imbalance. All O(window).
- `WindowStore` — lazy per-symbol registry.

### 3.3 Detectors (`detectors/`)
Each implements `inspect(trade, window) -> Alert | None`.

| Detector | Signal | Complexity |
|---|---|---|
| `zscore` | price z-score vs rolling mean/std | O(w) |
| `price_spike` | tick-to-tick % move | O(1) |
| `volume_spike` | quantity vs rolling **median** (robust) | O(w) |
| `velocity` | trades per symbol in short window | O(w) |
| `spoofing` | one-sided burst / imbalance ratio | O(w) |
| `wash_trade` | self-cross (`account == counterparty`) or matched reciprocal | O(w) |
| `isolation_forest` | multivariate outlier score, online-trained | O(trees) |

### 3.4 Engine (`engine.py`)
Orchestrates window update → detector fan-out → dedup → severity-sorted alerts.
Maintains metrics. Wraps each detector in try/except (fail-open). Dedup keyed on
`(symbol, detector)` with event-time cooldown.

### 3.5 Pipeline (`pipeline.py`)
Async driver: `source.stream()` → `engine.process()` → fan-out to sinks.
Cancellation-safe; tolerant of individual sink failures.

### 3.6 Sources & sinks
- Sources: `MarketSimulator` (labelled anomalies), `KafkaTradeSource` (lazy
  aiokafka), REST ingest. Contract: `TradeSource.stream()`.
- Sinks: `ConsoleSink`, `JsonlFileSink`, `Broadcaster`/`WebSocketSink`.
  Contract: `AlertSink.emit(trade, alert)`.

### 3.7 Service (`api/`)
FastAPI app; lifespan starts the configured background pipeline; a `Broadcaster`
fans alerts/trades to WebSocket clients with a replay ring buffer; serves the
zero-dependency dashboard.

### 3.8 Scale layer
- `spark/detection_sql.py` — detectors as Spark SQL window functions + trade schema.
- `spark/batch_backtest.py` — historical Parquet/HDFS backtest.
- `spark/streaming_job.py` — Structured Streaming over Kafka.
- `hadoop/mapper.py` + `reducer.py` — MapReduce (Streaming) per-symbol detection;
  `run_local.sh` runs the identical code via pipes.

---

## 4. Data design

### 4.1 Trade (wire format)
```jsonc
{ "symbol": "AAPL", "price": 194.32, "quantity": 100, "side": "buy",
  "account_id": "a1", "counterparty_id": "a2", "venue": "XNAS",
  "currency": "USD", "timestamp": "2026-01-01T15:00:00Z" }
```

### 4.2 Alert (wire format)
```jsonc
{ "alert_id": "alrt_…", "timestamp": "…", "trade_id": "trd_…", "symbol": "AAPL",
  "detector": "zscore", "severity": "critical", "score": 1.0,
  "reason": "price 999.0 is +42.1σ from mean 194.98", "details": { … } }
```

### 4.3 Storage
- Data lake: JSONL / Parquet on local FS or **HDFS** (`hdfs://namenode:9000/...`).
- Alert audit: append-only JSONL (log-shipper / warehouse friendly).

---

## 5. Key algorithms

### 5.1 Rolling z-score
`z = (price − mean_w) / std_w` over the preceding `w` trades; alert if
`|z| ≥ threshold`, escalate to critical at `threshold × k`.

### 5.2 Robust volume spike
Compare quantity to the rolling **median** (resistant to the very outliers being
detected); alert if `quantity ≥ median × multiplier`.

### 5.3 Wash / self-trade
Primary signal: `account_id == counterparty_id`. Secondary: same account on the
opposite side within a short window at matched price **and** size.

### 5.4 Isolation Forest (online)
Per-symbol feature buffer `[price_z, vol_z, return, velocity]`; lazily fit once
`train_size` samples exist, refit every `retrain_every`; anomaly score via a
logistic squash of `decision_function`.

### 5.5 Dedup/cooldown
Track last emitted event-time per `(symbol, detector)`; suppress repeats within
`alert_cooldown_seconds`.

---

## 6. Interface design
| Interface | Contract |
|---|---|
| REST `POST /trades` | Trade JSON → `{trade_id, anomalous, alerts[]}` |
| REST `GET /health\|/stats\|/config\|/alerts` | JSON status/metrics/config/recent alerts |
| WS `/ws/alerts`, `/ws/trades` | Server-push JSON frames |
| CLI | `serve \| simulate \| evaluate \| bench` |
| Config | YAML ruleset + `TRADEWATCH_*` env vars |

---

## 7. Error handling & resilience
- Invalid trades rejected at the boundary (Pydantic) or skipped (Kafka/MapReduce).
- Detector exceptions caught per-detector; counted, not propagated.
- Sink exceptions logged; pipeline continues.
- Kafka/Spark optional deps raise a clear, actionable error only when used.

---

## 8. Testing strategy
- **Unit:** models, windows, each detector, MapReduce mapper/reducer.
- **Integration:** engine end-to-end, FastAPI endpoints + WebSocket, simulator.
- **Quality gate:** event-based precision/recall/F1 (CI).
- **Performance gate:** per-event latency benchmark (CI).
- **Big-data smoke:** Spark batch + Hadoop MapReduce over generated data (CI).

---

## 9. Deployment
- Container: multi-stage Dockerfile, non-root, healthcheck.
- Compose profiles: default (simulator), `kafka` (broker + producer + consumer),
  `hadoop` (HDFS NameNode + DataNode).
- CI: GitHub Actions across Python 3.10–3.12 + big-data smoke + image build.

---

## 10. Traceability (SRS → design)
| Requirement | Realised by |
|---|---|
| FR-1..3 (ingest) | `sources/`, `api/app.py`, `config.Settings` |
| FR-4..7 (detect) | `engine.py`, `windows.py`, `detectors/`, `detection_rules.yaml` |
| FR-8..10 (alert) | `models.Alert`, engine dedup, `sinks/` |
| FR-11..13 (interfaces) | `api/app.py`, `engine.stats()` |
| FR-14..16 (scale) | `spark/`, `hadoop/` |
| FR-17..18 (eval/bench) | `evaluate.py`, `benchmark.py` |
| NFR-1 | synchronous core, `benchmark.py`, CI latency gate |
| NFR-2 | `evaluate.py`, CI quality gate |
| NFR-3 | per-detector/per-sink try-except |
| NFR-4 | Spark/Hadoop scale layer |
