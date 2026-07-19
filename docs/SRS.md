# Software Requirements Specification (SRS)

**Project:** TradeWatch — Real-Time Trade Anomaly Detection Engine
**Version:** 1.0
**Status:** Baseline

---

## 1. Introduction

### 1.1 Purpose
This document specifies the functional and non-functional requirements for
**TradeWatch**, a big-data system that detects anomalous and potentially abusive
trading activity in real time and at scale. It is intended for developers,
reviewers, and integrators.

### 1.2 Scope
TradeWatch ingests a stream of executed trades and evaluates each one against a
battery of statistical, behavioural and machine-learning detectors, emitting
structured, explainable alerts. It provides:

- a low-latency **speed layer** (Apache Kafka → FastAPI detection engine);
- a **batch/scale layer** (Apache Spark / PySpark + Apache Hadoop HDFS &
  MapReduce) for backtesting and distributed detection over a data lake.

Out of scope: order-book (L2) reconstruction, regulatory case-filing workflows,
and trade execution. TradeWatch is a detection/surveillance engine, not certified
compliance software.

### 1.3 Definitions
| Term | Meaning |
|---|---|
| Trade | A single executed transaction (symbol, price, quantity, side, …). |
| Alert | A detector's verdict that a trade is anomalous, with severity, score, reason. |
| Detector | A component that inspects a trade + window state and may raise an alert. |
| Episode | A contiguous run of anomalous trades treated as one event. |
| Speed layer | Real-time, per-event processing path. |
| Scale layer | Distributed batch/streaming processing over historical/large data. |
| Data lake | Durable storage of raw + derived trade data (HDFS / Parquet). |

### 1.4 References
- `docs/SDS.md` — Software Design Specification
- `docs/ARCHITECTURE.md` — architecture overview
- `README.md` — usage and quickstart

---

## 2. Overall description

### 2.1 Product perspective
TradeWatch is a self-contained, embeddable engine with pluggable ingress
(sources) and egress (sinks). It runs standalone (bundled simulator), behind an
HTTP/WebSocket service, on a Kafka event backbone, or as Spark/Hadoop jobs.

### 2.2 User classes
| Class | Needs |
|---|---|
| Surveillance/risk analyst | Timely, explainable alerts; tunable sensitivity. |
| Platform/integration engineer | Simple ingest contract; embeddable API; deploy artifacts. |
| Data engineer | Batch backtesting and distributed detection over historical data. |
| Reviewer / evaluator | Reproducible quality and performance evidence. |

### 2.3 Assumptions & dependencies
- Trades are available as JSON (Kafka/REST) or files (Parquet/JSONL).
- Python 3.10+; optional extras: `kafka` (aiokafka), `spark` (pyspark), Java for Spark/Hadoop.
- Event-time timestamps are reasonably monotonic per symbol.

---

## 3. Functional requirements

### 3.1 Ingestion
- **FR-1** The system shall accept trades from a built-in simulator, an Apache
  Kafka topic, and an HTTP `POST /trades` endpoint.
- **FR-2** The system shall validate every trade (required: `symbol`, `price > 0`,
  `quantity > 0`) and reject/skip malformed records without stopping ingestion.
- **FR-3** The active source shall be selectable via configuration
  (`TRADEWATCH_SOURCE = simulator | kafka`).

### 3.2 Detection
- **FR-4** The engine shall evaluate each trade with the enabled detectors:
  price z-score, price spike, volume spike, trade velocity, spoofing/imbalance,
  wash/self-trade, and an online Isolation Forest.
- **FR-5** Each detector shall use bounded, per-symbol, event-time windows.
- **FR-6** A detector failure shall not halt the pipeline (fail-open).
- **FR-7** Detection thresholds shall be configurable via a YAML ruleset without
  code changes.

### 3.3 Alerting
- **FR-8** Each alert shall include: id, timestamp, trade id, symbol, detector,
  severity (`low|medium|high|critical`), normalized score `[0,1]`, human-readable
  reason, and supporting details.
- **FR-9** The engine shall deduplicate repeat alerts per `(symbol, detector)`
  within a configurable cooldown window.
- **FR-10** Alerts shall be deliverable to console, JSONL file, and WebSocket
  subscribers, and the sink interface shall allow custom sinks.

### 3.4 Interfaces & observability
- **FR-11** The service shall expose `GET /health`, `GET /stats`, `GET /config`,
  `GET /alerts`, `POST /trades`, `WS /ws/alerts`, `WS /ws/trades`, and a dashboard.
- **FR-12** `POST /trades` shall return the anomaly decision synchronously.
- **FR-13** The system shall expose operational metrics (trades processed, alerts
  raised, alerts suppressed, per-detector/severity counts).

### 3.5 Scale layer
- **FR-14** The system shall provide Spark jobs (batch backtest over Parquet;
  Structured Streaming over Kafka) applying the same statistical rules.
- **FR-15** The system shall provide a Hadoop MapReduce (Streaming) job for
  per-symbol batch anomaly detection over the HDFS data lake.
- **FR-16** Spark and MapReduce jobs shall read/write HDFS (`hdfs://`) paths.

### 3.6 Evaluation & benchmarking
- **FR-17** The system shall provide a labelled simulator and an event-based
  evaluation harness reporting precision, recall and F1.
- **FR-18** The system shall provide a latency benchmark reporting p50/p95/p99
  and throughput.

---

## 4. Non-functional requirements

| ID | Category | Requirement |
|---|---|---|
| NFR-1 | Performance | Per-event decision latency **p99 < 200 ms**; rule core p99 < 1 ms. |
| NFR-2 | Accuracy | ≥ 0.90 event recall and < 3% false-alarm rate on the benchmark. |
| NFR-3 | Reliability | No single detector or sink failure may stop ingestion. |
| NFR-4 | Scalability | Batch/stream detection shall scale horizontally via Spark/Hadoop. |
| NFR-5 | Configurability | Thresholds and runtime settings via YAML + env vars (12-factor). |
| NFR-6 | Portability | Runs on Linux/macOS, Python 3.10–3.12, and in Docker. |
| NFR-7 | Maintainability | Lint-clean; unit/integration tested; CI-gated on quality + latency. |
| NFR-8 | Explainability | Every alert carries a human-readable reason and evidence. |
| NFR-9 | Security | No secrets in code; container runs as non-root; input validated. |

---

## 5. Acceptance criteria
- All functional requirements demonstrable via CLI, HTTP, or jobs.
- CI passes: tests, lint, detection-quality gate, latency gate, Spark+Hadoop
  smoke, Docker build.
- Evaluation meets NFR-1 and NFR-2 on the reference benchmark.
