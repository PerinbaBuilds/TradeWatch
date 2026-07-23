# TradeWatch — Real-Time Trade Anomaly Detection Engine

<p align="center">
  <b>An end-to-end big-data trade-surveillance platform — Apache Kafka → FastAPI for sub-10ms real-time anomaly alerts, an Apache Spark / PySpark / Databricks + Hadoop (HDFS + MapReduce) scale layer, a Hive + Snowflake warehouse, Airflow-orchestrated ETL, hardened security, and a multi-page analytics console.</b>
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/python-3.10%2B-blue">
  <img alt="FastAPI" src="https://img.shields.io/badge/API-FastAPI-009688">
  <img alt="Apache Kafka" src="https://img.shields.io/badge/streaming-Apache%20Kafka-231F20">
  <img alt="Apache Spark" src="https://img.shields.io/badge/compute-Spark%20%2F%20PySpark%20%2F%20Databricks-E25A1C">
  <img alt="Apache Hadoop" src="https://img.shields.io/badge/lake-Hadoop%20HDFS%20%2F%20MapReduce-66CCFF">
  <img alt="Warehouse" src="https://img.shields.io/badge/warehouse-Hive%20%2F%20Snowflake-29B5E8">
  <img alt="Airflow" src="https://img.shields.io/badge/orchestration-Apache%20Airflow-017CEE">
  <img alt="License" src="https://img.shields.io/badge/license-MIT-green">
  <img alt="Tests" src="https://img.shields.io/badge/tests-53%20passing-brightgreen">
</p>

<p align="center">
  <b>Platform at a glance</b><br/>
  <code>Kafka</code> ➜ <code>FastAPI engine</code> &nbsp;<i>speed layer · sub-10ms/event</i><br/>
  <code>HDFS</code> ➜ <code>Spark / Databricks</code> + <code>Hadoop MapReduce</code> ➜ <code>Hive</code> + <code>Snowflake</code> &nbsp;<i>lakehouse + warehouse</i><br/>
  <code>Apache Airflow</code> <i>orchestrates the daily ETL · security + audit logging throughout</i>
</p>

📄 Docs: [Requirements (SRS)](docs/SRS.md) · [Design (SDS)](docs/SDS.md) · [Architecture](docs/ARCHITECTURE.md) · [Data platform](docs/DATA_PLATFORM.md) · [Security](docs/SECURITY.md)

---

## Why this exists

Exchanges, brokers, prop desks and crypto venues are legally required to
surveil their own order flow for **market abuse** (spoofing, layering, wash
trading, momentum ignition) and to catch **fat-finger / bad-data** events
before they cause losses. Off-the-shelf surveillance suites are expensive,
opaque black boxes.

**TradeWatch** is a compact, transparent surveillance stack built around a
**Lambda-style big-data architecture**:

- a **speed layer** — an **Apache Kafka → FastAPI** pipeline whose engine scores
  every trade against statistical, behavioural and ML detectors and emits
  **structured, explainable alerts** in **sub-10ms**;
- a **batch/scale layer** — **Apache Spark / PySpark** jobs and a **Hadoop
  MapReduce** job that run the same detection logic over a **Hadoop HDFS data
  lake** (backtesting, threshold tuning, baseline bootstrapping) and as
  distributed **Structured Streaming** over Kafka.

The detection core is embeddable too — drop it in any Python process.

> On a labelled 15k-trade benchmark it catches **96%+ of injected manipulation
> episodes at a <1% false-alarm rate** (see [Detection quality](#detection-quality)),
> deciding each trade with a **p99 latency of ~0.3ms** (see [Performance](#performance)).

---

## Highlights

- ⚡ **Real-time streaming core** — synchronous, allocation-light hot path; event-time windows, not wall-clock; **p99 ≈ 0.3ms/event**.
- 🧠 **7 complementary detectors** — statistical, behavioural *and* an online Isolation Forest for multivariate outliers.
- 🔎 **Explainable alerts** — every alert carries a severity, a normalized score, a human-readable reason and the supporting numbers.
- 🔕 **Alert deduplication** — per-(symbol, detector) cooldown collapses a noisy episode into one actionable alert (à la PagerDuty grouping).
- 🟥 **Apache Kafka ingestion** — consume the production trade tape from a Kafka topic; a bundled producer + broker make it one command to demo end-to-end.
- 🔶 **Apache Spark / PySpark scale layer** — the same rules as Spark SQL window functions: a **batch backtest** over historical Parquet and a **Structured Streaming** Kafka job.
- 🔵 **Hadoop big-data layer** — **HDFS** as the data lake and a **MapReduce** (Streaming) job for massive batch anomaly scans; the mapper/reducer are CI-tested via the same pipe Hadoop runs.
- 🏢 **Warehouse + ETL** — **Apache Hive** SQL over the lake, a **Snowflake** gold layer, a **Databricks** job, all orchestrated by an **Apache Airflow** DAG (land → Spark → MapReduce → Hive → Snowflake, with retries + a data-quality gate). See [Data platform](docs/DATA_PLATFORM.md).
- 🔒 **Hardened & audited** — API-key auth + rate limiting + input **guardrails** on the write path, security headers/CSP, CORS allow-list, structured logging with request-ids, and an append-only **audit trail**. See [Security](docs/SECURITY.md).
- 🔌 **Integrate anywhere** — REST (`POST /trades`), WebSocket stream, embedded Python API, or a Kafka consumer.
- 📊 **Multi-page analytics console** — 8-page real-time surveillance dashboard (Overview, Live Feed, Instruments, Analytics, Detectors, Performance, Ruleset, System) with custom canvas charts, per-symbol sparklines, latency histograms and light/dark themes — zero external JS, served by the app itself.
- 🧪 **Measured, not hand-wavy** — a labelled simulator + `tradewatch evaluate` / `tradewatch bench` give you precision/recall/F1 and latency as CI gates.
- 🐳 **Ship it** — Dockerfile, docker-compose (with a Kafka profile), GitHub Actions CI, tunable YAML ruleset, 12-factor config.

---

## Architecture

A **Lambda-style** split: a low-latency speed layer for real-time alerting, and
a Spark scale layer for heavy historical/distributed processing. Both apply the
same detection rules.

```mermaid
flowchart TB
    PROD[Trade producers<br/>market feed · OMS · simulator] --> KAFKA[[Apache Kafka<br/>trades topic]]

    subgraph SPEED[Speed layer — real-time · sub-10ms/event]
        direction TB
        ENG[Detection Engine<br/>7 detectors · event-time windows · dedup]
        API[FastAPI service<br/>REST · WebSocket · live dashboard]
        ENG --> API
    end

    HDFS[(Hadoop HDFS<br/>data lake)]

    subgraph SCALE[Batch / scale layer — orchestrated by Apache Airflow]
        direction TB
        SS[Spark Structured Streaming<br/>windowed detection]
        BB[Spark / Databricks<br/>batch backtest]
        MR[Hadoop MapReduce<br/>Streaming mapper/reducer]
    end

    subgraph WH[Warehouse]
        direction TB
        HV[(Apache Hive<br/>SQL over the lake)]
        SF[(Snowflake<br/>gold layer)]
    end

    KAFKA --> ENG
    REST[REST POST /trades] --> ENG
    KAFKA --> SS
    KAFKA -. archive .-> HDFS
    HDFS --> BB
    HDFS --> MR
    BB & MR --> HV & SF
    BB & MR -. baselines & tuned thresholds .-> ENG

    API --> OUT[/Alerts: dashboard · JSONL audit · Slack / SIEM / Kafka/]
    SS --> OUT
    HV & SF --> BI[Analysts / BI]

    classDef k fill:#231F20,stroke:#555,color:#fff;
    classDef speed fill:#0f766e,stroke:#14b8a6,color:#ecfeff;
    classDef scale fill:#E25A1C,stroke:#f59e6b,color:#fff;
    classDef lake fill:#0369a1,stroke:#38bdf8,color:#f0f9ff;
    classDef wh fill:#164e63,stroke:#29B5E8,color:#e0f7ff;
    class KAFKA k;
    class ENG,API speed;
    class SS,BB,MR scale;
    class HDFS lake;
    class HV,SF wh;
```

**Security & observability** run across the whole stack: the `POST /trades`
write path is guarded by optional API-key auth, per-client rate limiting and
input **guardrails**; every response carries hardening headers/CSP; and an
append-only **audit trail** plus structured, request-id-correlated logs make it
SIEM-ready. See [Security](docs/SECURITY.md).

**Pluggable by design.** A `TradeSource` produces trades; the `DetectionEngine`
turns each trade into zero-or-more `Alert` objects; an `AlertSink` delivers them.
Swap any layer — Kafka, simulator, REST; console, JSONL, WebSocket, your own —
without touching the others.

The **7 detectors** inside the engine: price z-score, price spike, volume spike,
trade velocity, spoofing/imbalance, wash/self-trade, and an online Isolation
Forest (see [Detectors](#detectors)).

### Tech stack

| Layer | Technology | Role |
|---|---|---|
| Ingestion | **Apache Kafka** (`aiokafka`) | Consume the production trade tape; decouple producers from detection |
| Real-time engine | **Python 3.10+**, **scikit-learn** | Event-time windows + 7 detectors incl. online Isolation Forest |
| Service / API | **FastAPI**, **Uvicorn**, WebSocket | REST decisioning, live streams, dashboard |
| Distributed compute | **Apache Spark**, **PySpark**, **Databricks** | Structured Streaming + historical backtesting with Spark SQL; Databricks = managed Spark |
| Data lake + batch | **Apache Hadoop** (HDFS + MapReduce) | Durable storage & massive batch anomaly scans (Streaming job) |
| Warehouse | **Apache Hive**, **Snowflake** | SQL over the lake + a cloud gold layer for BI |
| Orchestration / ETL | **Apache Airflow** | Daily batch pipeline with retries + data-quality gates |
| Security | API-key, rate-limit, guardrails, CSP, audit log | Hardened write path + SIEM-ready audit trail |
| Config / validation | **Pydantic v2**, YAML | Typed models, 12-factor settings, tunable rules |
| Delivery | **Docker**, **docker-compose**, **GitHub Actions** | Containerised stacks + CI quality gates |

---

## Quickstart

### ▶ Run the entire platform, one command

Brings up **Kafka + Hadoop HDFS + Spark cluster + Hive + Airflow + the
TradeWatch API** (with security & logging on) and the producer — all wired
together and running simultaneously. Needs Docker with **~12 GB RAM**.

```bash
docker compose -f docker-compose.full.yml up --build     # macOS/Linux/Windows
#   make stack            (Make shortcut)
#   .\scripts\run_stack.ps1   (Windows PowerShell)
```

Then open:

| Service | URL |
|---|---|
| **Dashboard** (real-time console) | http://localhost:8000 |
| Spark master UI | http://localhost:8080 |
| HDFS NameNode UI | http://localhost:9870 |
| Airflow (ETL orchestration) | http://localhost:8081 · `admin`/`admin` |
| HiveServer2 | `jdbc:hive2://localhost:10000` · UI http://localhost:10002 |

Data flows end-to-end: producer → Kafka → real-time engine + dashboard, while a
**batch runner** continuously runs the Spark + Hadoop MapReduce jobs over the
HDFS lake and Airflow hosts the ETL DAG. Stop with
`docker compose -f docker-compose.full.yml down`.

> Snowflake & Databricks are managed SaaS (no container): the Spark cluster here
> is the runnable equivalent of Databricks, and the Snowflake gold-layer load
> runs in the batch runner when `SNOWFLAKE_*` credentials are set.

### Just the dashboard (lightweight, no Docker)

```bash
pip install -e ".[dev]"
tradewatch serve          # → http://localhost:8000
```

Or a lighter Docker subset:

```bash
docker compose up --build                 # API + dashboard only
docker compose --profile kafka up --build # Kafka → FastAPI pipeline
docker compose --profile hadoop up        # local HDFS
```

Stream to your terminal instead:

```bash
tradewatch simulate --tps 30 --anomaly-rate 0.02
```

Prove it works with hard numbers:

```bash
tradewatch evaluate --trades 15000   # precision / recall / F1
tradewatch bench                     # per-event latency
```

### Run the full Kafka → FastAPI pipeline

One command spins up a Kafka broker, a producer publishing the simulated tape,
and TradeWatch consuming that topic:

```bash
docker compose --profile kafka up --build
#   broker + producer + consumer → http://localhost:8000
```

To point the service at your **own** Kafka without Docker:

```bash
pip install -e ".[kafka]"
TRADEWATCH_SOURCE=kafka \
TRADEWATCH_KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
TRADEWATCH_KAFKA_TOPIC=trades \
  tradewatch serve
```

### Run the Apache Spark / PySpark scale layer

```bash
pip install -e ".[spark]"

# 1. generate a historical dataset (Parquet)
python examples/generate_history.py --out data/trades.parquet --trades 100000

# 2. batch-backtest the detectors across the whole history
python spark/batch_backtest.py --input data/trades.parquet --output data/anomalies.parquet

# 3. distributed Structured Streaming over Kafka
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark/streaming_job.py --bootstrap localhost:9092 --topic trades
```

The batch job prints a per-detector breakdown and writes flagged anomalies to
Parquet; see [`spark/`](spark/).

### Run the Hadoop (HDFS + MapReduce) big-data layer

The MapReduce job runs the same statistical detection as a Hadoop Streaming
mapper/reducer. `mapper | sort | reducer` is exactly what the cluster executes,
so you can run it locally with no Hadoop install:

```bash
python examples/generate_history.py --out data/trades.jsonl --format json --trades 100000
hadoop/run_local.sh data/trades.jsonl          # → symbol, detector, price, qty, score
```

On a real cluster it reads and writes the **HDFS data lake** (bring one up with
`docker compose --profile hadoop up`, NameNode UI at http://localhost:9870):

```bash
hdfs dfs -put data/trades.jsonl /tradewatch/trades/
hadoop/run_streaming.sh /tradewatch/trades /tradewatch/anomalies
```

Spark reads the same HDFS paths (`hdfs://namenode:9000/...`). See [`hadoop/`](hadoop/).

---

## Detectors

| Detector | Class of abuse / issue it targets | Core idea |
|---|---|---|
| **Z-score (price)** | Off-market prints, bad ticks | Price is > N σ from the rolling per-symbol mean |
| **Price spike** | Gaps, momentum ignition, fat fingers | Large instantaneous tick-to-tick % move |
| **Volume spike** | Block trades, fat-finger sizes | Size is a large multiple of the rolling **median** (robust) |
| **Velocity** | Quote stuffing, algo runaways | Trade count per symbol in a short window exceeds a cap |
| **Spoofing / imbalance** | Spoofing, layering | Heavily one-sided burst of prints in a few seconds |
| **Wash trade / self-trade** | Wash trading, painting the tape | Same beneficial owner on both sides (self-cross), matched price & size |
| **Isolation Forest** | Novel / multivariate anomalies | Online-trained model scores the *joint* feature vector |

Each detector is independently unit-tested and reads shared per-symbol window
state, so adding your own is a ~30-line file implementing one method.

---

## Detection quality

`tradewatch evaluate` streams labelled data (normal flow + injected abuse
episodes) through the engine and scores detections with **event-based** metrics —
the standard for surveillance systems: *did we catch the episode, and how noisy
were we on normal flow?*

```
============================================================
  TradeWatch — Detection Evaluation (event-based)
============================================================
  trades evaluated     : 15,000
  anomaly events       : 258
  events detected      : 248
  false-alarm episodes : 120
------------------------------------------------------------
  precision            : 0.674
  recall               : 0.961
  F1 score             : 0.792
  false-positive rate  : 0.997%  (of normal trades)
------------------------------------------------------------
  event recall by injected pattern:
    price_spike        65/65    (100.0%)
    spoofing           48/52    ( 92.3%)
    velocity_burst     39/41    ( 95.1%)
    volume_spike       40/41    ( 97.6%)
    wash_trade         56/59    ( 94.9%)
============================================================
```

The CI pipeline runs this evaluation on every push as a **quality gate**, so a
change that quietly wrecks detection fails the build.

---

## Performance

Per-event flagging latency, measured with `tradewatch bench` (single core, event
sourced from the simulator; your mileage varies with hardware):

| Configuration | p50 | p95 | p99 | Throughput |
|---|---|---|---|---|
| **Rule core** (statistical + behavioural detectors) | ~166 µs | ~221 µs | **~294 µs** | ~5,800 events/s |
| **Full engine** (+ online Isolation Forest scoring) | ~6.4 ms | ~7.9 ms | **~9.5 ms** | ~140 events/s |

Both configurations decide each trade **well inside a 200 ms budget** — the rule
core with ~600× headroom. The Isolation Forest adds the bulk of the latency
(per-event tree traversal + periodic refits); disable it in the ruleset when you
need maximum throughput, or run it in the Spark scale layer instead.

```bash
tradewatch bench          # prints the table above for your machine
```

---

## Integration

### REST — real-time decisioning
`POST /trades` ingests a trade and returns the anomaly decision synchronously —
the entry point for pre-trade checks or a risk gateway:

```bash
curl -s localhost:8000/trades -H 'content-type: application/json' -d '{
  "symbol": "AAPL", "price": 999.0, "quantity": 100, "side": "buy"
}' | jq
```

```json
{
  "trade_id": "trd_9f2c...",
  "anomalous": true,
  "alerts": [
    { "detector": "zscore", "severity": "critical", "score": 1.0,
      "reason": "price 999.0000 is +42.10σ from mean 194.9812 (σ=0.19)" }
  ]
}
```

### WebSocket — live alert stream
```
ws://localhost:8000/ws/alerts     # every alert as it fires
ws://localhost:8000/ws/trades     # the raw trade tape
```

### Embedded — no server
```python
from tradewatch import DetectionEngine, Trade

engine = DetectionEngine()
for alert in engine.process(Trade(symbol="BTC-USD", price=61990, quantity=0.5)):
    print(alert.severity, alert.reason)
```

### Kafka — production event backbone
```python
from tradewatch import DetectionEngine, Pipeline
from tradewatch.sources.kafka_source import KafkaTradeSource
from tradewatch.sinks import JsonlFileSink

pipeline = Pipeline(
    source=KafkaTradeSource(topic="trades", bootstrap_servers="broker:9092"),
    engine=DetectionEngine(),
    sinks=[JsonlFileSink("alerts.jsonl")],
)
# await pipeline.run()
```

See [`examples/`](examples/) for runnable scripts.

### HTTP endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Multi-page analytics console |
| `GET` | `/health` | Liveness / readiness |
| `GET` | `/stats` | Engine + pipeline metrics |
| `GET` | `/api/metrics?window=N` | Consolidated dashboard snapshot (KPIs, time-series, latency, per-symbol) |
| `GET` | `/config` | Effective detection ruleset |
| `GET` | `/alerts?limit=N` | Recent alerts (ring buffer) |
| `POST` | `/trades` | Ingest one trade → alerts |
| `WS` | `/ws/alerts` | Live alert stream |
| `WS` | `/ws/trades` | Live trade tape |

---

## Configuration

**Detection thresholds** live in [`config/detection_rules.yaml`](config/detection_rules.yaml) —
tune sensitivity without touching code:

```yaml
zscore:
  price_threshold: 3.0
price_spike:
  pct_threshold: 0.03
alert_cooldown_seconds: 8.0   # dedup window per (symbol, detector)
```

**Service settings** come from `TRADEWATCH_*` env vars (see [`.env.example`](.env.example)):

```bash
TRADEWATCH_PORT=8000
TRADEWATCH_SIMULATOR_ENABLED=false     # turn off the demo feed in production
TRADEWATCH_SIMULATOR_TRADES_PER_SECOND=25
```

---

## Trade schema

```jsonc
{
  "symbol": "AAPL",          // required
  "price": 194.32,           // required, > 0
  "quantity": 100,           // required, > 0
  "side": "buy",             // buy | sell
  "account_id": "acct_007",  // enables wash-trade detection
  "counterparty_id": "acct_007",
  "venue": "XNAS",
  "currency": "USD",
  "timestamp": "2026-01-01T15:00:00Z"  // event-time; defaults to now
}
```

---

## Development

```bash
make dev       # install with dev + kafka + spark extras
make test      # pytest (53 tests)
make lint      # ruff
make evaluate  # precision/recall report
make run       # serve the dashboard
```

Project layout:

```
src/tradewatch/
├── models.py        # Trade / Alert / Severity
├── config.py        # YAML ruleset + env settings (simulator + Kafka)
├── windows.py       # per-symbol event-time rolling stats
├── detectors/       # one file per detector (7 detectors)
├── engine.py        # orchestration + alert dedup
├── pipeline.py      # source → engine → sinks
├── sources/         # simulator, kafka, base
├── sinks/           # console, file (JSONL), websocket/broadcaster
├── api/             # FastAPI app + dashboard.html
├── evaluate.py      # event-based precision/recall harness
├── benchmark.py     # per-event latency benchmark
└── cli.py           # serve | simulate | evaluate | bench
spark/               # Apache Spark / PySpark scale layer
├── detection_sql.py # detectors as Spark SQL window functions
├── batch_backtest.py# historical Parquet backtest
└── streaming_job.py # Structured Streaming over Kafka
hadoop/              # Hadoop big-data layer (HDFS + MapReduce)
├── mapper.py        # Hadoop Streaming mapper (symbol-keyed)
├── reducer.py       # per-symbol z-score / volume MapReduce detection
└── run_local.sh     # run the job with Unix pipes (no cluster needed)
warehouse/           # SQL warehouse layer
├── hive/schema.hql  # Hive external tables over HDFS
└── snowflake/       # Snowflake DDL + Parquet→Snowflake loader
databricks/          # Databricks job notebook + job spec
airflow/dags/        # Airflow DAG orchestrating the daily ETL
examples/            # embedded, HTTP, Kafka producer, history generator
```

The engine itself also gained `guardrails.py`, `observability.py` and
`api/security.py` for the hardened, audited write path.

---

## Roadmap

- Order-book (L2) detectors for true spoofing/layering (cancel-to-fill ratios)
- Cross-instrument & cross-venue correlation (index-arb, ETF/NAV divergence)
- Alert case management + analyst feedback loop to auto-tune thresholds
- Prometheus metrics endpoint and Grafana dashboards
- Persist the Spark-trained per-symbol baselines back into the live engine
- Pluggable persistence (Postgres/ClickHouse) for the alert audit trail

---

## Disclaimer

TradeWatch is an engineering demonstration of streaming anomaly detection. It is
**not** regulatory-certified surveillance software and ships with no warranty.
Validate thoroughly against your own data and controls before any production or
compliance use.

## License

[MIT](LICENSE)
