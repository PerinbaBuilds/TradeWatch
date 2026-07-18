# Architecture

TradeWatch is built around a strict, three-layer contract so any layer can be
swapped without touching the others:

```
TradeSource  ──▶  DetectionEngine  ──▶  AlertSink
 (produce)         (decide)             (deliver)
```

At the system level this forms a **Lambda-style architecture**:

- **Speed layer** — Apache Kafka → the FastAPI-hosted `DetectionEngine`, deciding
  each trade in sub-10ms and streaming alerts to the dashboard/sinks. This is the
  code under `src/tradewatch/`.
- **Batch / scale layer** — distributed jobs that apply the same statistical
  detectors over large, at-rest data:
  - **Apache Spark / PySpark** (`spark/`) — detectors as Spark SQL window
    functions, for backtesting thresholds over historical Parquet and for
    distributed Structured Streaming over the same Kafka topic.
  - **Apache Hadoop** (`hadoop/`) — **HDFS** as the durable data lake and a
    **MapReduce** (Streaming) job for massive batch anomaly scans. The
    mapper emits symbol-keyed records; the reducer does per-symbol z-score /
    volume detection. Spark and MapReduce read/write the same `hdfs://` paths.

  Baselines learned here can seed the live engine.

All layers read the identical trade schema (`spark/detection_sql.py` and the
Hadoop mapper mirror `tradewatch.models.Trade`) and apply the same rules, so
offline and online agree.

## Data flow

1. A **`TradeSource`** yields `Trade` objects (`sources/`). Implementations:
   the built-in `MarketSimulator`, a `KafkaTradeSource`, or anything you write.
   The API's `POST /trades` is a source too — it hands trades straight to the
   engine.
2. The **`Pipeline`** (`pipeline.py`) is the async driver: it pulls from the
   source, calls the engine, and fans alerts out to sinks. It is
   cancellation-safe and tolerant of individual sink failures.
3. The **`DetectionEngine`** (`engine.py`) is synchronous and transport-agnostic.
   For each trade it:
   - updates that symbol's rolling **event-time window** (`windows.py`),
   - runs every enabled **detector** (`detectors/`),
   - applies **dedup/cooldown** so one episode yields one alert,
   - returns alerts sorted most-severe-first.
4. An **`AlertSink`** delivers alerts (`sinks/`): console, JSONL audit file, or
   the in-process `Broadcaster` that powers the WebSocket dashboard. Add Slack,
   a SIEM, Kafka, or a database by implementing one method.

## Why event-time windows

Detectors that reason about *rate* (velocity) or *recency* (spoofing, wash
trades) must use the trade's own timestamp, not the moment it was processed.
Keying off event-time makes detection correct under replay, back-pressure and
out-of-order-ish arrival, and it makes the offline evaluation faithful to live
behaviour. Each `SymbolWindow` evicts by both count and age so its statistics
track a bounded, recent slice of the market.

## Why a synchronous engine core

The hot path — window update + a handful of O(window) detector checks — is pure
CPU and must be predictable and cheap. Keeping it synchronous avoids per-trade
task/coroutine overhead and makes the engine trivially embeddable and unit-
testable. Concurrency lives at the edges (the async pipeline and I/O sinks),
where it belongs.

## Alert deduplication

A single manipulation episode (e.g. a 50-trade velocity burst) would otherwise
produce dozens of near-identical alerts, and the burst's *aftermath* keeps the
short window "hot" for a few seconds. The engine records the event-time of the
last emitted alert per `(symbol, detector)` and suppresses repeats within
`alert_cooldown_seconds`. This mirrors alert-grouping in incident tools and is
the single biggest lever on the precision/noise trade-off — see the sweep in the
[Detection quality](../README.md#detection-quality) section.

## Extending it

**Add a detector** — create `detectors/my_detector.py`:

```python
from .base import Detector
from ..models import Alert, Severity

class MyDetector(Detector):
    name = "my_detector"
    def inspect(self, trade, window):
        if suspicious(trade, window):
            return Alert.build(trade=trade, detector=self.name,
                               severity=Severity.HIGH, score=0.8, reason="...")
        return None
```

Register it in `engine.py::_build_detectors` (gated on a config flag) and add a
`MyDetectorConfig` to `config.py`. Unit-test it in isolation with a hand-built
`SymbolWindow`.

**Add a source or sink** — implement `TradeSource.stream()` or
`AlertSink.emit()` and pass it to the `Pipeline`.
```
