"""Offline evaluation harness.

Streams labelled trades from the simulator through the engine and scores the
detections against ground truth using **event-based** metrics — the standard
way to evaluate market-surveillance systems.

Rationale: an injected anomaly such as a velocity burst or a wash-trade pair is
one *episode* spanning several trades. What a surveillance desk cares about is
"did we catch the episode?" — not whether every constituent tick was
individually flagged. So:

* An **anomaly event** is a contiguous run of injected trades sharing a label.
  It counts as *detected* if the engine raised >=1 alert on any of its trades.
* A **false-alarm episode** is a maximal run of consecutive *normal* trades that
  were flagged.

    recall    = detected_events / total_events
    precision = detected_events / (detected_events + false_alarm_episodes)

This turns "the engine flags stuff" into measurable precision / recall / F1,
the evidence a risk or quant team wants before trusting a detector.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from .config import DetectionConfig
from .engine import DetectionEngine
from .sources import MarketSimulator


@dataclass
class EvalReport:
    total_trades: int
    total_events: int
    detected_events: int
    false_alarm_episodes: int
    normal_trades: int
    normal_trades_flagged: int
    per_label_recall: dict[str, tuple[int, int]] = field(default_factory=dict)

    @property
    def precision(self) -> float:
        denom = self.detected_events + self.false_alarm_episodes
        return self.detected_events / denom if denom else 0.0

    @property
    def recall(self) -> float:
        return self.detected_events / self.total_events if self.total_events else 0.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) else 0.0

    @property
    def false_positive_rate(self) -> float:
        return self.normal_trades_flagged / self.normal_trades if self.normal_trades else 0.0

    def as_dict(self) -> dict:
        return {
            "total_trades": self.total_trades,
            "total_events": self.total_events,
            "detected_events": self.detected_events,
            "false_alarm_episodes": self.false_alarm_episodes,
            "precision": round(self.precision, 4),
            "recall": round(self.recall, 4),
            "f1": round(self.f1, 4),
            "false_positive_rate": round(self.false_positive_rate, 4),
            "per_label_recall": {k: {"detected": h, "total": t} for k, (h, t) in self.per_label_recall.items()},
        }

    def print(self) -> None:
        print("=" * 60)
        print("  TradeWatch — Detection Evaluation (event-based)")
        print("=" * 60)
        print(f"  trades evaluated     : {self.total_trades:,}")
        print(f"  anomaly events       : {self.total_events:,}")
        print(f"  events detected      : {self.detected_events:,}")
        print(f"  false-alarm episodes : {self.false_alarm_episodes:,}")
        print("-" * 60)
        print(f"  precision            : {self.precision:.3f}")
        print(f"  recall               : {self.recall:.3f}")
        print(f"  F1 score             : {self.f1:.3f}")
        print(f"  false-positive rate  : {self.false_positive_rate:.3%}  (of normal trades)")
        print("-" * 60)
        print("  event recall by injected pattern:")
        for label, (hit, tot) in sorted(self.per_label_recall.items()):
            r = hit / tot if tot else 0.0
            print(f"    {label:<16} {hit:>4}/{tot:<4}  ({r:.1%})")
        print("=" * 60)


def evaluate(
    trades: int = 15000,
    anomaly_rate: float = 0.02,
    seed: int = 7,
    rules_path: str | None = None,
    symbols: list[str] | None = None,
) -> EvalReport:
    config = DetectionConfig.load(rules_path)
    engine = DetectionEngine(config)
    sim = MarketSimulator(
        symbols=symbols or ["AAPL", "MSFT", "BTC-USD", "ETH-USD", "TSLA"],
        anomaly_rate=anomaly_rate,
        seed=seed,
    )

    # Run the stream through the engine, recording (label, was_flagged) per trade.
    records: list[tuple[str | None, bool]] = []
    for trade, label in sim.labeled_batch(trades):
        alerts = engine.process(trade)
        records.append((label, bool(alerts)))

    return _score(records)


def _score(records: list[tuple[str | None, bool]]) -> EvalReport:
    total_events = detected_events = false_alarm_episodes = 0
    normal_trades = normal_flagged = 0
    per_label: dict[str, list[int]] = defaultdict(lambda: [0, 0])  # [detected, total]

    n = len(records)
    i = 0
    while i < n:
        label, _ = records[i]
        if label is not None:
            # Consume the contiguous anomaly episode sharing this label.
            j, any_flag = i, False
            while j < n and records[j][0] == label:
                any_flag = any_flag or records[j][1]
                j += 1
            total_events += 1
            per_label[label][1] += 1
            if any_flag:
                detected_events += 1
                per_label[label][0] += 1
            i = j
        else:
            # Walk the normal region, counting maximal runs of flagged trades.
            in_episode = False
            while i < n and records[i][0] is None:
                normal_trades += 1
                if records[i][1]:
                    normal_flagged += 1
                    if not in_episode:
                        false_alarm_episodes += 1
                        in_episode = True
                else:
                    in_episode = False
                i += 1

    return EvalReport(
        total_trades=n,
        total_events=total_events,
        detected_events=detected_events,
        false_alarm_episodes=false_alarm_episodes,
        normal_trades=normal_trades,
        normal_trades_flagged=normal_flagged,
        per_label_recall={k: (v[0], v[1]) for k, v in per_label.items()},
    )
