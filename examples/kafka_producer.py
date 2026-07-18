"""Publish a simulated trade tape to Kafka — the producer half of the
Kafka → FastAPI pipeline.

Run a broker (see docker-compose `kafka` profile), then:

    pip install -e ".[kafka]"
    python examples/kafka_producer.py --bootstrap localhost:9092 --topic trades

Start TradeWatch consuming that topic in another shell:

    TRADEWATCH_SOURCE=kafka TRADEWATCH_KAFKA_TOPIC=trades tradewatch serve
"""

from __future__ import annotations

import argparse
import asyncio
import json

from tradewatch.sources import MarketSimulator


async def run(bootstrap: str, topic: str, tps: float, anomaly_rate: float) -> None:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    print(f"producing ~{tps:.0f} trades/s to '{topic}' on {bootstrap} — Ctrl-C to stop")
    sim = MarketSimulator(
        symbols=["AAPL", "MSFT", "BTC-USD", "ETH-USD", "TSLA"],
        trades_per_second=tps,
        anomaly_rate=anomaly_rate,
    )
    sent = 0
    try:
        async for trade in sim.stream():
            await producer.send_and_wait(topic, trade.model_dump(mode="json"))
            sent += 1
            if sent % 100 == 0:
                print(f"  sent {sent} trades", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        await producer.stop()


def main() -> None:
    p = argparse.ArgumentParser(description="Publish simulated trades to Kafka")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="trades")
    p.add_argument("--tps", type=float, default=25.0)
    p.add_argument("--anomaly-rate", type=float, default=0.02)
    args = p.parse_args()
    asyncio.run(run(args.bootstrap, args.topic, args.tps, args.anomaly_rate))


if __name__ == "__main__":
    main()
