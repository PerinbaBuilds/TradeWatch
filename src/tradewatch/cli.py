"""Command-line interface for TradeWatch.

Subcommands
-----------
serve      run the FastAPI app + dashboard (optionally with the simulator)
simulate   stream simulated trades through the engine to the console
evaluate   measure precision/recall against the simulator's ground truth

Examples
--------
    tradewatch serve
    tradewatch simulate --tps 50 --anomaly-rate 0.02
    tradewatch evaluate --trades 20000
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from .config import DetectionConfig, Settings
from .engine import DetectionEngine
from .pipeline import Pipeline
from .sinks import ConsoleSink, JsonlFileSink
from .sources import MarketSimulator


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tradewatch", description="Real-Time Trade Anomaly Detection Engine")
    sub = p.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Run the API server + dashboard")
    serve.add_argument("--host", default=None)
    serve.add_argument("--port", type=int, default=None)
    serve.add_argument("--no-simulator", action="store_true", help="Start without the built-in simulator")

    sim = sub.add_parser("simulate", help="Stream simulated trades to the console")
    sim.add_argument("--tps", type=float, default=25.0, help="Trades per second")
    sim.add_argument("--anomaly-rate", type=float, default=0.02)
    sim.add_argument("--symbols", default="AAPL,MSFT,BTC-USD,ETH-USD,TSLA")
    sim.add_argument("--seed", type=int, default=None)
    sim.add_argument("--max-trades", type=int, default=None, help="Stop after N trades")
    sim.add_argument("--out", default=None, help="Also append alerts as JSONL to this file")
    sim.add_argument("--rules", default=None, help="Path to detection_rules.yaml")

    ev = sub.add_parser("evaluate", help="Measure detection precision/recall on labelled data")
    ev.add_argument("--trades", type=int, default=15000)
    ev.add_argument("--anomaly-rate", type=float, default=0.02)
    ev.add_argument("--seed", type=int, default=7)
    ev.add_argument("--rules", default=None)

    bench = sub.add_parser("bench", help="Measure per-event processing latency and throughput")
    bench.add_argument("--trades", type=int, default=40000)
    return p


def _cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    settings = Settings()
    if args.host:
        settings.host = args.host
    if args.port:
        settings.port = args.port
    if args.no_simulator:
        settings.simulator_enabled = False

    from .api.app import create_app

    app = create_app(settings)
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="info")
    return 0


async def _run_simulate(args: argparse.Namespace) -> int:
    config = DetectionConfig.load(args.rules)
    engine = DetectionEngine(config)
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    source = MarketSimulator(
        symbols=symbols,
        trades_per_second=args.tps,
        anomaly_rate=args.anomaly_rate,
        seed=args.seed,
        max_trades=args.max_trades,
    )
    sinks = [ConsoleSink()]
    if args.out:
        sinks.append(JsonlFileSink(args.out))

    pipeline = Pipeline(source=source, engine=engine, sinks=sinks)
    print(f"streaming {args.tps:.0f} trades/s across {len(symbols)} symbols — Ctrl-C to stop\n", flush=True)
    try:
        await pipeline.run()
    except KeyboardInterrupt:
        pass
    finally:
        await pipeline.close()
        s = engine.stats()
        print(
            f"\nprocessed {s['trades_processed']} trades, raised {s['alerts_raised']} alerts "
            f"({dict(s['alerts_by_severity'])})",
            flush=True,
        )
    return 0


def _cmd_evaluate(args: argparse.Namespace) -> int:
    from .evaluate import evaluate

    report = evaluate(
        trades=args.trades,
        anomaly_rate=args.anomaly_rate,
        seed=args.seed,
        rules_path=args.rules,
    )
    report.print()
    return 0


def _cmd_bench(args: argparse.Namespace) -> int:
    from .benchmark import run_all

    run_all(trades=args.trades)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _build_parser().parse_args(argv)
    if args.command == "serve":
        return _cmd_serve(args)
    if args.command == "simulate":
        return asyncio.run(_run_simulate(args))
    if args.command == "evaluate":
        return _cmd_evaluate(args)
    if args.command == "bench":
        return _cmd_bench(args)
    return 1


if __name__ == "__main__":
    sys.exit(main())
