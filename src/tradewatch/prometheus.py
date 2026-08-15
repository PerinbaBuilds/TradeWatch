"""Prometheus text-exposition for the `/metrics` endpoint.

Rather than pull in a client library, we render the Prometheus text format
directly from the live `MetricsCollector` + engine stats. That keeps the core
dependency-free while making the service scrapeable by a standard Prometheus
server and graphable in Grafana (a ready-made dashboard ships in
`deploy/grafana/`).

The exposition follows the format spec: `# HELP` / `# TYPE` lines, then samples.
Counters end in `_total`; the latency summary is exported as quantiles.
"""

from __future__ import annotations

from .metrics import MetricsCollector


def _line(name: str, value: float, labels: dict[str, str] | None = None) -> str:
    if labels:
        inner = ",".join(f'{k}="{_escape(v)}"' for k, v in labels.items())
        return f"{name}{{{inner}}} {value}"
    return f"{name} {value}"


def _escape(v: str) -> str:
    return v.replace("\\", "\\\\").replace('"', '\\"')


def render(metrics: MetricsCollector, engine_stats: dict, pipeline_running: bool) -> str:
    kpis = metrics.kpis()
    lat = metrics.latency_stats()
    out: list[str] = []

    def block(name: str, help_text: str, mtype: str, samples: list[str]) -> None:
        out.append(f"# HELP {name} {help_text}")
        out.append(f"# TYPE {name} {mtype}")
        out.extend(samples)

    block("tradewatch_trades_total", "Total trades processed.", "counter",
          [_line("tradewatch_trades_total", kpis["total_trades"])])
    block("tradewatch_alerts_total", "Total alerts raised.", "counter",
          [_line("tradewatch_alerts_total", kpis["total_alerts"])])
    block("tradewatch_notional_total", "Total traded notional observed.", "counter",
          [_line("tradewatch_notional_total", kpis["total_notional"])])
    block("tradewatch_symbols_tracked", "Distinct symbols currently tracked.", "gauge",
          [_line("tradewatch_symbols_tracked", kpis["symbols_tracked"])])
    block("tradewatch_alert_rate_pct", "Alerts as a percentage of trades.", "gauge",
          [_line("tradewatch_alert_rate_pct", kpis["alert_rate_pct"])])
    block("tradewatch_trades_per_second", "Recent trade ingestion rate.", "gauge",
          [_line("tradewatch_trades_per_second", kpis["trades_per_sec"])])
    block("tradewatch_pipeline_running", "1 if the background pipeline is running.", "gauge",
          [_line("tradewatch_pipeline_running", 1 if pipeline_running else 0)])
    block("tradewatch_uptime_seconds", "Process uptime.", "gauge",
          [_line("tradewatch_uptime_seconds", kpis["uptime_seconds"])])

    block("tradewatch_alerts_by_detector_total", "Alerts broken down by detector.", "counter",
          [_line("tradewatch_alerts_by_detector_total", c, {"detector": d})
           for d, c in metrics.by_detector.items()] or
          [_line("tradewatch_alerts_by_detector_total", 0, {"detector": "none"})])
    block("tradewatch_alerts_by_severity_total", "Alerts broken down by severity.", "counter",
          [_line("tradewatch_alerts_by_severity_total", c, {"severity": s})
           for s, c in metrics.by_severity.items()] or
          [_line("tradewatch_alerts_by_severity_total", 0, {"severity": "none"})])

    block("tradewatch_latency_microseconds", "Per-event processing latency (microseconds).", "summary",
          [_line("tradewatch_latency_microseconds", lat["p50"], {"quantile": "0.5"}),
           _line("tradewatch_latency_microseconds", lat["p95"], {"quantile": "0.95"}),
           _line("tradewatch_latency_microseconds", lat["p99"], {"quantile": "0.99"}),
           _line("tradewatch_latency_microseconds_sum", (lat["mean"] * lat["count"])),
           _line("tradewatch_latency_microseconds_count", lat["count"])])

    return "\n".join(out) + "\n"
