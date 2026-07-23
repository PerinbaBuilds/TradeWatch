"""Centralised platform health probing.

The dashboard's Platform page needs to know whether every component of the stack
is up — Kafka, HDFS, the Spark cluster, Hive, Airflow — and whether the batch
layer has actually executed. This module probes each service (fast, concurrent
TCP/HTTP checks, no heavy clients) and reports a status board.

Targets default to the service names in ``docker-compose.full.yml`` and can be
overridden with ``TRADEWATCH_PLATFORM_SERVICES`` (``Name|host|port|kind`` items,
comma-separated). When you run just ``tradewatch serve`` locally, the cluster
hosts won't resolve and show ``down`` — which is the honest truth (they aren't
running).
"""

from __future__ import annotations

import asyncio
import json
import socket
import time
from pathlib import Path
from typing import Any

# name, host, port, kind, docs-url-hint
_DEFAULT_SERVICES = [
    ("Kafka broker", "kafka", 9092, "streaming", None),
    ("HDFS NameNode", "namenode", 9870, "data-lake", "http://localhost:9870"),
    ("Spark master", "spark-master", 8080, "compute", "http://localhost:8080"),
    ("HiveServer2", "hive", 10000, "warehouse", "http://localhost:10002"),
    ("Airflow", "airflow-webserver", 8080, "orchestration", "http://localhost:8081"),
]


def _parse_services(raw: str | None) -> list[tuple[str, str, int, str, str | None]]:
    if not raw:
        return list(_DEFAULT_SERVICES)
    out = []
    for item in raw.split(","):
        parts = [p.strip() for p in item.split("|")]
        if len(parts) >= 3:
            name, host, port = parts[0], parts[1], int(parts[2])
            kind = parts[3] if len(parts) > 3 else "service"
            out.append((name, host, port, kind, None))
    return out or list(_DEFAULT_SERVICES)


def _probe_tcp(host: str, port: int, timeout: float = 0.6) -> tuple[str, float]:
    start = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return "up", (time.perf_counter() - start) * 1000
    except OSError:
        return "down", (time.perf_counter() - start) * 1000


class PlatformMonitor:
    def __init__(self, services_env: str | None = None, data_dir: str = "/data") -> None:
        self.services = _parse_services(services_env)
        self.data_dir = Path(data_dir)

    async def snapshot(self) -> dict[str, Any]:
        results = await asyncio.gather(*[self._check(s) for s in self.services])
        services = list(results)
        up = sum(1 for s in services if s["status"] == "up")
        return {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "summary": {"total": len(services), "up": up, "down": len(services) - up},
            "services": services,
            "batch": self._batch_status(),
        }

    async def _check(self, svc: tuple[str, str, int, str, str | None]) -> dict[str, Any]:
        name, host, port, kind, url = svc
        status, latency = await asyncio.to_thread(_probe_tcp, host, port)
        return {
            "name": name,
            "kind": kind,
            "target": f"{host}:{port}",
            "status": status,
            "latency_ms": round(latency, 1),
            "url": url,
        }

    def _batch_status(self) -> dict[str, Any]:
        """Read the batch runner's heartbeat file (written each cycle)."""
        hb = self.data_dir / "batch_status.json"
        try:
            data = json.loads(hb.read_text())
            age = time.time() - float(data.get("epoch", 0))
            data["status"] = "up" if age < 900 else "stale"
            data["age_seconds"] = int(age)
            return data
        except Exception:
            return {"status": "unknown", "detail": "no batch heartbeat yet"}
