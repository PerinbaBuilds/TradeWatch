"""Structured logging, audit trail, and error logging.

Two logging channels:

* the standard application logger (``tradewatch.*``) — human-readable or JSON,
  used for operational logs and errors; and
* a dedicated **audit logger** (``tradewatch.audit``) that appends one JSON line
  per security-relevant action (trade ingested, guardrail rejection, auth
  failure, config read). The audit trail is append-only and separate so it can
  be shipped to a SIEM and retained independently of app logs.

Everything is stdlib ``logging`` so it composes with any handler/shipper.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from contextvars import ContextVar
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

# Correlates all log lines emitted while handling one request.
request_id_var: ContextVar[str] = ContextVar("request_id", default="-")


class JsonFormatter(logging.Formatter):
    """Compact JSON log lines with request-id correlation."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "request_id": request_id_var.get(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for key, val in getattr(record, "extra_fields", {}).items():
            payload[key] = val
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", json_logs: bool = False) -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler()
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.addHandler(handler)


class AuditLogger:
    """Append-only JSONL audit trail of security-relevant actions."""

    def __init__(self, path: str | None = None) -> None:
        self._logger = logging.getLogger("tradewatch.audit")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False
        if not self._logger.handlers:
            handler: logging.Handler
            try:
                if path:
                    Path(path).parent.mkdir(parents=True, exist_ok=True)
                    handler = RotatingFileHandler(path, maxBytes=5_000_000, backupCount=3)
                else:
                    handler = logging.StreamHandler()
            except OSError:
                # e.g. audit path not writable (read-only volume) — fall back to
                # stderr so auditing degrades gracefully instead of crashing boot.
                logging.getLogger("tradewatch").warning("audit log path %s not writable; using stderr", path)
                handler = logging.StreamHandler()
            handler.setFormatter(JsonFormatter())
            self._logger.addHandler(handler)

    def record(self, action: str, *, outcome: str = "ok", **fields: Any) -> None:
        rec = logging.LogRecord("tradewatch.audit", logging.INFO, "", 0, action, None, None)
        rec.extra_fields = {"action": action, "outcome": outcome, "audit": True, **fields}
        self._logger.handle(rec)


def new_request_id() -> str:
    return uuid.uuid4().hex[:16]
