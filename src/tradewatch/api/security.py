"""API security: headers, request context/logging, rate limiting, API-key auth.

Posture:
* **Read endpoints** (dashboard, /health, /stats, /api/metrics, /config, /alerts)
  are open so the console works in a browser with no credentials.
* **The write path** (``POST /trades``) is the sensitive surface — it is guarded
  by an optional API key, a per-client rate limit, and input guardrails. In a
  hardened deployment set ``TRADEWATCH_API_KEY`` and put the service behind TLS.

All of this is standard, dependency-free ASGI/Starlette middleware.
"""

from __future__ import annotations

import time
from collections import deque

from fastapi import Header, HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from ..observability import AuditLogger, new_request_id, request_id_var


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Adds hardening headers to every response."""

    def __init__(self, app, csp: str | None = None) -> None:
        super().__init__(app)
        # The dashboard is self-contained: inline styles/scripts, same-origin WS,
        # no external hosts. This CSP allows exactly that and nothing else.
        self.csp = csp or (
            "default-src 'self'; script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
            "connect-src 'self' ws: wss:; base-uri 'none'; frame-ancestors 'none'"
        )

    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-XSS-Protection", "0")
        response.headers.setdefault("Content-Security-Policy", self.csp)
        response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
        return response


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Assigns a request id, times the request, logs access + errors."""

    def __init__(self, app, logger) -> None:
        super().__init__(app)
        self.logger = logger

    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or new_request_id()
        token = request_id_var.set(rid)
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            # Error logging: never leak internals to the client.
            self.logger.exception(
                "unhandled error on %s %s", request.method, request.url.path,
                extra={"extra_fields": {"path": request.url.path, "method": request.method}},
            )
            from starlette.responses import JSONResponse

            request_id_var.reset(token)
            return JSONResponse({"error": "internal server error", "request_id": rid}, status_code=500)
        elapsed_ms = (time.perf_counter() - start) * 1000
        response.headers["X-Request-ID"] = rid
        if request.url.path not in ("/health",):  # avoid probe spam
            self.logger.info(
                "%s %s -> %s (%.1fms)", request.method, request.url.path, response.status_code, elapsed_ms,
                extra={"extra_fields": {
                    "path": request.url.path, "method": request.method,
                    "status": response.status_code, "latency_ms": round(elapsed_ms, 1),
                }},
            )
        request_id_var.reset(token)
        return response


class RateLimiter:
    """Fixed-window-ish sliding rate limiter, per client key, in-memory."""

    def __init__(self, per_minute: int) -> None:
        self.per_minute = per_minute
        self._hits: dict[str, deque[float]] = {}

    def allow(self, key: str) -> bool:
        if self.per_minute <= 0:
            return True
        now = time.time()
        dq = self._hits.setdefault(key, deque())
        cutoff = now - 60.0
        while dq and dq[0] < cutoff:
            dq.popleft()
        if len(dq) >= self.per_minute:
            return False
        dq.append(now)
        return True


def make_api_key_dependency(expected_key: str | None, audit: AuditLogger):
    """Return a FastAPI dependency enforcing the API key when one is configured."""

    async def _dep(request: Request, x_api_key: str | None = Header(default=None)) -> None:
        if not expected_key:
            return  # auth disabled
        if x_api_key != expected_key:
            audit.record(
                "auth_failure", outcome="denied",
                client=request.client.host if request.client else "?", path=request.url.path,
            )
            raise HTTPException(status_code=401, detail="invalid or missing API key")

    return _dep
