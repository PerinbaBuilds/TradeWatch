# Security & observability

TradeWatch is built to run in front of untrusted producers, so the write path is
hardened and every security-relevant action is audited.

## Controls

| Control | What it does | Config |
|---|---|---|
| **Input guardrails** | Reject economically-absurd / malformed trades (non-finite values, bad symbols, price/qty/notional bounds, symbol-cardinality cap) before they reach the engine | `TRADEWATCH_GUARDRAILS_ENABLED` |
| **API-key auth** | `POST /trades` requires `X-API-Key` when a key is configured | `TRADEWATCH_API_KEY` |
| **Rate limiting** | Per-client sliding-window cap on ingestion | `TRADEWATCH_RATE_LIMIT_PER_MIN` |
| **Security headers** | CSP, `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy`, `Permissions-Policy` on every response | always on |
| **CORS allow-list** | Cross-origin access disabled unless explicitly allowed | `TRADEWATCH_CORS_ORIGINS` |
| **Strict validation** | Pydantic v2 models validate all input at the boundary | always on |
| **Non-root container** | Docker image runs as an unprivileged user | Dockerfile |

The **read** endpoints (dashboard, `/health`, `/stats`, `/api/metrics`,
`/config`, `/alerts`) are intentionally open so the console works in a browser;
harden them at the ingress/proxy if needed.

## Content-Security-Policy

The dashboard is fully self-contained (inline styles/scripts, same-origin
WebSocket, no external hosts), so the shipped CSP is tight:

```
default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline';
img-src 'self' data:; connect-src 'self' ws: wss:; base-uri 'none'; frame-ancestors 'none'
```

## Observability

* **Structured logging** — human-readable by default, JSON via
  `TRADEWATCH_LOG_JSON=true`, with a per-request correlation id (`X-Request-ID`).
* **Error logging** — unhandled exceptions are logged with the request id and a
  generic 500 is returned (no internals leak to the client).
* **Access logging** — method, path, status and latency per request.
* **Audit trail** — an append-only JSONL log (`tradewatch.audit`) records
  `trade_ingested`, `trade_rejected`, `rate_limited` and `auth_failure` events,
  ready to ship to a SIEM. Point it at a file with `TRADEWATCH_AUDIT_LOG_PATH`.

## Hardening checklist for production

- [ ] Set `TRADEWATCH_API_KEY` (or front with mTLS / an API gateway).
- [ ] Terminate TLS at a reverse proxy; never expose plain HTTP publicly.
- [ ] Set a realistic `TRADEWATCH_RATE_LIMIT_PER_MIN`.
- [ ] Ship `TRADEWATCH_AUDIT_LOG_PATH` to durable, tamper-evident storage.
- [ ] Restrict `TRADEWATCH_CORS_ORIGINS` to known front-ends.
- [ ] Manage all secrets (API key, Snowflake creds) via a secrets manager.
