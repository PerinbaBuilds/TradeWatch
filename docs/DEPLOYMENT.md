# Production deployment

TradeWatch ships as containers and is designed to run in production behind TLS
with authentication, structured logs and an audit trail. This guide covers a
single-host Docker deployment (the common case) and the notes you need to scale.

## 0. Prerequisites

- A Linux host (or WSL2) with **Docker Engine + Compose v2**.
- ~4–6 GB RAM for the **core** stack, ~12 GB for the **full** stack.
- A DNS record pointing at the host if you want a real HTTPS certificate.

## 1. Configure

```bash
cp .env.docker.example .env
# edit .env:
#   DOMAIN=surveillance.yourcompany.com     # or localhost for a local cert
#   TRADEWATCH_API_KEY=$(openssl rand -hex 24)
#   TRADEWATCH_CORS_ORIGINS=https://your-frontend
```

Never commit `.env`. In a real environment inject these from your secrets
manager (Docker/K8s secrets, Vault, SSM) rather than a file.

## 2. Launch (production overlay = TLS + auth + limits)

```bash
docker compose -f docker-compose.core.yml -f docker-compose.prod.yml up -d --build
#   full stack:  swap docker-compose.core.yml -> docker-compose.full.yml
```

The **prod overlay** (`docker-compose.prod.yml`):

- puts the API behind a **Caddy** reverse proxy with **automatic HTTPS** on 80/443;
- stops publishing the API port directly — only Caddy is public;
- **requires `TRADEWATCH_API_KEY`** (compose refuses to start without it),
  turns on **JSON logs**, **rate limiting**, and the **audit trail**;
- applies **CPU/memory limits** and **log rotation** to every long-running service.

Verify:

```bash
curl -k https://$DOMAIN/health
# ingest requires the key:
curl -k https://$DOMAIN/trades -H "X-API-Key: $TRADEWATCH_API_KEY" \
  -H 'content-type: application/json' -d '{"symbol":"AAPL","price":100,"quantity":5}'
```

## 3. Operate

| Concern | How |
|---|---|
| **Health** | `GET /health` (Docker healthcheck built in); the dashboard **Platform** page shows every service |
| **Logs** | JSON to stdout (`docker compose logs -f tradewatch`); ship to Loki/ELK/CloudWatch |
| **Audit trail** | append-only JSONL at `TRADEWATCH_AUDIT_LOG_PATH` (`/data/audit.jsonl`) — forward to your SIEM |
| **Metrics** | `GET /stats`, `GET /api/metrics` (scrape into your monitoring) |
| **Backups** | back up the `hdfs_namenode` / `hdfs_datanode` volumes and your Snowflake warehouse |
| **Updates** | `docker compose … pull && docker compose … up -d` (rolling per service) |

## 4. Build & publish images (CI/CD)

```bash
docker build -t <registry>/tradewatch:$(git rev-parse --short HEAD) .
docker build -f Dockerfile.batch -t <registry>/tradewatch-batch:$(git rev-parse --short HEAD) .
docker push <registry>/tradewatch:...
```

Pin image tags in your compose/manifests instead of `latest` for reproducible
rollouts. The GitHub Actions CI already builds both images on every push.

## 5. Scaling & HA notes (be honest about state)

- The **API + engine is stateful** — it holds per-symbol rolling windows and the
  online ML models in memory, so it runs as **one consumer**. To scale, run **one
  engine per Kafka partition set** and partition the `trades` topic **by symbol**
  (key = symbol) so each instance owns a disjoint symbol set. Do **not** put
  multiple engine replicas behind a load balancer over the same partitions —
  their windows would each see only part of the flow.
- **Kafka, HDFS, Spark, Hive** scale horizontally the normal way (more brokers /
  datanodes / workers). Use a managed Kafka (MSK/Confluent) and managed Spark
  (Databricks/EMR) in real production rather than the single-node demo containers.
- The **batch layer** (Spark/MapReduce) and **warehouse** (Hive/Snowflake) are
  independently scalable and stateless between runs.
- Front the API with your platform's autoscaler only for the **read** endpoints;
  keep the **ingest/engine** path single-owner per partition.

## 6. Security checklist

- [ ] `TRADEWATCH_API_KEY` set (or mTLS at the proxy / an API gateway in front).
- [ ] TLS everywhere (Caddy does this; use a real `DOMAIN`).
- [ ] `TRADEWATCH_CORS_ORIGINS` restricted to known front-ends.
- [ ] Audit log shipped to durable, tamper-evident storage.
- [ ] Secrets from a manager, never in the image or git.
- [ ] Network policy: only Caddy exposed publicly; internal services on the
      compose/overlay network only.

See [Security](SECURITY.md) for the full control list.
