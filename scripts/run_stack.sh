#!/usr/bin/env bash
# Launch the full integrated TradeWatch platform (macOS / Linux).
# Requires Docker Desktop / Engine with ~12 GB RAM allocated.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Starting the full TradeWatch platform (Kafka + Hadoop + Spark + Hive + Airflow + API)…"
echo "This pulls several images on first run and needs ~12 GB RAM."
docker compose -f docker-compose.full.yml up --build "$@"

cat <<'EOF'

Once healthy, open:
  Dashboard ....... http://localhost:8000
  Spark master .... http://localhost:8080
  HDFS NameNode ... http://localhost:9870
  Airflow ......... http://localhost:8081   (admin / admin)
  HiveServer2 ..... jdbc:hive2://localhost:10000  (UI http://localhost:10002)

Stop with:  docker compose -f docker-compose.full.yml down
EOF
