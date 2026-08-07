# Run the CORE TradeWatch stack (Windows PowerShell) — lighter, ~4-6 GB RAM.
# Kafka + HDFS + Spark cluster + batch + API all genuinely running.
# Requires Docker Desktop (Engine running). For the full stack incl Hive +
# Airflow use scripts/run_stack.ps1 instead.

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "Starting the CORE stack (Kafka + HDFS + Spark + batch + API)..." -ForegroundColor Cyan
Write-Host "First run pulls images; allocate ~4-6 GB RAM to Docker." -ForegroundColor Yellow

docker compose -f docker-compose.core.yml up --build

Write-Host ""
Write-Host "Once healthy, open:" -ForegroundColor Green
Write-Host "  Dashboard ...... http://localhost:8000  (Platform page = all services green)"
Write-Host "  Spark master ... http://localhost:8080"
Write-Host "  HDFS NameNode .. http://localhost:9870"
Write-Host ""
Write-Host "Stop with:  docker compose -f docker-compose.core.yml down"
