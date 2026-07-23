# Launch the full integrated TradeWatch platform (Windows PowerShell).
# Requires Docker Desktop with ~12 GB RAM allocated (Settings -> Resources).
#
#   .\scripts\run_stack.ps1

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "Starting the full TradeWatch platform (Kafka + Hadoop + Spark + Hive + Airflow + API)..." -ForegroundColor Cyan
Write-Host "First run pulls several images and needs ~12 GB RAM allocated to Docker." -ForegroundColor Yellow

docker compose -f docker-compose.full.yml up --build

Write-Host ""
Write-Host "Once healthy, open:" -ForegroundColor Green
Write-Host "  Dashboard ....... http://localhost:8000"
Write-Host "  Spark master .... http://localhost:8080"
Write-Host "  HDFS NameNode ... http://localhost:9870"
Write-Host "  Airflow ......... http://localhost:8081   (admin / admin)"
Write-Host "  HiveServer2 ..... jdbc:hive2://localhost:10000  (UI http://localhost:10002)"
Write-Host ""
Write-Host "Stop with:  docker compose -f docker-compose.full.yml down"
