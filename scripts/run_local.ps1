# Run TradeWatch locally WITHOUT Docker (Windows PowerShell).
# Starts the batch runner + the dashboard together, sharing a data dir so the
# Platform page shows the API up and the batch layer executing.
#
#   .\scripts\run_local.ps1
#
# The clustered services (Kafka/HDFS/Spark-cluster/Hive/Airflow) still need the
# Docker stack — they'll show "down" on the Platform page, which is honest.

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$env:TRADEWATCH_DATA_DIR = (Join-Path (Get-Location) "data")
New-Item -ItemType Directory -Force -Path $env:TRADEWATCH_DATA_DIR | Out-Null

Write-Host "Starting the batch runner (Spark backtest + Hadoop MapReduce) in the background..." -ForegroundColor Cyan
Start-Process -FilePath "python" -ArgumentList "scripts/batch_runner.py","--interval","180" -WindowStyle Minimized

Write-Host "Starting the dashboard on http://localhost:8000 ..." -ForegroundColor Green
Write-Host "Open the 'Platform' page to watch health + batch executions." -ForegroundColor Green
tradewatch serve
