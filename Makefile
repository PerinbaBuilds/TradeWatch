.PHONY: help install dev test lint fmt run simulate evaluate bench kafka hadoop docker clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Install runtime dependencies
	pip install -e .

dev: ## Install with dev + kafka + spark extras
	pip install -e ".[dev,kafka,spark]"

bench: ## Measure per-event latency and throughput
	tradewatch bench

kafka: ## Run the full Kafka -> FastAPI pipeline (broker + producer + consumer)
	docker compose --profile kafka up --build

hadoop: ## Run the Hadoop MapReduce job locally (generates data if needed)
	@test -f data/trades.jsonl || python examples/generate_history.py --out data/trades.jsonl --format json --trades 100000
	bash hadoop/run_local.sh data/trades.jsonl | head -20

test: ## Run the test suite
	pytest

lint: ## Lint with ruff
	ruff check src tests

fmt: ## Auto-format / autofix with ruff
	ruff check --fix src tests

run: ## Start the API server + dashboard (http://localhost:8000)
	tradewatch serve

simulate: ## Stream simulated trades to the console
	tradewatch simulate --tps 30 --anomaly-rate 0.02

evaluate: ## Measure detection precision/recall on labelled data
	tradewatch evaluate --trades 15000

docker: ## Build and run the container
	docker compose up --build

clean: ## Remove caches and build artifacts
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
