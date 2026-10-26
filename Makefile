.DEFAULT_GOAL := help
.PHONY: help setup db migrate seed api worker burst outage consume test check reset

help: ## List the commands
	@grep -E '^[a-z]+:.*## ' Makefile | awk 'BEGIN {FS = ":.*## "}; {printf "  make %-8s %s\n", $$1, $$2}'

setup: db migrate seed ## Start Postgres, create the schema, seed products

db: ## Start Postgres in Docker
	docker compose up -d --wait db

migrate: ## Create or update the schema
	uv run python -m orderflow.migrate

seed: ## Add the demo products and opening stock
	uv run python scripts/seed.py

api: ## Run the HTTP API on :8000
	uv run uvicorn orderflow.api:create_app --factory --port 8000

worker: ## Run the worker (stock and order-status consumers)
	uv run python -m orderflow.worker

burst: seed ## Unhappy path 1: seed, then a burst of orders with duplicate submissions
	uv run python scripts/burst.py

outage: ## Unhappy path 2: worker outage and catch-up (stop `make worker` first)
	uv run python scripts/outage.py

consume: ## Option A: example consumer of the order feed
	uv run python scripts/consume_feed.py

test: ## Run the tests (needs `make db`)
	uv run pytest

check: ## Lint, format check and type check
	uv run ruff check . && uv run ruff format --check . && uv run mypy

reset: ## Delete all data and set up again
	docker compose down -v
	$(MAKE) setup
