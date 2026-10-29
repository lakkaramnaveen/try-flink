# Shortcuts for the common workflows.  Run `make` to list them.
.DEFAULT_GOAL := help
PYTHON ?= python3
BENCH_DAYS ?= 200

.PHONY: help install panel up down restart scale sql results logs data test e2e bench-data bench bench-report

help:  ## List the available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  make %-13s %s\n", $$1, $$2}'

install:  ## Install the Python dependencies
	$(PYTHON) -m pip install -r requirements.txt

panel:  ## Start the control panel at http://localhost:5050
	$(PYTHON) app.py

up:  ## Start Kafka and Flink in Docker
	docker compose up -d --build

down:  ## Stop and remove the Docker containers (Kafka data is discarded)
	docker compose down --remove-orphans

restart: down up  ## Clean restart of the Docker stack

TM ?= 2
scale:  ## Run TM TaskManagers (default 2), e.g. `make scale TM=3`
	docker compose up -d --scale taskmanager=$(TM)

sql:  ## Submit the clicks-per-minute job (it writes to the clicks-per-minute topic)
	docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/clicks-per-minute.sql

results:  ## Follow the job's results: clicks per video per minute
	docker compose exec kafka rpk topic consume clicks-per-minute -f '%v\n'

logs:  ## Follow the Flink logs
	docker compose logs -f jobmanager taskmanager

data:  ## Regenerate youtube-clicks.csv
	$(PYTHON) generate-fake-clicks-data.py

test:  ## Run the Python and JavaScript tests (no Docker needed)
	$(PYTHON) -m unittest test_app test_flink_monitor test_results test_traffic_metrics test_simulation_clock test_simulator_producer
	node test_traffic_chart.cjs

e2e:  ## Check on a fresh stack that Flink counts every click exactly once, across a rescale and a crash (~5 min)
	RUN_E2E=1 $(PYTHON) -m unittest -v test_e2e

bench-data:  ## Load BENCH_DAYS days of clicks (100k each) into the bench-clicks topic
	docker compose exec kafka rpk topic create bench-clicks -p 4 -r 1 || true
	$(PYTHON) bench/bulk-load.py bench-clicks $(BENCH_DAYS)

bench:  ## Compare parallelism 1 and 4 on the bench-clicks backlog (results are appended to bench/results.csv)
	bench/bench.sh p1 1 legacy
	bench/bench.sh p4 4 legacy

bench-report:  ## Summarize every benchmark run so far, fastest setup first
	$(PYTHON) bench/report.py
