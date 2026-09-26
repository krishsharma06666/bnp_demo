# Mac/Linux shortcuts. On Windows PowerShell type the command under each target (docs/RUNBOOK.md).
.PHONY: install up down ps logs seed seed-bad load-file check test lint smoke failover race locust dashboard

install:        ## laptop deps (tests, linters, locust)
	python -m pip install -r requirements-dev.txt
up:             ## whole platform: 3 brokers, DB + migrations, 3 API copies + LB, all services, monitoring
	docker compose up -d --build --wait
down:           ## stop and DELETE all data
	docker compose down -v --remove-orphans
ps:
	docker compose ps
logs:
	docker compose logs -f --tail 20 dispatcher driver-consumer trip-consumer payment-consumer autopilot
seed:           ## our 541 rows
	docker compose run --rm producer --file data/bookings.csv
seed-bad:       ## 23 bad rows
	docker compose run --rm producer --file data/bookings_bad.csv
load-file:      ## 100k rows through the producer
	python data/generate_load.py
	docker compose run --rm producer --file data/generated/load_100000.csv
check:
	python -m monitoring.check_counts --any
test:
	python -m pytest -q
lint:
	python -m ruff check . && python -m bandit -q -r common services db monitoring -ll -s B101,B104,B108,B110,B112,B311,B404,B603,B607,B608
smoke:
	python -m scripts.kafka_smoke_test
failover:
	python -m scripts.broker_failover_test --kill kafka2
race:
	docker compose stop simulator && python -m services.driver.race_test --rounds 20; docker compose start simulator
locust:
	locust -f loadtest/locustfile.py --host http://localhost:8000
dashboard:      ## rebuild Grafana JSON after editing build_dashboard.py
	python -m monitoring.grafana.build_dashboard
