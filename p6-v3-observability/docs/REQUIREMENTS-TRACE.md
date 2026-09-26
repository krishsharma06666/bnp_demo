# Requirements trace — every line of the brief → where it is built → how it is proven

| # | Requirement (brief) | Where | Proof (automated test / demo) |
|---|---|---|---|
| 1 | Kafka middleware between high-speed booking stream and relational DB | whole repo; `docker-compose.yml` | `tests/test_end_to_end.py` (all 7 service loops together); `docker compose up` |
| 2 | Handle traffic spikes without data loss | API returns 202 only after `acks=all`; Kafka buffers; writers commit offset after DB commit | `test_api.py::test_kafka_down_gives_503_and_key_is_freed`; chaos `traffic-spike`, `db-down`; Locust |
| 3 | Database consistency | one transaction per batch; FKs; CHECKs; conditional UPDATEs | `test_ride_workflow.py`, `test_database.py::test_constraints_protect_new_rows`, `check_counts` |
| 4 | Idempotency | API Idempotency-Key (Redis, shared by copies) → Kafka idempotent producer → `processed_events` → `UNIQUE(idempotency_key)`; payment `PAY-<id>` | `test_api.py` (same key, across copies), `test_pipeline_offline.py` (file twice → 0 new rows), `test_payment.py` |
| 5 | Concurrency control | `FOR UPDATE`, `SKIP LOCKED`, conditional updates, `version_no`, lock order booking → driver, savepoints | `test_ride_workflow.py` (100 simultaneous races, 2 dispatchers at once, driver can't win 2 rides) |
| 6 | Driver race: several drivers accept the same ride | `services/driver/responses.py` | `test_three_offered_drivers_accept_at_once_exactly_one_wins`; `race_test --rounds 20`; Grafana "Race losers" |
| 7 | Accept **and** reject stored in DB **and** Kafka | `driver_responses` table + `ride.events` (ACCEPTED/REJECTED/ASSIGNED/LOST/TIMEOUT) | `test_end_to_end.py::test_full_ride…` asserts both the rows and the events |
| 8 | Retry limit | `MAX_RETRIES=3` in `common/db.with_db_retry` (payments); bookings retry until the DB is back | `test_db_retry.py` (real outage: exactly 3 retries, then give up; recovers) |
| 9 | Dead-letter queue | `payment.requested.dlq` with error headers; `dlq-replay` | `test_payment.py`; chaos `payment-db-down` |
| 10 | Partial failure | bookings keep flowing while payments fail | chaos `payment-db-down`; `test_reconciler.py` |
| 11 | Eventual consistency | reconciler: COMPLETED without payment → request again; idempotent payment insert | `test_reconciler.py`; Grafana "Completed rides without payment" → 0 |
| 12 | Logs INFO / WARNING / RETRYING / ERROR | `common/logger.py` (JSON, custom RETRYING level) | `docker compose logs`; `test_db_retry.py` counts RETRYING |
| 13 | Grafana: inserted and rejected counts | dashboard row 1 + row 6 (exact SQL counts) + rejections by reason + messaging log | `test_monitoring_config.py` runs every SQL panel on the schema |
| 14 | 3 brokers, replication factor 3, `acks=all`, `min.insync.replicas=2` | `docker-compose.yml`, `create_topics.sh`, `common/kafka_client.py` | `scripts/broker_failover_test.py`; chaos `broker-down`; `test_platform_files.py` |
| 15 | Validation + rejected records with reasons | `services/validator/rules.py`; `rejected_records` | `test_validator_rules.py`; `bookings_bad.csv` (23 rows, all rejected) |
| 16 | Connection pooling | `common/db.pool` (psycopg_pool), Supabase transaction pooler | config in `DATABASE.md` |
| 17 | Real Uber-like workflow | book → nearest 3 drivers → accept/reject/timeout → rounds → trip → payment → live tracking | `test_end_to_end.py` (4 journeys), `/live` page |
| 18 | "If one fails, other containers handle the request" | 3 API copies behind HAProxy; 3 brokers; scalable consumers (Kafka rebalance) | measured: one API copy killed mid-traffic → 300/300 requests 202; chaos `api-crash`, `consumer-kill` |
| 19 | Self-healing | restart policies + heartbeat healthcheck + **Autopilot HEAL** | `test_autopilot.py`; chaos `consumer-hang` |
| 20 | DevOps / SecOps | CI (ruff, bandit, gitleaks, pip-audit, tests on PostgreSQL, compose + HAProxy check, Trivy, GHCR), Dependabot, CODEOWNERS; non-root read-only containers; API keys; RLS | `.github/workflows/ci.yml`; `test_platform_files.py`; `test_database.py::test_rls…` |
| 21 | FastAPI | `services/api/main.py` | `test_api.py` (25 tests) |
| 22 | Special features | surge pricing, velocity (fraud) guard, live tracker (SSE), smart dispatch rounds, driver simulator | `test_api.py::test_surge…`, `test_guard.py`, `test_simulator.py` |
| 23 | Unique feature | **Autopilot**: lag-driven autoscaling + hang detection + self-healing with an audit trail in Kafka | `test_autopilot.py`; chaos `traffic-spike` / `consumer-hang`; Grafana row 5 |
| 24 | Optional: cloud | Supabase (managed PostgreSQL) + images published to GHCR | `DATABASE.md`; CI `image` job |
| 25 | Optional: vector search | not built (documented as future work: similar-route search with pgvector on Supabase) | – |
