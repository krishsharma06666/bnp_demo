# Team guide v3 — who owns what, how to test it, how to push it

v3 turns the pipeline into a **real Uber-like workflow** (book → nearest drivers → accept/reject → trip → payment),
stores every decision in the database **and** Kafka, runs **3 API copies behind a load balancer**, adds the
**Autopilot** (self-scaling + self-healing), DevSecOps (CI, scans, hardened containers) and a live tracker.

Each member ships v3 as **one branch + one pull request** from their zip `pN-v3-….zip`.

## 1. Git workflow (everyone, PowerShell)

v3 changes shared code, so we integrate on a branch `release/v3` first and merge into `main` once it is green.

**Person 1, once, before anyone else:**
```powershell
git switch main; git pull
git switch -c release/v3
git push -u origin release/v3
```
GitHub → Settings → Branches → add rule for `main`: *Require a pull request* + *Require status checks: ci*.

**Every member (in the merge order below):**
```powershell
git switch release/v3; git pull
git switch -c feat/<your-branch>
Expand-Archive -Force ..\pN-v3-<name>.zip .          # unzip INTO the repo root, overwrite
# the zip contains DELETE_THESE_FILES.txt -> remove those files:
Get-Content DELETE_THESE_FILES.txt | ForEach-Object { if (Test-Path $_) { git rm -q $_ } }
Remove-Item DELETE_THESE_FILES.txt
python -m pip install -r requirements-dev.txt
python -m pytest -q <your tests>                    # see your section
git add -A
git status                                          # only YOUR paths; never .env / .venv
git commit -m "feat(<area>): v3 <what>"
git push -u origin feat/<your-branch>
```
GitHub → **Pull request: `feat/<your-branch>` → `release/v3`** → 1 review (CODEOWNERS asks the right person) → merge.
Tests that need a later member's code may fail until that member merges — that is expected on `release/v3`.

**After all 6 merged (Person 1):**
```powershell
git switch release/v3; git pull
python -m pytest -q                                  # 99 passed
docker compose up -d --build --wait                  # whole platform
```
Then PR **`release/v3` → `main`**. CI must be green (lint, secret scan, tests on PostgreSQL, compose check,
image build + Trivy scan). Merge → CI publishes the image to GitHub Container Registry.

## 2. Merge order

| # | Branch | Owner | Why this order |
|---|---|---|---|
| 1 | `feat/platform-v3` | Person 1 | shared code (`common/`), one image, compose base, HAProxy, Redis, Autopilot, CI |
| 2 | `feat/db-v3` | Person 2 | schema + migrations everyone writes to |
| 3 | `feat/api-v3` | Person 3 | the front door (`/v1`) |
| 4 | `feat/dispatch-v3` | Person 4 | validation + nearest-driver dispatch |
| 5 | `feat/rides-v3` | Person 5 | writers: bookings, driver answers, trips, payments |
| 6 | `feat/observability-v3` | Person 6 | simulator, dashboards, alerts, end-to-end test, reports |

Rule: change only your own paths. Anything in `common/`, root files or `infra/` → ask Person 1.

---

## Person 1 — Platform, Kafka, high availability, Autopilot, CI/CD  (`feat/platform-v3`)

| File | What |
|---|---|
| `docker-compose.yml` | `name: ride-middleware`; 3 brokers (unchanged v2 settings, ports bound to localhost); **redis** (password), **lb** (HAProxy), **autopilot** |
| `Dockerfile`, `.dockerignore` | ONE image for all Python services, non-root uid 10001, heartbeat HEALTHCHECK |
| `infra/compose/app.yml` | hardened base every service `extends`: env from `.env`, read-only FS, `no-new-privileges`, `cap_drop: ALL`, limits, restart, healthcheck, log rotation |
| `infra/haproxy/haproxy.cfg` | load balancer: finds API copies by DNS, health checks every 2 s, retries on another copy, SSE-friendly, stats + metrics on :8404 |
| `infra/kafka/create_topics.sh` | 12 topics (adds `ride.requested`, `driver.offers`, `trip.updates`, `ride.events`, `ops.events`) |
| `common/topics.py`, `config.py`, `events.py`, `metrics.py`, `db.py`, `kafka_client.py` | new topics, API keys, Redis, dispatch settings, `ride_event()`, v3 metrics, heartbeat while waiting for the DB |
| `common/geo.py` | 18 Bengaluru areas with coordinates, distance, base fare |
| `common/health.py` | liveness heartbeat → "hung" becomes *unhealthy* |
| `services/autopilot/{policy,docker_ops,autopilot}.py` | **the unique feature**: sense (Prometheus lag, Docker health) → heal → decide → clone/remove containers → `ops.events` + metrics |
| `.github/workflows/ci.yml`, `dependabot.yml`, `CODEOWNERS`, `pull_request_template.md`, `ruff.toml` | CI/CD + SecOps |
| `requirements.txt`, `requirements-dev.txt`, `.env.example`, `Makefile` | one dependency list, placeholders only |
| `tests/test_autopilot.py`, `test_platform_files.py`, `test_db_retry.py`, `conftest.py` | autopilot policy/heal/scale/clone; topic script; hardening; DB retry |
| `docs/ARCHITECTURE.md`, `docs/TEAM-GUIDE.md`, `README.md` | |
| **Deletes** | every `services/*/Dockerfile` and `services/*/requirements.txt` (replaced by the one image) |

**Test:** `python -m pytest -q tests/test_autopilot.py tests/test_platform_files.py` → all pass.
`python -m ruff check .` → clean. `docker compose config --quiet` → no output.
**Demo:** `python -m loadtest.chaos broker-down`, `api-crash`, `consumer-hang` (Autopilot HEAL), `traffic-spike` (SCALE_UP/DOWN); show http://localhost:8404/stats and Grafana row 5; show the GitHub Actions run.
**Report:** Kafka settings and why each prevents loss; HA table; Autopilot design; DevSecOps pipeline.

## Person 2 — Database (`feat/db-v3`)

| File | What |
|---|---|
| `db/migrations/001_schema.sql` | your Supabase schema, verbatim, `IF NOT EXISTS`, no DROPs |
| `db/migrations/002_middleware.sql` | `locations`, `driver_responses` (accept + reject stored), `processed_events`, `rejected_records`; new columns (`version_no`, `surge_multiplier`, `dispatch_round`, `current_location` …); CHECKs `NOT VALID` |
| `db/migrations/003_seed.sql` | 18 areas, 1,200 riders, 500 drivers, 500 vehicles |
| `db/migrations/004_offer_guard.sql` | outcome `NOT_OFFERED` |
| `db/migrations/005_row_level_security.sql` | RLS on: Supabase REST API sees nothing |
| `db/migrate.py` | applies each file once, transaction-level advisory lock (poolers + parallel runners safe) |
| `db/compose.yml`, `db/init-test-db.sql`, `db/__init__.py` | local postgres + `db-migrate` job |
| `tests/test_database.py`, `docs/DATABASE.md` | the teammate template filled in, Supabase setup |
| **Deletes** | `db/init/*`, `db/checks.sql`, `db/payment/schema.sql`, `db/Dockerfile` (two-database v2 layout) |

**Test:** `python -m pytest -q tests/test_database.py` → 3 passed. On Supabase: `python -m db.migrate` → 5 files applied; Table Editor shows 10 tables.
**Demo:** `driver_responses` for one ride (who was offered, who accepted/rejected, who won); constraints refusing bad rows.
**Report:** ER diagram, keys/indexes and why, write-heavy choices, migrations strategy, Supabase pooling + RLS.

## Person 3 — Booking API (`feat/api-v3`)

| File | What |
|---|---|
| `services/api/main.py` | `/v1` rider + driver + public routes, API-key auth, surge-priced fare, 202 only after Kafka `acks=all`, SSE live stream, security headers, `X-Replica` |
| `services/api/idempotency.py` | Idempotency-Key in Redis shared by all 3 copies (200 same answer / 409 in progress), local fallback |
| `services/api/limits.py` | rate limit; **snowflake booking ids** with a Redis-leased node id per copy (unique across copies, exact in JavaScript) |
| `services/api/surge.py` | demand (Redis, 5 min) ÷ supply (dispatcher) → ×1.0 … ×2.5 |
| `services/api/reader.py`, `live.html` | ride status for riders; the live tracker page |
| `services/producer/compose.yml` | `api` with `deploy.replicas: 3` (no fixed port — the LB is the door), producer |
| `tests/test_api.py` | 25 tests: auth, 413/422/429, idempotency across copies, 503 + retry, surge, SSE, driver routes, ids |

**Test:** `python -m pytest -q tests/test_api.py` → 25 passed.
**Demo:** http://localhost:8000/live — book, watch the timeline; same Idempotency-Key twice; Swagger `/docs`; `X-Replica` changing between calls.
**Report:** ingestion design, why 202 after acks=all, idempotency across copies, id design, surge formula, API security.

## Person 4 — Middleware logic: validation + smart dispatch (`feat/dispatch-v3`)

| File | What |
|---|---|
| `services/validator/rules.py`, `validator.py` | areas from `common/geo`, surge kept on the row; **velocity guard** for live API traffic |
| `services/validator/guard.py` | > 5 bookings/min per user → `USER_VELOCITY` rejection (Redis, shared) |
| `services/validator/compose.yml` | validator + dispatcher |
| `services/dispatcher/dispatch.py` | lock booking → rank AVAILABLE drivers by distance → claim 3 nearest (`SKIP LOCKED`) → OFFERED rows; sweep: 15 s TIMEOUT → next round → `NO_DRIVER` after 3 |
| `services/dispatcher/dispatcher.py` | consumes `ride.requested`, publishes `driver.offers` + `ride.events`, publishes driver supply per area for surge |
| `services/driver/race_test.py` | live race demo through the real system (`/v1`, API keys, offered drivers) |
| `tests/test_guard.py` | velocity guard + only for API traffic |

**Test:** `python -m pytest -q tests/test_guard.py tests/test_validator_rules.py` → pass (dispatch is tested in `test_ride_workflow.py` after Person 5 merges).
**Demo:** a booking offered to the 3 nearest drivers; all reject → round 2 offers 3 different drivers; nobody answers → TIMEOUT ×3 rounds → NO_DRIVER; `race_test --rounds 20` → 20/20 one winner.
**Report:** validation rules, dispatch algorithm (distance, rounds, timeouts), concurrency (SKIP LOCKED, lock order), velocity guard.

## Person 5 — Writers: bookings, driver answers, trips, payments (`feat/rides-v3`)

| File | What |
|---|---|
| `services/consumer/consumer.py`, `db_writer.py`, `rejected_writer.py`, `compose.yml` | exactly-once booking insert; REQUESTED → `ride.requested`; all five writers in one compose file |
| `services/driver/responses.py` | ACCEPT/REJECT stored; first valid ACCEPT wins (booking + driver conditional updates in one savepoint); LOST / DECLINED / NOT_OFFERED; replay-safe |
| `services/driver/driver_consumer.py` | `driver.response` → DB → `ride.events` ACCEPTED / REJECTED / ASSIGNED / LOST |
| `services/trip/trip.py`, `trip_consumer.py` | START / COMPLETE / CANCEL state machine, driver released at destination, `payment.requested` |
| `services/payment/writer.py`, `payment_consumer.py` | payment on the team `payments` table (id = booking_id, `PAY-<id>`, txn ref); 3 retries → DLQ; `ride.events PAID` |
| `tests/test_ride_workflow.py`, `test_payment.py`, `test_db_writer.py` | 11 real-concurrency workflow tests (100 simultaneous races, 2 dispatchers at once …), payments, writer |
| **Deletes** | `services/driver/assign.py`, `tests/test_driver_race.py` (v2 race, replaced by `responses.py` + workflow tests) |

**Test:** `python -m pytest -q tests/test_ride_workflow.py tests/test_payment.py tests/test_db_writer.py` → pass.
**Demo:** two drivers ACCEPT the same ride → one ASSIGNED, one LOST (both rows in `driver_responses`, both events in `ride.events`); trip start → complete → payment; `payment-db-down` chaos → RETRYING ×3 → DLQ → replay.
**Report:** exactly-once writing, the race and how SQL decides it, trip state machine, retry limit + DLQ, partial failure.

## Person 6 — Observability, simulation, QA (`feat/observability-v3`)

| File | What |
|---|---|
| `services/simulator/simulator.py`, `compose.yml` | riders book N/s; 500 drivers accept 60 % / reject 25 % / ignore 15 %, start and complete their rides — all through the real API |
| `services/reconciler/reconciler.py` | COMPLETED without payment → request again (eventual consistency) |
| `monitoring/grafana/build_dashboard.py` → `dashboards/pipeline.json` | 6 rows: rides funnel, drivers (accept/reject/outcomes, dispatch p95), traffic + HA (copies UP, per-copy load, lag), surge per area, **Autopilot**, data quality + resilience |
| `monitoring/grafana/provisioning/datasources/datasources.yml` | Prometheus + one SQL source (local or Supabase via `GRAFANA_DB_*`) |
| `monitoring/prometheus/prometheus.yml`, `alerts.yml` | finds every copy by DNS (incl. Autopilot clones); 8 alerts |
| `monitoring/compose.yml`, `check_counts.py` | Grafana password from `.env`; consistency checker (payments, 2 winners, driver on 2 rides) |
| `loadtest/locustfile.py`, `chaos.py` | riders + drivers with API keys on `/v1`; 8 chaos scenarios |
| `tests/test_end_to_end.py` | **all 7 service loops running together** + API: full ride, reject → round 2 → cancel, timeouts → NO_DRIVER, invalid booking stored |
| `tests/test_simulator.py`, `test_monitoring_config.py`, `test_reconciler.py`, `test_pipeline_offline.py` | simulator behaviour; every Grafana SQL panel runs on the schema; dashboard matches generator |
| `docs/RUNBOOK.md`, `END-TO-END-FLOWS.md`, `REQUIREMENTS-TRACE.md`, `INTEGRATION-REPORT.md` | |

**Test:** `python -m pytest -q` → **99 passed** (you merge last, so the whole suite must be green on `release/v3`).
**Demo:** Grafana tour row by row while the simulator runs; then the chaos scenarios; `check_counts` PASS.
**Report:** metrics and alerts, dashboards, test strategy, load-test numbers, chaos results.

---

## 3. Demo script (12 minutes)

| Min | Who | Show |
|---|---|---|
| 0–1 | P1 | architecture picture; `docker compose ps` (~25 containers healthy) |
| 1–3 | P3 | `/live`: book a ride → timeline to PAID; repeat Idempotency-Key → same answer; `X-Replica` |
| 3–5 | P4 + P5 | 3 nearest offers; two ACCEPT → one ASSIGNED, one LOST; the rows in `driver_responses` and events in `ride.events` |
| 5–7 | P6 | Grafana: funnel, accept/reject, surge per area, API load split over 3 copies |
| 7–10 | P1 + P5 | chaos: broker-down (0 lost), api-crash (no errors), payment-db-down (DLQ + replay), traffic-spike (Autopilot scales), consumer-hang (Autopilot heals) |
| 10–11 | P2 | `check_counts` PASS; Supabase tables; RLS |
| 11–12 | P1 | GitHub Actions: tests, scans, image published |
