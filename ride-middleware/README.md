# ride-middleware — High-Concurrency Data Integration (v3)

A Kafka middleware between a high-speed stream of ride bookings and a relational database, running a real
Uber-like workflow: **book → nearest 3 drivers → accept / reject / timeout → trip → payment**. Every decision is
stored in PostgreSQL **and** published to Kafka. 3 API copies sit behind a load balancer, there are 3 Kafka brokers,
and every consumer can scale out. The **Autopilot** scales and heals the system by itself, and Grafana shows it all live.

```
 riders / drivers / Locust / simulator
            │ :8000
      ┌─────▼─────┐        Redis (idempotency · surge · ids)
      │  HAProxy  │─────►  API ×3 (FastAPI) ──► booking.raw ─► Validator ─► booking.validated ─► Booking writer ─┐
      └───────────┘              │  │                          └► booking.rejected ─► Rejected writer        │
                                 │  └► driver.response ─► Driver-response writer (race: 1 winner)             ▼
                                 └───► trip.updates ─► Trip writer ─► payment.requested ─► Payment writer   PostgreSQL
                    ride.requested ─► Dispatcher (3 nearest, rounds, timeouts) ─► driver.offers              / Supabase
                    ride.events ◄── every state change          payment DLQ ─► replay · Reconciler
      Kafka: 3 brokers · RF 3 · min.insync 2 · acks=all       Prometheus + alerts ─► Grafana  ·  Autopilot ─► Docker
```

## Quick start (Windows PowerShell; same on Mac/Linux)

```powershell
python -m pip install -r requirements-dev.txt
Copy-Item .env.example .env          # then change every "change-me"
python -m pytest -q                  # 99 passed, no Docker needed
docker compose up -d --build --wait  # the whole platform
start http://localhost:8000/live     # book a ride and watch it live
start http://localhost:3000          # Grafana
```

## Documentation

| Doc | For |
|---|---|
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | start, full workflow by hand, datasets, chaos tests, troubleshooting |
| [docs/END-TO-END-FLOWS.md](docs/END-TO-END-FLOWS.md) | one ride through every service, with the real messages and SQL |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | components, no-loss guarantees, HA, Autopilot, DevSecOps, topics, ports |
| [docs/DATABASE.md](docs/DATABASE.md) | tables, keys, migrations, pool settings, Supabase setup |
| [docs/TEAM-GUIDE.md](docs/TEAM-GUIDE.md) | who owns what, per-member tests, Git workflow, demo script |
| [docs/REQUIREMENTS-TRACE.md](docs/REQUIREMENTS-TRACE.md) | every brief requirement → code → proof |
| [docs/INTEGRATION-REPORT.md](docs/INTEGRATION-REPORT.md) | results and lessons |
| [docs/kafka.md](docs/kafka.md) | Kafka platform details |

## Team

| Person | Area |
|---|---|
| 1 | Kafka platform, shared code, HA (HAProxy, Redis), **Autopilot**, CI/CD, DevSecOps |
| 2 | Database: schema, migrations, Supabase, RLS |
| 3 | Booking API: `/v1`, idempotency, surge, ids, live tracker |
| 4 | Validation, velocity guard, smart dispatch |
| 5 | Writers: bookings, driver race, trips, payments + DLQ |
| 6 | Simulator, reconciler, Grafana, alerts, load + chaos tests, end-to-end test |
