# Integration report — v3

## Summary

We built a Kafka middleware for a ride-booking platform that behaves like a small Uber. Riders book through a FastAPI
service running as **3 copies behind HAProxy**. Bookings are validated, stored exactly once, and offered to the
**3 nearest available drivers**. Drivers accept or reject; the first valid accept wins. Then the trip runs and the
rider pays. **Every decision is stored in PostgreSQL and published to Kafka** (`driver_responses` + `ride.events`).

The system runs on a 3-broker Kafka cluster (RF 3, `min.insync.replicas=2`, `acks=all`) with 9 scalable services.
The **Autopilot** adds copies when Kafka lag grows and restarts hung containers. Grafana shows the whole flow live.

## Results

"Automated" rows are proven by the test suite (`python -m pytest -q` → **99 passed**; the same suite runs in CI on a
real PostgreSQL). **[measure]** rows are for the team to fill in from the live Docker run on the demo laptop.

| Check | Expected | Status |
|---|---|---|
| Our 541-row CSV | 530 inserted, 11 rejected (9 invalid state, 2 duplicates), 316 payments | ✅ automated (`test_pipeline_offline.py`) · live **[measure]** |
| Same file sent twice | 0 new bookings | ✅ automated |
| 100,000-row load file | 95,170 inserted, 4,830 rejected (2,918 duplicates, 660 INVALID_STATE, 642 INVALID_STATUS, 610 SCHEMA_TYPE), 55,605 payments | ✅ computed on the v3 schema (77 s without Kafka) · live time **[measure]** |
| Full ride: book → offer → race → trip → payment | 1 winner, 1 LOST, 1 DECLINED stored in DB and Kafka; COMPLETED + paid | ✅ automated (`test_end_to_end.py`, real service loops) |
| All drivers reject | round 2 offers 3 *different* drivers | ✅ automated |
| Nobody answers | 3 rounds × 3 TIMEOUT → NO_DRIVER | ✅ automated |
| 100 bookings × 5 drivers accepting at the same instant | exactly 100 winners, 100 drivers ON_TRIP | ✅ automated (real concurrent connections) |
| Two dispatcher copies at once | never the same driver offered twice | ✅ automated |
| Driver not offered the ride sends ACCEPT | stored as NOT_OFFERED, cannot win | ✅ automated |
| API copy killed mid-traffic (HAProxy + 3 copies) | 0 failed requests | ✅ measured here with HAProxy 2.8: 300/300 → 202, load moved to the other 2 copies |
| Real DB outage, payment path | RETRYING ×3 → DLQ; recovers | ✅ automated (`test_db_retry.py`) · live **[measure]** |
| Broker killed mid-traffic | 0 lost | live **[measure]** (`scripts/broker_failover_test.py`) |
| Traffic spike | Autopilot SCALE_UP, lag drains, SCALE_DOWN after calm | ✅ logic automated · live **[measure]** time to drain |
| Hung consumer | unhealthy → Autopilot HEAL | ✅ logic automated · live **[measure]** time to heal |
| API throughput (1 copy, Kafka stubbed, v2 measure) | ~930 req/s, p95 79 ms | reference; live with 3 copies + Kafka **[measure]** with Locust |
| Every Grafana SQL panel | runs on the schema | ✅ automated |
| Security scans | ruff, bandit, gitleaks, pip-audit clean | ✅ run here: all clean; Trivy runs in CI |

## Problems we hit and how we fixed them

| Problem | Fix |
|---|---|
| Two API copies could create the same `booking_id` in the same millisecond → the second booking would be silently dropped by `ON CONFLICT DO NOTHING` | snowflake ids with a **per-copy node id leased in Redis**; 53 bits so the browser reads them exactly |
| A driver who was never offered a ride could accept it and bypass nearest-driver dispatch | offer guard: only drivers with an open offer can win; others are stored as `NOT_OFFERED` |
| Existing Supabase rows had "Available" instead of "AVAILABLE" → new CHECK constraints blocked updates | normalise values first, add CHECKs `NOT VALID` |
| Two dispatcher copies offered the same driver | claim the driver row with `FOR UPDATE SKIP LOCKED` |
| Supabase session pooler has few connections for ~12 containers | services on the transaction pooler (6543, no prepared statements), migrations on the session pooler |
| A session-level advisory lock can leak through a pooler | migrations take a transaction-level lock per file |
| Supabase exposes `public` tables through its REST API | row-level security on every table (owner-only access) |
| A hung process stays "running" so Docker never restarts it | heartbeat healthcheck + Autopilot HEAL |
| Topic script had a stray line after the list (would stop `kafka-init`) | fixed + test that runs the script and compares with `ALL_TOPICS` |

## Limits and next steps

- Kafka UI, Prometheus and Grafana run once each (fine for monitoring; not in the data path).
- Redis is a single node: if it is down the API still works (per-copy cache + DB unique keys, surge 1.0).
- Real GPS would replace the 18-area model; `locations` would become PostGIS points.
- Optional vector search: pgvector on Supabase for "similar routes / rider preferences" — not built.
