# Database — Person 2

We use **one PostgreSQL database** for everything (the team schema has foreign keys `payments → bookings`, so bookings
and payments must live together). It runs either on **Supabase** (shared, for the team and the demo) or as the
**local `postgres` container** (for offline work and the "database down" chaos demo). The code is identical; only
`.env` changes.

## Template

| Field | Supabase (team database) | Local container |
|---|---|---|
| **Database name** | `postgres` | `ridehail` (tests: `ridehail_test`) |
| **Username** | `postgres.<project-ref>` (pooler) — direct: `postgres` | `app` |
| **Password** | your Supabase DB password — **only in `.env`, never in Git or chat** | `LOCAL_DB_PASSWORD` in `.env` (default `app`) |
| **Host** | `aws-0-<region>.pooler.supabase.com` | `postgres` (inside Docker) / `localhost` (laptop) |
| **Port** | `6543` transaction pooler (services) · `5432` session pooler (migrations, laptop scripts, Grafana) | `5432` |
| **SSL** | `sslmode=require` | off |
| **Tables** | `users`, `drivers`, `vehicles`, `bookings`, `payments` (team) + `locations`, `driver_responses`, `processed_events`, `rejected_records`, `schema_migrations` (middleware) | same |
| **Primary keys** | see table below | same |
| **Unique indexes** | see table below | same |
| **Foreign keys** | see table below | same |
| **Migration SQL** | `db/migrations/001_schema.sql` … `005_row_level_security.sql`, applied by `python -m db.migrate` | same (the `db-migrate` container runs them on `docker compose up`) |
| **Seed SQL** | `db/migrations/003_seed.sql` — 18 areas, 1,200 riders, 500 drivers, 500 vehicles | same |
| **Connection pool settings** | per service copy: min 1, max `DB_POOL_MAX` (5; use 2 on the free tier), `prepare_threshold=None`, dead-connection check, 5 s checkout timeout | same |

## Tables, keys and indexes

| Table | Primary key | Unique | Foreign keys | Other indexes / checks |
|---|---|---|---|---|
| `users` | `user_id` | `email` | – | – |
| `drivers` | `driver_id` | – | – | `idx_drivers_available (availability_status, current_location)`; check status ∈ AVAILABLE / ON_TRIP / OFFLINE; `version_no` (optimistic lock) |
| `vehicles` | `vehicle_id` | – | `driver_id → drivers` | – |
| `bookings` | `booking_id` | `idempotency_key` | `user_id → users`, `driver_id → drivers` | `idx_bookings_status`, `idx_bookings_driver`; checks: fare 0–5000, known status, pickup ≠ destination; `version_no`, `surge_multiplier`, `dispatch_round`, `assigned_at`, `inserted_at` |
| `payments` | `payment_id` (= booking_id) | `idempotency_key` (`PAY-<id>`), `transaction_reference` | `booking_id → bookings`, `user_id → users` | check amount > 0 |
| `locations` | `name` | – | – | lat / lng of the 18 service areas |
| `driver_responses` | `response_id` | `(booking_id, driver_id)` | `booking_id → bookings`, `driver_id → drivers` | partial indexes on open offers (`response='OFFERED'`) by booking and by driver; response ∈ OFFERED / ACCEPTED / REJECTED / TIMEOUT; outcome ∈ ASSIGNED / LOST / DECLINED / EXPIRED / NOT_OFFERED |
| `processed_events` | `event_id` | – | – | one row per Kafka event ever applied (exactly-once) |
| `rejected_records` | `id` | `event_id` | – | `idx_rejected_code`; raw payload as JSONB |
| `schema_migrations` | `version` | – | – | which migration files are applied |

## Migrations

| File | What | Safe on an existing database? |
|---|---|---|
| `001_schema.sql` | the team schema exactly as designed, with `IF NOT EXISTS` and **without** the `DROP TABLE` lines | yes — never drops data |
| `002_middleware.sql` | new tables, new columns with defaults, normalises old status spellings, CHECKs added `NOT VALID` | yes — additive only |
| `003_seed.sql` | reference data, `ON CONFLICT DO NOTHING` | yes |
| `004_offer_guard.sql` | allows outcome `NOT_OFFERED` (an answer from a driver who got no offer is stored, never wins) | yes |
| `005_row_level_security.sql` | RLS on every table: Supabase's public REST API (anon key) sees nothing; our services own the tables and are unaffected | yes |

`python -m db.migrate` applies each file once, in order, in its own transaction, under a transaction-level advisory
lock — so several runners at once (4 tested) and connection poolers are safe. `python -m db.migrate --status` lists what is applied.

> **Your original Supabase script** drops `driver_responses` but never creates it, and drops all tables every time it
> runs. Don't run it again — run `python -m db.migrate` instead. It keeps your data and adds what is missing.

## Supabase setup (once, by Person 2)

1. Supabase → your project → **Connect** → copy the **Transaction pooler** (port 6543) and **Session pooler** (port 5432) URIs.
   The direct connection (`db.<ref>.supabase.co`) is IPv6-only and usually does not work from Docker Desktop — use the poolers.
2. Put them in `.env` (every teammate does this on their own laptop; share the password privately, never in Git):
   ```ini
   DOCKER_DATABASE_URL=postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:6543/postgres?sslmode=require
   DOCKER_MIGRATE_DATABASE_URL=postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres?sslmode=require
   DATABASE_URL=postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres?sslmode=require
   DB_PREPARE_THRESHOLD=none
   DB_POOL_MAX=2
   GRAFANA_DB_HOST=aws-0-<region>.pooler.supabase.com:5432
   GRAFANA_DB_NAME=postgres
   GRAFANA_DB_USER=postgres.<ref>
   GRAFANA_DB_PASSWORD=<password>
   GRAFANA_DB_SSLMODE=require
   ```
3. Apply the schema from your laptop: `python -m db.migrate` → `applied 001_schema.sql … 005_row_level_security.sql`.
4. Check in Supabase → Table Editor: 10 tables; `drivers` has 500 rows, `locations` 18. Security Advisor: no "RLS disabled" warnings.
5. `docker compose up -d --build --wait` now uses Supabase (the local `postgres` container still starts but nothing uses it).

**Why two ports:** the transaction pooler (6543) lets ~12 containers share a few real connections, but it can't keep
session state — hence `DB_PREPARE_THRESHOLD=none` (no server-side prepared statements) and migrations on the session
pooler (5432).

## Useful queries

```sql
-- the life of one ride
SELECT booking_id, booking_status, driver_id, fare, surge_multiplier, dispatch_round, inserted_at, assigned_at
  FROM bookings WHERE booking_id = :id;
SELECT driver_id, dispatch_round, response, outcome, offered_at, responded_at
  FROM driver_responses WHERE booking_id = :id ORDER BY offered_at;
SELECT * FROM payments WHERE booking_id = :id;

-- consistency checks (all must be 0)
SELECT count(*) FROM bookings b WHERE booking_status = 'COMPLETED'
   AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.booking_id = b.booking_id);        -- missing payments
SELECT booking_id FROM driver_responses WHERE outcome = 'ASSIGNED'
 GROUP BY 1 HAVING count(*) > 1;                                                       -- two winners
SELECT idempotency_key FROM bookings GROUP BY 1 HAVING count(*) > 1;                  -- duplicates
```
