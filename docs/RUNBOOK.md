# Runbook — start, run the full workflow, break it, prove it (Windows PowerShell)

Mac/Linux: same commands (or `make` targets). Needs **Docker Desktop with ≥ 8 GB memory** (Settings → Resources),
Python 3.11+, Git. Every command runs from the repo folder.

## 1. First time on a laptop

```powershell
git clone https://github.com/<org>/ride-middleware.git
cd ride-middleware
python -m venv .venv
.\.venv\Scripts\Activate.ps1                       # if blocked: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
python -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
notepad .env                                        # change every "change-me" value; Supabase lines: see docs/DATABASE.md
python -m pytest -q                                 # 97 passed (no Docker needed)
```

## 2. Start the whole platform

```powershell
docker compose down -v --remove-orphans             # clean start (deletes local data)
docker compose up -d --build --wait                 # builds ONE image, starts ~25 containers, waits for health
docker compose ps
```

Expected: `kafka`, `kafka2`, `kafka3`, `postgres`, `redis`, 3 × `api`, `validator`, `consumer`, `rejected-writer`,
`dispatcher`, `driver-consumer`, `trip-consumer`, `payment-consumer`, `reconciler`, `simulator`, `autopilot` =
**healthy / running**; `kafka-init` and `db-migrate` = **exited (0)** (one-shot jobs).

| Open | URL |
|---|---|
| **Live ride tracker** | http://localhost:8000/live (paste your `RIDER_API_KEY`) |
| Grafana | http://localhost:3000 → "Ride Middleware" (admin / `GRAFANA_ADMIN_PASSWORD`) |
| API docs | http://localhost:8000/docs |
| Load balancer | http://localhost:8404/stats |
| Kafka UI | http://localhost:8080 |
| Prometheus alerts | http://localhost:9090/alerts |

The **simulator** is already booking 2 rides/s and playing 500 drivers (60 % accept, 25 % reject, 15 % ignore), so
Grafana fills up on its own within a minute.

## 3. The full workflow, by hand (what to show first)

```powershell
$env:K = (Select-String -Path .env -Pattern '^RIDER_API_KEY=(.*)').Matches.Groups[1].Value
$env:DK = (Select-String -Path .env -Pattern '^DRIVER_API_KEY=(.*)').Matches.Groups[1].Value

# 1) rider books (twice with the same key -> same answer, ONE booking)
$b = Invoke-RestMethod -Method Post http://localhost:8000/v1/bookings -ContentType application/json `
      -Headers @{ "X-API-Key"=$env:K; "Idempotency-Key"="DEMO-0001" } `
      -Body '{"user_id":7,"pickup_location":"Koramangala","destination_location":"Whitefield"}'
$b                                                   # status ACCEPTED, fare, surge_multiplier, booking_id
Invoke-RestMethod -Method Post http://localhost:8000/v1/bookings -ContentType application/json `
      -Headers @{ "X-API-Key"=$env:K; "Idempotency-Key"="DEMO-0001" } `
      -Body '{"user_id":7,"pickup_location":"Koramangala","destination_location":"Whitefield"}'   # same booking_id

# 2) it is stored and offered to the 3 nearest drivers (the simulator's drivers answer within ~4 s)
Start-Sleep 3
Invoke-RestMethod "http://localhost:8000/v1/bookings/$($b.booking_id)" -Headers @{ "X-API-Key"=$env:K }

# 3) every decision is in the database ...
docker exec postgres psql -U app -d ridehail -c "SELECT driver_id, dispatch_round, response, outcome FROM driver_responses WHERE booking_id=$($b.booking_id) ORDER BY offered_at"
# ... and in Kafka
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ride.events --from-beginning --timeout-ms 5000 | Select-String "$($b.booking_id)"
```

Status goes `REQUESTED → DRIVER_ASSIGNED → STARTED → COMPLETED`, then `payment: COMPLETED` (the simulator drives the trip).
Or open **http://localhost:8000/live**, click *Book ride* and watch the same timeline appear live.

**Drive a ride yourself** (stop the simulator first so it doesn't answer for you):

```powershell
docker compose stop simulator
$b = Invoke-RestMethod -Method Post http://localhost:8000/v1/bookings -ContentType application/json `
      -Headers @{ "X-API-Key"=$env:K; "Idempotency-Key"="DEMO-0002" } `
      -Body '{"user_id":8,"pickup_location":"Indiranagar","destination_location":"Hebbal"}'
Start-Sleep 2
docker exec postgres psql -U app -d ridehail -At -c "SELECT driver_id FROM driver_responses WHERE booking_id=$($b.booking_id) AND response='OFFERED'"
# take two of the offered driver ids:
$d1 = <first id>; $d2 = <second id>
foreach ($d in $d1,$d2) { Invoke-RestMethod -Method Post "http://localhost:8000/v1/drivers/$d/respond" -ContentType application/json -Headers @{ "X-API-Key"=$env:DK } -Body "{`"booking_id`":$($b.booking_id),`"action`":`"ACCEPT`"}" }
Start-Sleep 1
Invoke-RestMethod "http://localhost:8000/v1/bookings/$($b.booking_id)" -Headers @{ "X-API-Key"=$env:K }   # one winner
$w = (Invoke-RestMethod "http://localhost:8000/v1/bookings/$($b.booking_id)" -Headers @{ "X-API-Key"=$env:K }).driver.driver_id
Invoke-RestMethod -Method Post "http://localhost:8000/v1/drivers/$w/trips/$($b.booking_id)/start"    -Headers @{ "X-API-Key"=$env:DK }
Invoke-RestMethod -Method Post "http://localhost:8000/v1/drivers/$w/trips/$($b.booking_id)/complete" -Headers @{ "X-API-Key"=$env:DK }
Start-Sleep 2
Invoke-RestMethod "http://localhost:8000/v1/bookings/$($b.booking_id)" -Headers @{ "X-API-Key"=$env:K }   # COMPLETED, payment COMPLETED
docker compose start simulator
```

## 4. Load the datasets and check the numbers

```powershell
docker compose run --rm producer                         # our 541 rows
python -m monitoring.check_counts --any                   # consistency checks -> RESULT: PASS
docker compose run --rm producer --file data/bookings_bad.csv    # 23 bad rows -> all rejected with a reason
python data/generate_load.py                              # 100,000 rows
docker compose run --rm producer --file data/generated/load_100000.csv
```

Exact numbers on an empty database without simulator traffic (`docker compose stop simulator` first):
our CSV → **530 inserted, 11 rejected, 316 payments**; 100k file → **95,170 inserted, 4,830 rejected**
(2,918 duplicates, 660 INVALID_STATE, 642 INVALID_STATUS, 610 SCHEMA_TYPE), **55,605 payments**.
`python -m monitoring.check_counts --payments 316` → PASS. Sending a file twice stores **0** new bookings.

## 5. Break it on purpose (watch Grafana while each runs)

| Command | What you should see |
|---|---|
| `python -m loadtest.chaos broker-down --seconds 60` | "Kafka brokers up" 3 → 2 → 3; API keeps answering 202; 0 lost |
| `python -m loadtest.chaos api-crash` | http://localhost:8404/stats: one copy DOWN then UP; no failed requests |
| `python -m loadtest.chaos consumer-hang` | ~2 min later: container *unhealthy* → Autopilot **HEAL** (Grafana "Heals", `docker compose logs autopilot`) |
| `python -m loadtest.chaos traffic-spike --seconds 120` | lag rises → Autopilot **SCALE_UP** → lag drains → a minute later **SCALE_DOWN** |
| `python -m loadtest.chaos db-down --seconds 60` | API still 202; writers log `RETRYING`; lag grows; after restart lag → 0; check_counts PASS |
| `python -m loadtest.chaos db-slow --seconds 60` | same as above without errors, just waiting |
| `python -m loadtest.chaos payment-db-down --seconds 60` | bookings/rides keep flowing; payment-consumer `RETRYING` ×3 → `moved to DLQ`; then `docker compose run --rm dlq-replay` or wait for the reconciler |
| `docker compose stop simulator; python -m services.driver.race_test --rounds 20; docker compose start simulator` | `20/20 races had exactly one winner` |
| `locust -f loadtest/locustfile.py --host http://localhost:8000` → http://localhost:8089, 500 users | req/s and p95 in Locust; Grafana "API requests / s per copy" split over 3 copies |

## 6. Everyday commands

```powershell
docker compose logs -f --tail 20 dispatcher driver-consumer   # logs are JSON: level INFO / WARNING / RETRYING / ERROR
docker compose logs autopilot | Select-String "autopilot action"
docker compose up -d --scale validator=3                      # manual scaling still works
docker compose restart api                                    # rolling: LB keeps the others serving
docker compose down                                           # stop (keeps data)   ·   down -v = delete data too
```

## 7. When something is wrong

| Symptom | Fix |
|---|---|
| `open //./pipe/dockerDesktopLinuxEngine` | start Docker Desktop, wait for "Engine running" |
| `kafka-init` exited 1 | `docker compose logs kafka-init`; usually a broker not healthy yet → `docker compose up -d` again |
| `db-migrate` exited 1 | `docker compose logs db-migrate`; Supabase: check the password and use the **pooler** URLs (docs/DATABASE.md) |
| API 401 | send `X-API-Key` with the value from **your** `.env` |
| `/live` shows nothing after "ACCEPTED" | `docker compose ps` — validator/consumer/dispatcher must be running; `docker compose logs validator` |
| Containers restart in a loop, `OOMKilled` | give Docker Desktop more memory (8 GB) |
| Grafana SQL panels empty | `.env` `GRAFANA_DB_*` must point at the same DB as the services, then `docker compose up -d grafana` |
| Port already in use | another program uses 8000/3000/5432 — stop it, or change the left side of `ports:` |
