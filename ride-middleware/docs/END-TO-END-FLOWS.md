# End-to-end: one ride through the whole system

Priya (rider 7) books **Koramangala → Whitefield** at 18:42. Road distance 18.4 km → base fare
50 + 12 × 18.4 = **₹270.80**. Koramangala is busy (demand 12 in 5 min, 8 free drivers) → surge ×1.2 → **₹324.96**.
IDs below are examples; this exact journey is automated in `tests/test_end_to_end.py`.

## 1. Rider books — API copy 2 of 3 (Person 3)

```http
POST http://localhost:8000/v1/bookings           → HAProxy → api-2
X-API-Key: <RIDER_API_KEY>
Idempotency-Key: WEB-5f1c2a
{"user_id": 7, "pickup_location": "Koramangala", "destination_location": "Whitefield"}
```

1. API key OK → rate limit OK (≤ 20/s per client) → `SET idem:WEB-5f1c2a NX` in Redis → **NEW**.
2. Surge = demand ÷ supply from Redis → ×1.2. Fare ₹324.96. `booking_id` = snowflake (time + api-2's node id + sequence).
3. Sends to `booking.raw` (key `WEB-5f1c2a`) and **waits for acks=all** (2 of 3 brokers have it).
4. Answers **202** in ~5 ms. Stores the answer under the key for 24 h:

```json
{"booking_id": 370128455131264, "status": "ACCEPTED", "fare": "324.96", "surge_multiplier": 1.2,
 "idempotency_key": "WEB-5f1c2a", "track": "/v1/bookings/370128455131264/stream"}
```

The phone retries (bad network) → api-3 answers **200 with the same body**, nothing new in Kafka.

Message on `booking.raw` (the team contract, `common/events.py`):
```json
{"event_id": "7656dd4a-…", "event_type": "BookingRaw", "booking_id": "370128455131264",
 "idempotency_key": "WEB-5f1c2a", "source": "booking-api", "attempt": 0, "schema_version": 1,
 "payload": {"booking_id": "370128455131264", "user_id": "7", "driver_id": "", "pickup_location": "Koramangala",
             "destination_location": "Whitefield", "booking_status": "REQUESTED", "fare": "324.96",
             "idempotency_key": "WEB-5f1c2a", "created_at": "26-09-2026 18.42", "updated_at": "26-09-2026 18.42",
             "surge_multiplier": "1.2"}}
```

## 2. Validator (Person 4)

Checks required fields, types, known area, pickup ≠ destination, fare 0–5,000, dates, status/driver rules, and the
**velocity guard** (rider 7 made 1 booking this minute ≤ 5). → `booking.validated`, then commits its Kafka offset.
*If invalid* (e.g. pickup "Mysore") → `booking.rejected` with `error_code` → Rejected writer → `rejected_records`
(and Grafana "Rejected by reason").

## 3. Booking writer (Person 5)

ONE transaction: `INSERT INTO processed_events (event_id…)` + `INSERT INTO bookings … ON CONFLICT DO NOTHING`.
Commit → then Kafka offset commit. Status REQUESTED → publishes `ride.requested` and `ride.events REQUESTED`.
*Crash between DB commit and offset commit* → message re-read → `processed_events` says "done" → skipped (REPLAY).

## 4. Dispatcher (Person 4)

`SELECT … FROM bookings WHERE booking_id = … FOR UPDATE` → still REQUESTED, no open offers → round 1.
Ranks AVAILABLE drivers by distance from Koramangala, claims each with
`SELECT … FROM drivers WHERE driver_id = … AND availability_status='AVAILABLE' AND no open offer FOR UPDATE SKIP LOCKED`,
inserts 3 rows in `driver_responses` (response `OFFERED`), sets `bookings.dispatch_round = 1`. Publishes:

```json
driver.offers  {"booking_id": 370128455131264, "round": 1, "pickup": "Koramangala", "expires_in_s": 15,
                "drivers": [{"driver_id": 10, "km": 0.0}, {"driver_id": 28, "km": 0.0}, {"driver_id": 46, "km": 0.0}]}
ride.events    {"event": "OFFERED", "booking_id": 370128455131264, "round": 1, "drivers": [10, 28, 46]}
```

## 5. Drivers answer (API → Person 5)

```http
POST /v1/drivers/28/respond   {"booking_id": 370128455131264, "action": "ACCEPT"}   18:42:03.120
POST /v1/drivers/10/respond   {"booking_id": 370128455131264, "action": "ACCEPT"}   18:42:03.121
POST /v1/drivers/46/respond   {"booking_id": 370128455131264, "action": "REJECT"}   18:42:04.500
```
All three go to `driver.response` with key = booking_id → **same partition → processed in order**.

Driver-response writer, for driver 28:
```sql
-- store the answer (only if it was really offered)
UPDATE driver_responses SET response='ACCEPTED', responded_at=now() WHERE … AND response='OFFERED';
SAVEPOINT;
UPDATE bookings SET driver_id=28, booking_status='DRIVER_ASSIGNED', version_no=version_no+1
 WHERE booking_id=370128455131264 AND booking_status='REQUESTED';        -- 1 row → won the ride
UPDATE drivers SET availability_status='ON_TRIP', version_no=version_no+1
 WHERE driver_id=28 AND availability_status='AVAILABLE';                    -- 1 row → driver was free
RELEASE SAVEPOINT;  UPDATE driver_responses SET outcome='ASSIGNED' …;  COMMIT;
```
Driver 10: the booking UPDATE hits 0 rows → **LOST**. Driver 46 → **DECLINED**. All three rows stay in
`driver_responses`; `ride.events` gets `ACCEPTED(28) ASSIGNED(28) ACCEPTED(10) LOST(10) REJECTED(46)`.

| driver_id | round | response | outcome |
|---|---|---|---|
| 28 | 1 | ACCEPTED | ASSIGNED |
| 10 | 1 | ACCEPTED | LOST |
| 46 | 1 | REJECTED | DECLINED |

*Other paths:* all 3 reject → sweep starts round 2 with the next 3 nearest; nobody answers in 15 s → `TIMEOUT` /
`EXPIRED` → next round; after round 3 → `NO_DRIVER`. A driver who was never offered the ride → stored as
`NOT_OFFERED`, can't win.

## 6. Trip (Person 5)

```http
POST /v1/drivers/28/trips/370128455131264/start      → trip.updates → STARTED   (only driver 28, only from DRIVER_ASSIGNED)
POST /v1/drivers/28/trips/370128455131264/complete   → trip.updates → COMPLETED
```
On COMPLETE: booking → COMPLETED, driver 28 → AVAILABLE at **Whitefield** (new position for the next dispatch),
`payment.requested` {amount 324.96, user 7, method UPI}. Rider cancel instead → CANCELLED, driver released, open offers closed.

## 7. Payment (Person 5)

```sql
INSERT INTO payments (payment_id, booking_id, user_id, amount, payment_method, payment_status,
                      idempotency_key, transaction_reference, created_at)
VALUES (370128455131264, 370128455131264, 7, 324.96, 'UPI', 'COMPLETED', 'PAY-370128455131264', 'TXN-…', now())
ON CONFLICT DO NOTHING;
```
→ `payment.completed` + `ride.events PAID`. Database down → `RETRYING` 1 s, 2 s, 4 s → still down → message to
`payment.requested.dlq` with the error in headers. Later: `dlq-replay`, or the **reconciler** finds "COMPLETED but no
payment" after 2 min and requests it again. Either way: exactly one payment row.

## 8. What the rider sees

`/live` (Server-Sent Events, one update per change):
```
✓ ACCEPTED by API        Kafka confirmed · fare ₹324.96 · surge ×1.2
✓ STORED in database
• OFFERED to nearest drivers     round 1, 3 offers
✗ REJECTED by 1 driver(s)
✓ DRIVER ASSIGNED        Driver 28 · Mini Swift · KA-01-0028
✓ TRIP STARTED
✓ TRIP COMPLETED
✓ PAYMENT COMPLETED
```

## 9. What Grafana shows at the same time

Funnel +1 in every box; "Driver answers" donut ACCEPTED 2 / REJECTED 1; outcome ASSIGNED / LOST / DECLINED;
dispatch time ~3 s; Koramangala surge ×1.2; API load split over 3 copies; lag back to 0.

## 10. Where each brief requirement showed up in this one ride

No data loss (acks=all, RF 3, commit-after-DB) · DB consistency (transactions, FKs, CHECKs) · idempotency (Redis key,
Kafka idempotent producer, `processed_events`, UNIQUE keys) · concurrency control (conditional UPDATEs, SKIP LOCKED,
lock order) · driver race (28 vs 10) · retry limit + DLQ (payment) · partial failure + eventual consistency
(reconciler) · INFO / WARNING / RETRYING / ERROR logs · Grafana inserted / rejected · 3 brokers + HA API.
