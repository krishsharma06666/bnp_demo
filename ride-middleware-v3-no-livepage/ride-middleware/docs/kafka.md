# Kafka platform — Person 1

> **Current: Kafka v3.** 3 brokers (`kafka`, `kafka2`, `kafka3`, KRaft), replication factor 3, `min.insync.replicas=2`,
> 6 partitions, **12 topics** (see the table in [ARCHITECTURE.md](ARCHITECTURE.md#6-topics)), `kafka-init` creates them on
> `docker compose up`. Laptop bootstrap: `localhost:9092,localhost:9094,localhost:9096`.
> v3 added `ride.requested`, `driver.offers`, `trip.updates`, `ride.events` (every ride state change) and `ops.events`
> (Autopilot actions). Failover proof: `python -m scripts.broker_failover_test --kill kafka2`.
> The history below explains how `common/` was built in v1/v2.

We build Kafka in versions. Each version must pass its checks before the next one starts.

| Version | What it adds | Status |
|---|---|---|
| **v1** | 1 broker (KRaft), 3 core topics, shared `common/` code, Kafka UI, smoke test with `bookings.csv` | **this branch** |
| v2 | 3 brokers, replication factor 3, `min.insync.replicas=2`, broker-failover test | next |
| v3 | Retry topics (`*.retry.1`, `*.retry.2`), DLQ (`*.dlq`), `processing.events` for the dashboard | after Person 5 starts retries |
| v4 (optional) | Outbox + Debezium for the Booking API | only if time allows |

---

## v1 — what's in it

```
ride-middleware/
├── docker-compose.yml            # kafka + kafka-init (creates topics) + kafka-ui
├── Makefile                      # make up / smoke / test / topics / down
├── requirements.txt
├── .env.example  .gitignore  pytest.ini
├── infra/kafka/create_topics.sh  # booking.raw, booking.validated, booking.rejected
├── common/                       # every teammate imports this
│   ├── config.py                 #   KAFKA_BOOTSTRAP from env
│   ├── topics.py                 #   topic name constants
│   ├── events.py                 #   Envelope = message format (team contract)
│   └── kafka_client.py           #   make_producer / make_consumer with safe settings
├── scripts/kafka_smoke_test.py   # sends the CSV through Kafka and verifies it
├── tests/test_common.py          # 7 unit tests, no Kafka needed
└── data/bookings.csv             # our 541 rows
```

**Topics (v1):**

| Topic | Key | Written by | Read by |
|---|---|---|---|
| `booking.raw` | idempotency_key | Producer (Person 3) | Validator (Person 4) |
| `booking.validated` | idempotency_key | Validator (Person 4) | Consumer (Person 5) |
| `booking.rejected` | idempotency_key | Validator / Consumer | Dashboard (Person 6) |

3 partitions each, 7-day retention, auto-create disabled (a typo in a topic name fails loudly instead of silently creating a new topic).

**Why the key is `idempotency_key`:** rows 539, 540 and 541 share `BOOK-0000539`. Same key → same partition → read in file order, so booking 539 is always stored first and 540/541 are always the duplicates.

---

## v1 — how to run (on your laptop)

Needs Docker Desktop and Python 3.11+.

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
make install          # or: pip install -r requirements.txt
make test             # 7 unit tests, no Kafka needed
make up               # starts Kafka, creates topics, starts Kafka UI
make topics           # shows the 3 topics with 3 partitions each
make smoke            # sends bookings.csv and reads it back
```

No `make` on Windows? Run the commands inside the Makefile directly, for example `docker compose up -d --wait kafka kafka-ui`, then `docker compose run --rm kafka-init`, then `python -m scripts.kafka_smoke_test`.

Open **http://localhost:8080** (Kafka UI) → Topics → `booking.raw` → Messages to see every row.

**Expected smoke-test output** (timings and partition counts will differ):

```
run_id=3f9a1c2e  rows in file=541
messages per partition: {0: ..., 1: ..., 2: ...}

=== Kafka v1 smoke test ===
[PASS] 1. topics exist                             booking.raw, booking.validated, booking.rejected
[PASS] 2. all rows delivered                       sent 541/541, failed 0, 0.4s
[PASS] 3. nothing lost                             read back 541/541
[PASS] 4. nothing read twice                       duplicate deliveries: 0
[PASS] 5. same key -> same partition, in order     BOOK-0000539: bookings ['539', '540', '541'] on partition [n]
RESULT: ALL PASSED
```

The test can be run many times: each run is tagged with its own `run_id`, so earlier messages are ignored.

---

## v1 — definition of done

- [ ] `make test` → 7 passed
- [ ] `make up` → kafka and kafka-ui healthy, kafka-init prints the 3 topics
- [ ] `make smoke` → ALL PASSED
- [ ] Kafka UI shows 541 messages in `booking.raw` after one smoke run
- [ ] Every teammate runs `make up && make smoke` on their own laptop and gets ALL PASSED
- [ ] Merged to `main` through a pull request

---

## How teammates use it

Nobody writes Kafka config by hand. Import from `common/`:

```python
# Person 3 — Producer
from common.events import booking_raw_event
from common.kafka_client import make_producer, publish
from common.topics import BOOKING_RAW

p = make_producer()
ev = booking_raw_event(row)                       # row = dict from csv.DictReader
publish(p, BOOKING_RAW, ev.kafka_key(), ev.to_bytes())
p.flush()
```

```python
# Person 4 — Validator (Person 5's consumer looks the same with BOOKING_VALIDATED)
from common.events import Envelope
from common.kafka_client import make_consumer
from common.topics import BOOKING_RAW

c = make_consumer("validator", [BOOKING_RAW])
while True:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        continue
    ev = Envelope.from_bytes(msg.value())
    # ... validate ev.payload, publish to booking.validated or booking.rejected ...
    c.commit(message=msg, asynchronous=False)      # commit ONLY after the work succeeded
```

Scripts on your laptop use `localhost:9092` (the default). A service running inside docker compose sets `KAFKA_BOOTSTRAP=kafka:29092`.

---

## Push v1 to the shared repo

```bash
git switch -c feat/kafka-v1
git add docker-compose.yml Makefile requirements.txt .env.example .gitignore pytest.ini \
        infra/ common/ scripts/ tests/ data/bookings.csv docs/kafka.md
git commit -m "feat(kafka): v1 single-broker Kafka, core topics, shared client, smoke test"
git push -u origin feat/kafka-v1
# open a pull request -> 1 review -> merge -> tell the team "Kafka v1 is on main"
```

---

## v2 preview (next version)

1. Replace the single `kafka` service with `kafka1`, `kafka2`, `kafka3` (same settings, `KAFKA_NODE_ID` 1/2/3, voters `1@kafka1:9093,2@kafka2:9093,3@kafka3:9093`, host ports 9092/9093/9094 → use different EXTERNAL ports).
2. Set replication factor 3, `KAFKA_MIN_INSYNC_REPLICAS=2`, `KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE=false`, offsets/transaction topics RF 3, min ISR 2.
3. `KAFKA_BOOTSTRAP=localhost:9092,localhost:9094,localhost:9096`.
4. New test: start `make smoke` in a loop, `docker stop kafka2` in the middle → still ALL PASSED; `make topics` shows the leader moved; restart kafka2 → under-replicated partitions return to 0.
