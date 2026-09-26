"""END-TO-END: every service's REAL run() loop, together, like `docker compose up` - minus Docker.

    rider --HTTP--> API --> booking.raw --> validator --> booking.validated --> consumer --> bookings (DB)
      --> ride.requested --> dispatcher --> driver_responses (OFFERED) + driver.offers
    driver --HTTP--> API --> driver.response --> driver-consumer --> ASSIGNED / LOST / DECLINED (DB + ride.events)
    driver --HTTP--> API --> trip.updates --> trip-consumer --> STARTED / COMPLETED (DB) --> payment.requested
      --> payment-consumer --> payments (DB) --> ride.events PAID
    rider --HTTP--> GET /v1/bookings/{id}: COMPLETED, driver, payment

Kafka is replaced by an in-memory, thread-safe log (same produce / consume / commit calls),
Redis by fakeredis. Everything else is the production code on a real PostgreSQL.
Owner: Person 1 (integration). Takes ~15 s.
"""
import signal
import threading
import time
from collections import defaultdict

import fakeredis
import pytest
from fastapi.testclient import TestClient

from common import config
from common import db as dbmod
from common.events import Envelope
from common.topics import BOOKING_REJECTED, DRIVER_OFFERS, RIDE_EVENTS

RIDER = {"X-API-Key": config.RIDER_API_KEY}
DRIVER = {"X-API-Key": config.DRIVER_API_KEY}


# ------------------------------------------------------------------ in-memory Kafka
class Msg:
    def __init__(self, topic, key, value, headers, offset):
        self._t, self._k, self._v, self._h, self._o = topic, key, value, headers, offset

    def topic(self): return self._t
    def key(self): return self._k.encode() if isinstance(self._k, str) else self._k
    def value(self): return self._v
    def headers(self): return self._h
    def partition(self): return 0
    def offset(self): return self._o
    def error(self): return None


class Bus:
    def __init__(self):
        self.log, self.pos, self.cv = defaultdict(list), defaultdict(int), threading.Condition()

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None, **_):
        with self.cv:
            m = Msg(topic, key, value, headers, len(self.log[topic]))
            self.log[topic].append(m)
            self.cv.notify_all()
        if on_delivery:
            on_delivery(None, m)

    def fetch(self, group, topics, n, timeout):
        end = time.time() + max(timeout, 0)
        with self.cv:
            while True:
                out = []
                for t in topics:
                    p = self.pos[(group, t)]
                    new = self.log[t][p:p + n - len(out)]
                    self.pos[(group, t)] += len(new)
                    out += new
                if out or time.time() >= end:
                    return out
                self.cv.wait(end - time.time())

    def events(self, topic):
        with self.cv:
            return [Envelope.from_bytes(m.value()) for m in self.log[topic]]


class FakeProducer:
    def __init__(self, bus): self.bus = bus
    def produce(self, *a, **k): self.bus.produce(*a, **k)
    def poll(self, *_): return 0
    def flush(self, *_): return 0


class FakeConsumer:
    def __init__(self, bus, group, topics): self.bus, self.group, self.topics = bus, group, topics
    def consume(self, num_messages=1, timeout=1.0): return self.bus.fetch(self.group, self.topics, num_messages, timeout)
    def poll(self, timeout=1.0):
        got = self.bus.fetch(self.group, self.topics, 1, timeout)
        return got[0] if got else None
    def commit(self, *a, **k): pass
    def close(self): pass


class BusSender:                                          # what the API uses instead of KafkaSender
    def __init__(self, bus): self.bus = bus
    def send(self, topic, key, value, timeout=5.0):
        self.bus.produce(topic, key, value)
        return True
    def close(self): pass


# ------------------------------------------------------------------ the running platform
SERVICES = ["services.validator.validator", "services.consumer.consumer", "services.consumer.rejected_writer",
            "services.dispatcher.dispatcher", "services.driver.driver_consumer", "services.trip.trip_consumer",
            "services.payment.payment_consumer"]


@pytest.fixture()
def platform(db, pg_url, monkeypatch):
    import importlib

    import redis

    bus, stops = Bus(), []
    shared = fakeredis.FakeServer()
    monkeypatch.setattr(redis.Redis, "from_url", classmethod(lambda cls, *a, **k: fakeredis.FakeRedis(server=shared)))
    monkeypatch.setattr(signal, "signal", lambda sig, fn: stops.append(fn))
    monkeypatch.setattr(dbmod, "DB_POOL_MAX", 20)
    monkeypatch.setattr(dbmod, "_pools", {})
    real_pool = dbmod.pool
    monkeypatch.setattr(dbmod, "pool", lambda url=None: real_pool(pg_url))
    threads = []
    for name in SERVICES:
        mod = importlib.import_module(name)
        monkeypatch.setattr(mod, "start_http_server", lambda *a, **k: None, raising=False)
        monkeypatch.setattr(mod, "make_producer", lambda **k: FakeProducer(bus), raising=False)
        monkeypatch.setattr(mod, "make_consumer", lambda g, t, **k: FakeConsumer(bus, g, t), raising=False)
        if name.endswith("dispatcher"):
            monkeypatch.setattr(mod, "OFFER_TIMEOUT_S", 1.5)           # fast timeouts for the test
            monkeypatch.setattr(mod, "SWEEP_INTERVAL", 0.5)
        threads.append(threading.Thread(target=mod.run, name=name, daemon=True))
    for t in threads:
        t.start()

    from services.api.main import create_app
    from services.api.reader import get_ride

    def reader(booking_id):
        with dbmod.pool().connection() as c:
            return get_ride(c, booking_id)

    app = create_app(sender=BusSender(bus), redis_client=fakeredis.FakeRedis(server=shared), db_reader=reader)
    with TestClient(app) as api:
        yield api, bus, db
    for stop in stops:
        stop()
    for t in threads:
        t.join(timeout=5)
    for p in dbmod._pools.values():
        p.close()


def wait_for(fn, timeout=20, what=""):
    end = time.time() + timeout
    while time.time() < end:
        got = fn()
        if got:
            return got
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {what}")


def ride_events(bus, booking_id):
    return [e.payload for e in bus.events(RIDE_EVENTS) if e.payload["booking_id"] == booking_id]


def offers(bus, booking_id):
    return [e.payload for e in bus.events(DRIVER_OFFERS) if e.payload["booking_id"] == booking_id]


def book(api, key, pickup="Koramangala", dest="Whitefield", user=7):
    r = api.post("/v1/bookings", headers={**RIDER, "Idempotency-Key": key},
                 json={"user_id": user, "pickup_location": pickup, "destination_location": dest})
    assert r.status_code == 202, r.text
    return r.json()


def answer(api, driver_id, booking_id, action):
    r = api.post(f"/v1/drivers/{driver_id}/respond", headers=DRIVER, json={"booking_id": booking_id, "action": action})
    assert r.status_code == 202


# ------------------------------------------------------------------ the journeys
def test_full_ride_request_race_trip_payment(platform):
    api, bus, db = platform
    ride = book(api, "E2E-HAPPY-1")
    b = ride["booking_id"]
    assert api.post("/v1/bookings", headers={**RIDER, "Idempotency-Key": "E2E-HAPPY-1"},
                    json={"user_id": 7, "pickup_location": "Koramangala",
                          "destination_location": "Whitefield"}).json() == ride          # retry: same answer

    offer = wait_for(lambda: offers(bus, b), what="offer to nearest drivers")[0]
    d1, d2, d3 = [d["driver_id"] for d in offer["drivers"]]
    assert offer["round"] == 1 and len(offer["drivers"]) == 3

    answer(api, d1, b, "ACCEPT")                   # two drivers accept, one rejects
    answer(api, d2, b, "ACCEPT")
    answer(api, d3, b, "REJECT")
    assigned = wait_for(lambda: [e for e in ride_events(bus, b) if e["event"] == "ASSIGNED"], what="ASSIGNED")
    winner = assigned[0]["driver_id"]
    loser = d2 if winner == d1 else d1
    wait_for(lambda: [e for e in ride_events(bus, b) if e["event"] == "LOST"], what="LOST")

    api.post(f"/v1/drivers/{loser}/trips/{b}/start", headers=DRIVER)            # loser can't start the trip
    api.post(f"/v1/drivers/{winner}/trips/{b}/start", headers=DRIVER)
    wait_for(lambda: [e for e in ride_events(bus, b) if e["event"] == "STARTED"], what="STARTED")
    api.post(f"/v1/drivers/{winner}/trips/{b}/complete", headers=DRIVER)
    wait_for(lambda: [e for e in ride_events(bus, b) if e["event"] == "PAID"], what="PAID")

    # --- the rider sees the whole story -------------------------------------------------------
    status = api.get(f"/v1/bookings/{b}", headers=RIDER).json()
    assert status["status"] == "COMPLETED" and status["payment"] == "COMPLETED"
    assert status["driver"]["driver_id"] == winner and status["offers_sent"] == 3 and status["offers_rejected"] == 1

    # --- the database has every decision -------------------------------------------------------
    db.rollback()
    answers = {d: (r, o) for d, r, o in db.execute(
        "SELECT driver_id, response, outcome FROM driver_responses WHERE booking_id=%s", (b,))}
    assert answers == {winner: ("ACCEPTED", "ASSIGNED"), loser: ("ACCEPTED", "LOST"), d3: ("REJECTED", "DECLINED")}
    assert db.execute("SELECT availability_status, current_location FROM drivers WHERE driver_id=%s",
                      (winner,)).fetchone() == ("AVAILABLE", "Whitefield")     # released at the destination
    amount, method = db.execute("SELECT amount, payment_status FROM payments WHERE booking_id=%s", (b,)).fetchone()
    assert str(amount) == ride["fare"] and method == "COMPLETED"

    # --- and Kafka has the same story, in order -------------------------------------------------
    names = [e["event"] for e in ride_events(bus, b)]
    for earlier, later in [("REQUESTED", "OFFERED"), ("OFFERED", "ASSIGNED"), ("ASSIGNED", "STARTED"),
                           ("STARTED", "COMPLETED"), ("COMPLETED", "PAID")]:
        assert names.index(earlier) < names.index(later), names
    assert names.count("ACCEPTED") == 2 and names.count("REJECTED") == 1 and names.count("STARTED") == 1


def test_all_drivers_reject_next_round_then_rider_cancels(platform):
    api, bus, db = platform
    b = book(api, "E2E-REJECT-1", pickup="Indiranagar", dest="Hebbal")["booking_id"]
    first = wait_for(lambda: offers(bus, b), what="round 1")[0]
    for d in first["drivers"]:
        answer(api, d["driver_id"], b, "REJECT")
    second = wait_for(lambda: [o for o in offers(bus, b) if o["round"] == 2], what="round 2")[0]
    assert not {d["driver_id"] for d in first["drivers"]} & {d["driver_id"] for d in second["drivers"]}
    taker = second["drivers"][0]["driver_id"]
    answer(api, taker, b, "ACCEPT")
    wait_for(lambda: [e for e in ride_events(bus, b) if e["event"] == "ASSIGNED"], what="ASSIGNED")
    assert api.post(f"/v1/bookings/{b}/cancel", headers=RIDER).status_code == 202
    wait_for(lambda: [e for e in ride_events(bus, b) if e["event"] == "CANCELLED"], what="CANCELLED")
    db.rollback()
    assert db.execute("SELECT availability_status FROM drivers WHERE driver_id=%s", (taker,)).fetchone()[0] == "AVAILABLE"
    assert db.execute("SELECT count(*) FROM driver_responses WHERE booking_id=%s AND outcome='DECLINED'",
                      (b,)).fetchone()[0] == 3


def test_nobody_answers_times_out_to_no_driver(platform):
    api, bus, db = platform
    b = book(api, "E2E-SILENT-1", pickup="Yelahanka", dest="Hebbal")["booking_id"]
    wait_for(lambda: [e for e in ride_events(bus, b) if e["event"] == "NO_DRIVER"], timeout=30, what="NO_DRIVER")
    names = [e["event"] for e in ride_events(bus, b)]
    assert names.count("OFFERED") == 3 and names.count("TIMEOUT") == 9           # 3 rounds x 3 drivers
    db.rollback()
    assert db.execute("SELECT booking_status FROM bookings WHERE booking_id=%s", (b,)).fetchone()[0] == "NO_DRIVER"
    assert api.get(f"/v1/bookings/{b}", headers=RIDER).json()["status"] == "NO_DRIVER"


def test_invalid_booking_is_rejected_and_stored_in_db_and_kafka(platform):
    api, bus, db = platform
    b = book(api, "E2E-BAD-1", pickup="Mysore")["booking_id"]
    rej = wait_for(lambda: [e for e in bus.events(BOOKING_REJECTED) if e.booking_id == str(b)], what="rejection")
    code = rej[0].payload["error_code"]

    def stored():
        db.rollback()
        return db.execute("SELECT error_code FROM rejected_records WHERE booking_id=%s", (str(b),)).fetchone()
    assert wait_for(stored, what="rejected_records row")[0] == code
    assert api.get(f"/v1/bookings/{b}", headers=RIDER).status_code == 404        # never became a booking
