"""Booking API v1 (Person 3). Kafka -> FakeSender, Redis -> fakeredis, database -> fake reader.
No broker, no Redis server, no database needed."""
import json
import threading

import fakeredis
import pytest
from fastapi.testclient import TestClient

from common import config
from services.api.idempotency import IdempotencyStore
from services.api.limits import BookingIdGenerator, NodeLease, TokenBucket
from services.api.main import create_app
from services.api.surge import SurgeEngine, multiplier

RIDER = {"X-API-Key": config.RIDER_API_KEY}
DRIVER = {"X-API-Key": config.DRIVER_API_KEY}
BODY = {"user_id": 57, "pickup_location": "Indiranagar", "destination_location": "BTM Layout"}


class FakeSender:
    def __init__(self, ok=True):
        self.ok, self.sent = ok, []

    def send(self, topic, key, value, timeout=5.0):
        self.sent.append((topic, key, json.loads(value)))
        return self.ok

    def close(self):
        pass


class FakeRides:
    """Stands in for the database read: returns whatever the test put in `rides`."""
    def __init__(self):
        self.rides, self.calls = {}, 0

    def __call__(self, booking_id):
        self.calls += 1
        r = self.rides.get(booking_id)
        return r() if callable(r) else r


@pytest.fixture()
def redis():
    return fakeredis.FakeRedis()


def client(sender=None, redis=None, reader=None):
    return TestClient(create_app(sender=sender or FakeSender(), redis_client=redis, db_reader=reader or FakeRides()))


def book(c, key, body=BODY, **headers):
    return c.post("/v1/bookings", json=body, headers={**RIDER, "Idempotency-Key": key, **headers})


# ------------------------------------------------------------------ auth + security
def test_missing_or_wrong_api_key_is_401():
    with client() as c:
        assert c.post("/v1/bookings", json=BODY, headers={"Idempotency-Key": "K-0001"}).status_code == 401
        assert c.post("/v1/bookings", json=BODY, headers={"X-API-Key": "nope", "Idempotency-Key": "K-0001"}).status_code == 401
        # a rider key cannot act as a driver, and a driver key cannot book
        assert c.post("/v1/drivers/5/respond", json={"booking_id": 1, "action": "ACCEPT"}, headers=RIDER).status_code == 401
        assert c.post("/v1/bookings", json=BODY, headers={**DRIVER, "Idempotency-Key": "K-0002"}).status_code == 401


def test_security_headers_and_replica_header():
    with client() as c:
        r = c.get("/health")
    assert r.headers["X-Content-Type-Options"] == "nosniff"
    assert r.headers["X-Frame-Options"] == "DENY"
    assert r.headers["X-Replica"] == r.json()["replica"]


def test_oversized_body_is_413():
    with client() as c:
        r = c.post("/v1/bookings", content=b"x" * 20_000,
                   headers={**RIDER, "Idempotency-Key": "BIG-1", "Content-Type": "application/json"})
    assert r.status_code == 413


def test_unknown_area_goes_to_validator_bad_shape_is_422():
    """Business rules live in ONE place (the validator), so an unknown area is accepted here and then
    REJECTED by the validator -> stored in rejected_records + booking.rejected (counted in Grafana)."""
    s = FakeSender()
    with client(s) as c:
        r = book(c, "K-0101", {**BODY, "pickup_location": "Mysore"})
        assert r.status_code == 202 and r.json()["surge_multiplier"] == 1.0
        from services.validator.rules import validate
        _, errors, _ = validate(s.sent[0][2]["payload"])
        assert any("LOCATION" in e for e in errors)
        assert book(c, "K-0102", {**BODY, "user_id": "abc"}).status_code == 422
        assert c.post("/v1/bookings", json=BODY, headers=RIDER).status_code == 422      # no Idempotency-Key


# ------------------------------------------------------------------ booking -> Kafka
def test_booking_accepted_202_and_sent_to_kafka(redis):
    s = FakeSender()
    with client(s, redis) as c:
        r = book(c, "APP-0001")
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "ACCEPTED" and body["track"] == f"/v1/bookings/{body['booking_id']}/stream"
    topic, key, ev = s.sent[0]
    assert topic == "booking.raw" and key == "APP-0001"
    row = ev["payload"]
    assert row["booking_status"] == "REQUESTED" and row["driver_id"] == "" and row["surge_multiplier"] == "1.0"
    # the row goes through the SAME validator rules as the CSV rows
    from services.validator.rules import validate
    clean, errors, _ = validate(row)
    assert errors == [] and clean["booking_id"] == body["booking_id"]
    assert body["booking_id"] < 2**53                         # exact in JavaScript (live page)


def test_same_idempotency_key_same_answer_no_second_event(redis):
    s = FakeSender()
    with client(s, redis) as c:
        r1 = book(c, "APP-0002")
        r2 = book(c, "APP-0002")
    assert r2.status_code == 200 and r2.headers["Idempotent-Replay"] == "true"
    assert r1.json() == r2.json()
    assert len(s.sent) == 1


def test_idempotency_is_shared_between_api_copies(redis):
    """Load balancer sends the retry to ANOTHER copy -> still the same answer (Redis is shared)."""
    s1, s2 = FakeSender(), FakeSender()
    with client(s1, redis) as a, client(s2, redis) as b:
        r1 = book(a, "APP-0003")
        r2 = book(b, "APP-0003")
    assert r2.status_code == 200 and r2.json() == r1.json()
    assert len(s1.sent) + len(s2.sent) == 1


def test_same_key_in_flight_is_409(redis):
    redis.set("idem:APP-0004", "__pending__")                # another copy is processing it right now
    with client(redis=redis) as c:
        assert book(c, "APP-0004").status_code == 409


def test_kafka_down_gives_503_and_key_is_freed(redis):
    s = FakeSender(ok=False)
    with client(s, redis) as c:
        assert book(c, "APP-0005").status_code == 503
        s.ok = True                                            # client retries with the SAME key
        assert book(c, "APP-0005").status_code == 202


def test_redis_down_falls_back_to_local_cache():
    class DeadRedis:
        def __getattr__(self, name):
            def boom(*a, **k):
                raise ConnectionError("redis down")
            return boom
    s = FakeSender()
    with client(s, DeadRedis()) as c:
        r1 = book(c, "APP-0006")
        r2 = book(c, "APP-0006")
        assert c.get("/ready").json()["redis"] is False
    assert r1.status_code == 202 and r2.status_code == 200 and len(s.sent) == 1


def test_rate_limit_429_for_one_flooding_client_only(redis):
    with client(redis=redis) as c:
        codes = [book(c, f"F-{i:04d}", **{"X-Client-Id": "spammer"}).status_code for i in range(60)]
        other = book(c, "OK-0001", **{"X-Client-Id": "normal-rider"})
    assert codes.count(202) >= 40 and codes.count(429) > 0    # burst 40 allowed, then throttled
    assert other.status_code == 202                            # other clients unaffected


# ------------------------------------------------------------------ surge
def test_surge_multiplier_rules():
    assert multiplier(0, None) == 1.0                         # supply unknown -> never surge
    assert multiplier(5, 10) == 1.0                           # more drivers than riders
    assert multiplier(20, 10) == 1.5
    assert multiplier(100, 0) == 2.5                          # capped


def test_surge_applied_to_fare(redis):
    redis.hset("supply:available", "Indiranagar", 2)          # dispatcher says: 2 free drivers
    s = FakeSender()
    with client(s, redis) as c:
        normal = book(c, "S-0001").json()                     # demand 0 -> x1.0
        for i in range(7):
            redis.incr(f"demand:Indiranagar:{SurgeEngine(redis)._minute()}")
        surged = book(c, "S-0002").json()                     # demand 8 vs supply 2 -> x2.5
        board = c.get("/v1/surge").json()
    assert normal["surge_multiplier"] == 1.0 and surged["surge_multiplier"] == 2.5
    assert float(surged["fare"]) == pytest.approx(float(normal["fare"]) * 2.5, abs=0.01)
    assert s.sent[-1][2]["payload"]["surge_multiplier"] == "2.5"
    assert board["Indiranagar"]["available_drivers"] == 2 and board["Indiranagar"]["multiplier"] == 2.5


# ------------------------------------------------------------------ rider reads + cancel
def test_get_booking_404_then_found():
    rides = FakeRides()
    with client(reader=rides) as c:
        assert c.get("/v1/bookings/42", headers=RIDER).status_code == 404
        rides.rides[42] = {"booking_id": 42, "status": "REQUESTED"}
        assert c.get("/v1/bookings/42", headers=RIDER).json()["status"] == "REQUESTED"


def test_get_booking_db_down_is_503():
    def broken(_):
        raise RuntimeError("db down")
    with client(reader=broken) as c:
        assert c.get("/v1/bookings/42", headers=RIDER).status_code == 503


def test_cancel_goes_to_trip_updates():
    s = FakeSender()
    with client(s) as c:
        r = c.post("/v1/bookings/77/cancel", headers=RIDER)
    assert r.status_code == 202
    topic, key, ev = s.sent[0]
    assert topic == "trip.updates" and key == "77" and ev["payload"]["action"] == "CANCEL"


def test_live_stream_sends_each_change_and_stops_when_final(monkeypatch):
    import services.api.main as main

    async def no_wait(_):
        return None
    monkeypatch.setattr(main.asyncio, "sleep", no_wait)
    states = iter([
        {"status": "REQUESTED", "payment": None},
        {"status": "REQUESTED", "payment": None},                        # unchanged -> not re-sent
        {"status": "DRIVER_ASSIGNED", "payment": None, "driver": {"driver_id": 9}},
        {"status": "COMPLETED", "payment": None},
        {"status": "COMPLETED", "payment": "COMPLETED"},                 # final -> stream ends
    ])
    rides = FakeRides()
    rides.rides[5] = lambda: next(states)
    with client(reader=rides) as c:
        assert c.get("/v1/bookings/5/stream?key=wrong").status_code == 401
        with c.stream("GET", f"/v1/bookings/5/stream?key={config.RIDER_API_KEY}") as r:
            data = [json.loads(line[6:]) for line in r.iter_lines() if line.startswith("data: ")]
    assert [d["status"] for d in data] == ["REQUESTED", "DRIVER_ASSIGNED", "COMPLETED", "COMPLETED"]
    assert data[-1]["payment"] == "COMPLETED"


def test_live_page_served_with_csp():
    with client() as c:
        r = c.get("/live")
    assert r.status_code == 200 and "EventSource" in r.text
    assert "default-src 'self'" in r.headers["Content-Security-Policy"]


# ------------------------------------------------------------------ driver side
def test_driver_accept_and_reject_keyed_by_booking_id():
    s = FakeSender()
    with client(s) as c:
        a = c.post("/v1/drivers/283/respond", json={"booking_id": 2, "action": "ACCEPT"}, headers=DRIVER)
        b = c.post("/v1/drivers/284/respond", json={"booking_id": 2, "action": "REJECT"}, headers=DRIVER)
        bad = c.post("/v1/drivers/284/respond", json={"booking_id": 2, "action": "MAYBE"}, headers=DRIVER)
    assert a.status_code == b.status_code == 202 and bad.status_code == 422
    assert [(t, k, e["payload"]["driver_id"], e["payload"]["action"]) for t, k, e in s.sent] == [
        ("driver.response", "2", 283, "ACCEPT"), ("driver.response", "2", 284, "REJECT")]


def test_driver_start_and_complete_go_to_trip_updates():
    s = FakeSender()
    with client(s) as c:
        assert c.post("/v1/drivers/9/trips/5/start", headers=DRIVER).status_code == 202
        assert c.post("/v1/drivers/9/trips/5/complete", headers=DRIVER).status_code == 202
    assert [(t, k, e["payload"]["action"], e["payload"]["driver_id"]) for t, k, e in s.sent] == [
        ("trip.updates", "5", "START", 9), ("trip.updates", "5", "COMPLETE", 9)]


def test_metrics_endpoint():
    with client() as c:
        c.get("/health")
        text = c.get("/metrics/").text
    assert "api_requests_total" in text


# ------------------------------------------------------------------ building blocks
def test_ids_unique_under_concurrency_and_across_copies():
    gens = [BookingIdGenerator(node_id=n) for n in (0, 1, 2)]    # 3 API copies
    out = []
    lock = threading.Lock()

    def run(g):
        mine = [g.next() for _ in range(3000)]
        with lock:
            out.extend(mine)
    ths = [threading.Thread(target=run, args=(g,)) for g in gens for _ in range(3)]
    [t.start() for t in ths]
    [t.join() for t in ths]
    assert len(out) == len(set(out)) == 27000
    assert max(out) < 2**53


def test_node_leases_are_unique_and_released(redis):
    a, b, c = (NodeLease(redis, f"api-{i}") for i in range(3))
    assert len({a.node_id, b.node_id, c.node_id}) == 3
    assert a.renew()
    a.release()
    d = NodeLease(redis, "api-new")
    assert d.node_id == a.node_id                              # freed id is reused


def test_idempotency_store_states(redis):
    st = IdempotencyStore(redis)
    assert st.begin("X")[0] == "NEW"
    assert st.begin("X")[0] == "BUSY"
    st.complete("X", {"ok": 1})
    assert st.begin("X") == ("DONE", {"ok": 1})
    assert st.begin("Y")[0] == "NEW"
    st.abort("Y")
    assert st.begin("Y")[0] == "NEW"


def test_token_bucket_refills():
    b = TokenBucket(rate_per_sec=1000, burst=2)
    assert b.allow("x") and b.allow("x") and not b.allow("x")
    import time
    time.sleep(0.01)
    assert b.allow("x")
