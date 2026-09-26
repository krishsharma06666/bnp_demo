"""Payment service (Person 5) on the TEAM `payments` table: the brief's "Payment DB goes down" scenario
on a REAL database outage.   DB down -> RETRYING x3 -> DLQ -> DB back -> DLQ replay -> PAID exactly once."""
import json

import psycopg
import pytest

from common import db as dbmod
from common.events import payment_requested_event
from services.payment import payment_consumer as pc
from services.payment.writer import PayItem, write_payments
from tests.test_db_retry import set_db_online

BOOK = """INSERT INTO bookings (booking_id, user_id, driver_id, pickup_location, destination_location,
          booking_status, fare, idempotency_key, created_at, updated_at)
          VALUES (%s, 57, 279, 'Indiranagar', 'BTM Layout', 'COMPLETED', 450.00, %s, now(), now())"""


def completed(conn, ids):
    for b in ids:
        conn.execute(BOOK, (b, f"B-{b}"))
    conn.commit()


class Msg:
    """Stand-in for a confluent_kafka Message."""
    def __init__(self, value, offset, topic="payment.requested", key=None, partition=0):
        self._v, self._o, self._t, self._k, self._p = value, offset, topic, key, partition

    def value(self): return self._v
    def key(self): return self._k
    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.sent.append((topic, key, value, dict(headers or [])))

    def poll(self, t=0):
        return 0

    def by_topic(self, t):
        return [s for s in self.sent if s[0] == t]


def payment_msgs(ids, start=0):
    return [Msg(payment_requested_event(b, "450.00", "trip-consumer", user_id=57).to_bytes(), start + i,
                key=str(b).encode()) for i, b in enumerate(ids)]


@pytest.fixture()
def fast(monkeypatch):
    monkeypatch.setattr(dbmod.time, "sleep", lambda s: None)
    monkeypatch.setattr(dbmod, "POOL_TIMEOUT", 1.0)
    monkeypatch.setattr(dbmod, "_pools", {})
    yield
    for p in dbmod._pools.values():
        p.close()


def test_one_payment_per_booking_in_team_table(db):
    completed(db, [7])
    ev = payment_requested_event(7, "115.91", "trip-consumer", user_id=57)
    again = payment_requested_event(7, "115.91", "reconciler", user_id=57)   # different event, same booking
    res = write_payments(db, [PayItem(ev, "t", 0, 1), PayItem(ev, "t", 0, 1), PayItem(again, "t", 0, 2)])
    db.commit()
    assert [o for _, o in res] == ["PAID", "REPLAY", "ALREADY_PAID"]
    row = db.execute("""select payment_id, booking_id, user_id, amount, payment_status, idempotency_key,
                               transaction_reference like 'TXN-7-%%' from payments""").fetchall()
    from decimal import Decimal
    assert row == [(7, 7, 57, Decimal("115.91"), "COMPLETED", "PAY-7", True)]
    assert write_payments(db, [PayItem(payment_requested_event(99999, "1", "x"), "t", 0, 3)])[0][1] == "UNKNOWN_BOOKING"


def test_payment_db_down_retry_dlq_then_replay(scratch_db_url, fast):
    url = scratch_db_url
    with psycopg.connect(url) as c:
        completed(c, [1, 2, 3, 4, 5, 6, 7])
    prod = FakeProducer()
    assert pc.process_batch(payment_msgs([1, 2, 3]), prod, url=url)["PAID"] == 3          # 1. normal

    set_db_online(url, "outage_test", online=False)                                     # 2. real outage
    try:
        summary = pc.process_batch(payment_msgs([4, 5, 6, 7], start=10), prod, url=url)
    finally:
        set_db_online(url, "outage_test", online=True)
    assert summary["DLQ"] == 4
    dlq = prod.by_topic("payment.requested.dlq")
    assert len(dlq) == 4 and dlq[0][3]["attempts"] == b"4" and b"retries failed" in dlq[0][3]["error"]

    replay = [Msg(v, 100 + i, key=k) for i, (_, k, v, _) in enumerate(dlq)]               # 3. DB back: replay
    assert pc.process_batch(replay, prod, url=url)["PAID"] == 4
    replay2 = [Msg(v, 200 + i, key=k) for i, (_, k, v, _) in enumerate(dlq)]              # 4. replay again
    assert pc.process_batch(replay2, prod, url=url)["REPLAY"] == 4
    with psycopg.connect(url) as fresh:
        assert fresh.execute("select count(*) from payments").fetchone()[0] == 7
    completed_ids = [json.loads(v)["booking_id"] for _, _, v, _ in prod.by_topic("payment.completed")]
    assert sorted(completed_ids) == ["1", "2", "3", "4", "5", "6", "7"]
    assert len(prod.by_topic("ride.events")) == 7                                       # PAID events


def test_poison_message_goes_straight_to_dlq(scratch_db_url, fast):
    with psycopg.connect(scratch_db_url) as c:
        completed(c, [9])
    prod = FakeProducer()
    s = pc.process_batch([Msg(b"not json", 1)] + payment_msgs([9], start=2), prod, url=scratch_db_url)
    assert s["DLQ"] == 1 and s["PAID"] == 1
