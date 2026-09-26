"""Uber-style ride workflow on the TEAM SCHEMA, real PostgreSQL, real concurrency.
dispatch (Person 4) -> driver ACCEPT/REJECT (Person 5) -> trip START/COMPLETE/CANCEL (Person 5)."""
import threading
from collections import Counter

import psycopg

from common.geo import km_between
from services.dispatcher.dispatch import available_by_area, dispatch, sweep
from services.driver.responses import record_response
from services.trip.trip import apply

NEW = """INSERT INTO bookings (booking_id, user_id, pickup_location, destination_location, booking_status,
                              fare, idempotency_key, created_at, updated_at)
         VALUES (%s, 57, %s, 'BTM Layout', 'REQUESTED', 218.00, %s, now(), now())"""


def book(conn, bid, pickup="Koramangala"):
    conn.execute(NEW, (bid, pickup, f"K-{bid}"))
    conn.commit()


def offer(conn, bid, *drivers):
    """What the dispatcher does: record an OFFER for these drivers (only offered drivers can win)."""
    for d in drivers:
        conn.execute("INSERT INTO driver_responses (booking_id, driver_id, dispatch_round, response) "
                     "VALUES (%s, %s, 1, 'OFFERED')", (bid, d))
    conn.commit()


def status(conn, bid):
    conn.rollback()
    return conn.execute("select booking_status, driver_id, version_no from bookings where booking_id=%s", (bid,)).fetchone()


def driver_state(conn, d):
    conn.rollback()
    return conn.execute("select availability_status, version_no, current_location from drivers where driver_id=%s", (d,)).fetchone()


def concurrent(url, fn_args):
    """Run record_response for each (booking, driver, action) at the SAME instant, own connection each."""
    barrier, out = threading.Barrier(len(fn_args)), {}

    def worker(i, args):
        with psycopg.connect(url) as c:
            barrier.wait()
            out[i] = record_response(c, *args)[0]
            c.commit()
    ts = [threading.Thread(target=worker, args=(i, a)) for i, a in enumerate(fn_args)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return [out[i] for i in range(len(fn_args))]


def test_dispatch_offers_the_3_nearest_available_drivers(db):
    book(db, 1, "Koramangala")
    r = dispatch(db, 1)
    db.commit()
    assert r.status == "OFFERED" and r.round == 1 and len(r.drivers) == 3
    # nobody available is closer than the drivers we picked
    locs = dict(db.execute("select driver_id, current_location from drivers").fetchall())
    worst = max(km for _, km in r.drivers)
    offered = {d for d, _ in r.drivers}
    assert all(km_between("Koramangala", locs[d]) >= worst - 0.05 for d in locs if d not in offered)
    rows = db.execute("select response, dispatch_round from driver_responses where booking_id=1").fetchall()
    assert rows == [("OFFERED", 1)] * 3
    assert dispatch(db, 1).status == "WAITING"                   # second call: round still open


def test_three_offered_drivers_accept_at_once_exactly_one_wins(db, pg_url):
    book(db, 2)
    offered = [d for d, _ in dispatch(db, 2).drivers]
    db.commit()
    outcomes = concurrent(pg_url, [(2, d, "ACCEPT") for d in offered])
    assert sorted(outcomes) == ["ASSIGNED", "LOST", "LOST"]
    st, driver, ver = status(db, 2)
    winner = offered[outcomes.index("ASSIGNED")]
    assert (st, driver, ver) == ("DRIVER_ASSIGNED", winner, 2)
    assert driver_state(db, winner)[:2] == ("ON_TRIP", 2)            # driver version_no bumped
    stored = dict(db.execute("select driver_id, response||'/'||outcome from driver_responses where booking_id=2").fetchall())
    assert sorted(stored.values()) == ["ACCEPTED/ASSIGNED", "ACCEPTED/LOST", "ACCEPTED/LOST"]
    assert record_response(db, 2, winner, "ACCEPT")[0] == "ASSIGNED"  # Kafka replay: still the winner


def test_rejections_are_stored_and_next_round_offers_new_drivers(db):
    book(db, 3)
    first = [d for d, _ in dispatch(db, 3).drivers]
    for d in first:
        assert record_response(db, 3, d, "REJECT")[0] == "DECLINED"
    db.commit()
    _, results = sweep(db, timeout_s=15)
    db.commit()
    assert [(r.booking_id, r.status, r.round) for r in results] == [(3, "OFFERED", 2)]
    assert not set(first) & {d for d, _ in results[0].drivers}       # never re-offered to the same driver
    got = Counter(r for (r,) in db.execute("select response from driver_responses where booking_id=3"))
    assert got == {"REJECTED": 3, "OFFERED": 3}


def test_no_answer_times_out_then_no_driver_after_3_rounds(db):
    book(db, 4)
    dispatch(db, 4)
    db.commit()
    for expected_round in (2, 3):
        expired, results = sweep(db, timeout_s=0)                   # every open offer is "too old"
        db.commit()
        assert len(expired) == 3 and results[0].status == "OFFERED" and results[0].round == expected_round
    expired, results = sweep(db, timeout_s=0)
    db.commit()
    assert results[0].status == "NO_DRIVER"
    assert status(db, 4)[0] == "NO_DRIVER"
    assert Counter(r for (r,) in db.execute("select response from driver_responses where booking_id=4")) == {"TIMEOUT": 9}


def test_a_driver_cannot_win_two_rides(db):
    book(db, 5)
    book(db, 6)
    offer(db, 5, 42)
    offer(db, 6, 42)
    assert record_response(db, 5, 42, "ACCEPT")[0] == "ASSIGNED"
    assert record_response(db, 6, 42, "ACCEPT")[0] == "LOST"         # driver 42 is ON_TRIP now
    db.commit()
    assert status(db, 6)[0] == "REQUESTED"                           # booking 6 still searching (savepoint undone)


def test_trip_lifecycle_and_driver_release(db):
    book(db, 7, "Indiranagar")
    offer(db, 7, 100)
    assert record_response(db, 7, 100, "ACCEPT")[0] == "ASSIGNED"
    assert apply(db, 7, "COMPLETE", 100)["result"] == "REJECTED_TRANSITION"   # can't complete before start
    assert apply(db, 7, "START", 100)["result"] == "STARTED"
    assert apply(db, 7, "START", 999)["result"] == "REJECTED_TRANSITION"      # wrong driver
    done = apply(db, 7, "COMPLETE", 100)
    db.commit()
    assert done == {"result": "COMPLETED", "user_id": 57, "fare": "218.00"}
    assert driver_state(db, 100)[0] == "AVAILABLE" and driver_state(db, 100)[2] == "BTM Layout"   # at destination
    assert apply(db, 7, "COMPLETE", 100)["result"] == "REJECTED_TRANSITION"   # replay: nothing changes


def test_rider_cancel_frees_driver_and_closes_offers(db):
    book(db, 8)
    offered = [d for d, _ in dispatch(db, 8).drivers]
    record_response(db, 8, offered[0], "ACCEPT")
    assert apply(db, 8, "CANCEL")["result"] == "CANCELLED"
    db.commit()
    assert driver_state(db, offered[0])[0] == "AVAILABLE"
    assert db.execute("select count(*) from driver_responses where booking_id=8 and response='OFFERED'").fetchone()[0] == 0


def test_100_concurrent_races_never_two_winners(db, pg_url):
    for b in range(1000, 1100):
        book(db, b)
        offer(db, b, *[5 * (b - 1000) + k for k in range(1, 6)])
    for b in range(1000, 1100):
        drivers = [5 * (b - 1000) + k for k in range(1, 6)]             # 5 different drivers per booking
        assert concurrent(pg_url, [(b, d, "ACCEPT") for d in drivers]).count("ASSIGNED") == 1
    db.rollback()
    assert db.execute("select count(*) from bookings where booking_status='DRIVER_ASSIGNED'").fetchone()[0] == 100
    assert db.execute("select count(*) from drivers where availability_status='ON_TRIP'").fetchone()[0] == 100


def test_two_dispatcher_copies_sweeping_at_once_never_double_offer(db, pg_url):
    for b in range(2000, 2030):
        db.execute(NEW.replace("now(), now())", "now(), now())"), (b, "MG Road", f"S-{b}"))
    db.execute("update bookings set inserted_at = now() - interval '1 minute' where booking_id >= 2000")
    db.commit()
    barrier = threading.Barrier(2)

    def sweeper():
        with psycopg.connect(pg_url) as c:
            barrier.wait()
            sweep(c, timeout_s=15)
            c.commit()
    ts = [threading.Thread(target=sweeper) for _ in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    per_booking = dict(db.execute("""select booking_id, count(*) from driver_responses
                                      where booking_id >= 2000 group by 1""").fetchall())
    assert len(per_booking) == 30 and set(per_booking.values()) == {3}
    busy = db.execute("""select driver_id, count(*) from driver_responses where response='OFFERED'
                          group by 1 having count(*) > 1""").fetchall()
    assert busy == []                                                 # no driver holds two open offers


def test_available_by_area(db):
    supply = available_by_area(db)
    assert sum(supply.values()) == 500 and len(supply) == 18


def test_driver_who_was_not_offered_cannot_take_the_ride(db):
    """Security: only the nearest drivers picked by the dispatcher can win. Others are stored, never assigned."""
    book(db, 60, "Koramangala")
    offered = {d for d, _ in dispatch(db, 60).drivers}
    db.commit()
    outsider = next(d for d in range(1, 501) if d not in offered and driver_state(db, d)[0] == "AVAILABLE")
    assert record_response(db, 60, outsider, "ACCEPT")[0] == "NOT_OFFERED"
    db.commit()
    assert status(db, 60)[:2] == ("REQUESTED", None)                  # ride still waiting for an offered driver
    assert driver_state(db, outsider)[0] == "AVAILABLE"
    row = db.execute("select response, outcome from driver_responses where booking_id=60 and driver_id=%s",
                     (outsider,)).fetchone()
    assert row == ("ACCEPTED", "NOT_OFFERED")                        # but the answer IS stored (audit)
    winner = sorted(offered)[0]
    assert record_response(db, 60, winner, "ACCEPT")[0] == "ASSIGNED"
    db.commit()
    assert status(db, 60)[:2] == ("DRIVER_ASSIGNED", winner)
