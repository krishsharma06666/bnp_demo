"""Consumer DB logic (Person 5) against a REAL PostgreSQL with our real schema (Person 2)."""
from datetime import datetime

import pytest

from common.events import booking_raw_event
from services.consumer.db_writer import Item, write_bookings, write_rejections
from services.validator.rules import IST, validate
from services.validator.validator import process

NOW = datetime(2026, 9, 26, 15, 0, tzinfo=IST)


def validated_item(row, offset=0):
    clean, errors, warns = validate(row, NOW)
    assert not errors
    ev = booking_raw_event(row).model_copy(update={"event_type": "BookingValidated", "payload": clean})
    return Item(ev, "booking.validated", 0, offset)


ROW = dict(booking_id="7", user_id="57", driver_id="279", pickup_location="Indiranagar",
           destination_location="BTM Layout", booking_status="COMPLETED", fare="115.91",
           idempotency_key="BOOK-0000007", created_at="06-04-2026 1.29", updated_at="06-04-2026 3.26")


def test_insert_then_replay_is_skipped(db):
    item = validated_item(ROW)
    assert [r.outcome for r in write_bookings(db, [item])] == ["INSERTED"]
    db.commit()
    assert [r.outcome for r in write_bookings(db, [item])] == ["REPLAY"]    # same event again
    db.commit()
    assert db.execute("select count(*) from bookings").fetchone()[0] == 1


def test_same_key_different_event_is_duplicate(db):
    a = validated_item(ROW)
    b = validated_item({**ROW, "booking_id": "540"})          # new event, same idempotency_key
    res = write_bookings(db, [a, b])
    db.commit()
    assert [r.outcome for r in res] == ["INSERTED", "DUPLICATE"]
    assert res[1].error_code == "DUPLICATE_IDEMPOTENCY_KEY"


def test_unknown_user_is_rejected_but_batch_continues(db):
    good = validated_item(ROW)
    bad = validated_item({**ROW, "booking_id": "8", "user_id": "999999", "idempotency_key": "K-8"})
    good2 = validated_item({**ROW, "booking_id": "9", "idempotency_key": "K-9"})
    res = write_bookings(db, [good, bad, good2])
    db.commit()
    assert [r.outcome for r in res] == ["INSERTED", "REJECTED", "INSERTED"]
    assert res[1].error_code == "UNKNOWN_REFERENCE"
    assert db.execute("select count(*) from bookings").fetchone()[0] == 2
    # the rejected event is remembered, so a re-delivery is a REPLAY, not a second rejection
    assert write_bookings(db, [bad])[0].outcome == "REPLAY"


def test_rejections_stored_once(db):
    topic, ev = process(booking_raw_event({**ROW, "fare": "abc"}).to_bytes())
    assert topic == "booking.rejected"
    assert write_rejections(db, [ev, ev]) == 1
    db.commit()
    code = db.execute("select error_code from rejected_records").fetchone()[0]
    assert code == "SCHEMA_TYPE"


@pytest.mark.parametrize("field,value", [("fare", "-1"), ("pickup_location", "BTM Layout")])
def test_database_also_blocks_bad_data(db, field, value):
    """Defence in depth: even if the validator had a bug, the DB refuses bad rows (fare, pickup = destination)."""
    import psycopg
    params = {**validated_item(ROW).event.payload, field: value}
    with pytest.raises((psycopg.errors.CheckViolation, psycopg.errors.ForeignKeyViolation)):
        db.execute("""insert into bookings (booking_id,user_id,driver_id,pickup_location,destination_location,
            booking_status,fare,idempotency_key,created_at,updated_at) values (%(booking_id)s,%(user_id)s,
            %(driver_id)s,%(pickup_location)s,%(destination_location)s,%(booking_status)s,%(fare)s,
            %(idempotency_key)s,%(created_at)s,%(updated_at)s)""", params)
    db.rollback()
