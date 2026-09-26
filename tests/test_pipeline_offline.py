"""Whole base layer WITHOUT Kafka: CSV -> Validator.process -> Consumer DB writer -> Postgres.
Proves the final numbers before anyone runs docker. (Person 6 owns this test.)

Kafka only moves the messages; this test calls each stage directly in the same order.
"""
import csv
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from common.events import booking_raw_event
from common.topics import BOOKING_REJECTED, BOOKING_VALIDATED
from services.consumer.consumer import rejection_event
from services.consumer.db_writer import Item, write_bookings, write_rejections
from services.validator import rules
from services.validator.validator import process

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 26, 15, 0, tzinfo=rules.IST)


def run_pipeline(db, csv_name, times=1):
    with open(ROOT / "data" / csv_name, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    real_validate = rules.validate
    rejected = []
    with patch("services.validator.validator.validate", lambda r: real_validate(r, NOW)):
        for _ in range(times):
            validated = []
            for i, row in enumerate(rows):                        # Producer -> booking.raw
                raw = booking_raw_event(row).to_bytes()
                topic, ev = process(raw)                           # Validator
                if topic == BOOKING_VALIDATED:
                    validated.append(Item(ev, topic, 0, i))
                else:
                    rejected.append(ev)
            for r in write_bookings(db, validated):               # Consumer
                if r.outcome in ("DUPLICATE", "REJECTED"):
                    rejected.append(rejection_event(r.item.event, r.error_code, r.message))
            db.commit()
    write_rejections(db, rejected)                                 # Rejected writer
    db.commit()
    assert all(ev.event_type == "BookingRejected" for ev in rejected)
    return rejected


def counts(db):
    inserted = db.execute("select count(*) from bookings").fetchone()[0]
    by_code = dict(db.execute("select error_code, count(*) from rejected_records group by 1").fetchall())
    return inserted, by_code


def test_our_dataset_530_inserted_11_rejected(db):
    run_pipeline(db, "bookings.csv")
    inserted, by_code = counts(db)
    assert inserted == 530
    assert by_code == {"INVALID_STATE": 9, "DUPLICATE_IDEMPOTENCY_KEY": 2}
    # booking 539 won; 540 and 541 were the duplicates
    assert db.execute("select booking_id from bookings where idempotency_key='BOOK-0000539'").fetchall() == [(539,)]
    status = dict(db.execute("select booking_status, count(*) from bookings group by 1").fetchall())
    assert status == {"COMPLETED": 316, "DRIVER_ASSIGNED": 67, "CONFIRMED": 49, "CANCELLED": 40,
                      "STARTED": 23, "REQUESTED": 18, "FAILED": 17}


def test_sending_the_file_twice_adds_nothing(db):
    run_pipeline(db, "bookings.csv", times=2)
    inserted, by_code = counts(db)
    assert inserted == 530                                        # still 530
    assert by_code["DUPLICATE_IDEMPOTENCY_KEY"] == 2 + 532        # second pass: every valid row is a duplicate
    assert by_code["INVALID_STATE"] == 9 + 9                       # new events -> rejected again
    assert sum(by_code.values()) == 11 + 532 + 9                   # = 552


def test_bad_file_all_rejected(db):
    run_pipeline(db, "bookings_bad.csv")
    inserted, by_code = counts(db)
    assert inserted == 0
    assert sum(by_code.values()) == 23
    assert set(by_code) == {"SCHEMA_TYPE", "MISSING_FIELD", "INVALID_STATUS", "INVALID_LOCATION",
                            "FARE_RANGE", "INVALID_STATE", "TIME_ORDER"}


def test_poison_message_rejected_not_crashed():
    topic, ev = process(b"this is not json")
    assert topic == BOOKING_REJECTED and ev.payload["error_code"] == "POISON"


def test_payments_eventually_match_completed_bookings(db, paydb):
    """Eventual consistency: every COMPLETED booking gets exactly one payment; reconciler finds 0 gaps."""
    from common.events import payment_requested_event
    from services.payment.writer import PayItem, write_payments
    from services.reconciler.reconciler import find_missing

    run_pipeline(db, "bookings.csv")
    completed = db.execute("select booking_id, fare from bookings where booking_status='COMPLETED'").fetchall()
    # what the booking consumer publishes after each COMPLETED insert:
    items = [PayItem(payment_requested_event(b, f, "consumer", user_id=57), "payment.requested", 0, i)
             for i, (b, f) in enumerate(completed)]
    half = len(items) // 2
    write_payments(paydb, items[:half])                   # payment service processed only half so far...
    paydb.commit()
    count, _ = find_missing(db)
    assert count == len(completed) - half                 # ...reconciler sees the gap
    write_payments(paydb, items)                          # rest arrive (+ first half re-delivered)
    paydb.commit()
    assert find_missing(db)[0] == 0                       # eventually consistent
    assert paydb.execute("select count(*) from payments").fetchone()[0] == 316
