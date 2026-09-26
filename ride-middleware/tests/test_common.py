"""Unit tests for the Kafka shared code. No Kafka needed: `pytest -q`."""
import csv
from collections import Counter
from pathlib import Path

from common.events import Envelope, booking_raw_event
from common.kafka_client import CONSUMER_DEFAULTS, PRODUCER_DEFAULTS
from common.topics import V1_TOPICS

DATA = Path(__file__).resolve().parents[1] / "data" / "bookings.csv"


def rows():
    with open(DATA, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_dataset_shape():
    r = rows()
    assert len(r) == 541
    assert list(r[0].keys()) == [
        "booking_id", "user_id", "driver_id", "pickup_location", "destination_location",
        "booking_status", "fare", "idempotency_key", "created_at", "updated_at",
    ]


def test_every_row_becomes_an_event_and_round_trips():
    for row in rows():
        ev = booking_raw_event(row)
        back = Envelope.from_bytes(ev.to_bytes())
        assert back == ev
        assert back.payload == row


def test_event_ids_are_unique():
    ids = [booking_raw_event(r).event_id for r in rows()]
    assert len(set(ids)) == len(ids)


def test_key_is_idempotency_key():
    keys = Counter(booking_raw_event(r).kafka_key() for r in rows())
    shared = {k: n for k, n in keys.items() if n > 1}
    assert shared == {"BOOK-0000539": 3}          # rows 539, 540, 541 share one key


def test_bad_row_does_not_crash_envelope():
    ev = booking_raw_event({"booking_id": "abc", "fare": "xyz", "idempotency_key": ""})
    assert ev.kafka_key() == "abc"                # falls back to booking_id


def test_safe_kafka_settings():
    assert PRODUCER_DEFAULTS["acks"] == "all"
    assert PRODUCER_DEFAULTS["enable.idempotence"] is True
    assert CONSUMER_DEFAULTS["enable.auto.commit"] is False


def test_topic_names():
    assert V1_TOPICS == ["booking.raw", "booking.validated", "booking.rejected"]
