"""Validator rules (Person 4). No Kafka, no DB needed."""
import csv
from collections import Counter
from datetime import datetime
from pathlib import Path

from services.validator.rules import IST, error_code, validate

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 26, 15, 0, tzinfo=IST)   # fixed "today" so results never change


def load(name):
    with open(ROOT / "data" / name, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_our_dataset_gives_9_rejections_all_invalid_state():
    rows = load("bookings.csv")
    rejected = [r["booking_id"] for r in rows if validate(r, NOW)[1]]
    assert rejected == ["3", "37", "58", "106", "165", "205", "313", "352", "423"]
    codes = Counter(error_code(validate(r, NOW)[1][0]) for r in rows if validate(r, NOW)[1])
    assert codes == {"INVALID_STATE": 9}


def test_532_rows_pass_validation():
    # 541 - 9 = 532 go to booking.validated; the consumer later finds 2 duplicates -> 530 stored
    rows = load("bookings.csv")
    assert sum(1 for r in rows if not validate(r, NOW)[1]) == 532


def test_warnings():
    rows = load("bookings.csv")
    warns = Counter(w for r in rows if not validate(r, NOW)[1] for w in validate(r, NOW)[2])
    assert warns["DRIVER_ON_FAILED"] == 3
    assert warns["FUTURE_TIMESTAMP"] > 0


def test_clean_row_is_typed():
    clean, errors, _ = validate(load("bookings.csv")[6], NOW)   # booking 7
    assert errors == []
    assert clean["booking_id"] == 7 and clean["driver_id"] == 279
    assert clean["fare"] == "115.91"
    assert clean["created_at"] == "2026-04-06T01:29:00+05:30"


def test_every_bad_row_rejected_with_expected_code():
    from data.generate_bad_rows import build_rows
    for expected, row in build_rows():
        clean, errors, _ = validate(row, NOW)
        assert clean is None, row
        assert error_code(errors[0]) == expected, (row, errors)


def test_bad_csv_file_matches_generator():
    assert len(load("bookings_bad.csv")) == 23
