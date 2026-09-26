"""Generate data/bookings_bad.csv: rows that MUST be rejected, one group per error code.
Owner: Person 2.  Run: python data/generate_bad_rows.py
"""
import csv
from pathlib import Path

HEADER = ["booking_id", "user_id", "driver_id", "pickup_location", "destination_location",
          "booking_status", "fare", "idempotency_key", "created_at", "updated_at"]

GOOD = dict(user_id="100", driver_id="200", pickup_location="Koramangala",
            destination_location="Whitefield", booking_status="COMPLETED", fare="450.00",
            created_at="10-03-2026 10.00", updated_at="10-03-2026 10.45")

# (error code the validator must give, field overrides)
CASES = [
    ("SCHEMA_TYPE", dict(fare="abc")),
    ("SCHEMA_TYPE", dict(user_id="U12")),
    ("SCHEMA_TYPE", dict(driver_id="D-9")),
    ("SCHEMA_TYPE", dict(created_at="2026/13/45 10:00")),
    ("SCHEMA_TYPE", dict(fare="12,50")),
    ("MISSING_FIELD", dict(pickup_location="")),
    ("MISSING_FIELD", dict(user_id="")),
    ("MISSING_FIELD", dict(fare="")),
    ("MISSING_FIELD", dict(created_at="")),
    ("INVALID_STATUS", dict(booking_status="DONE")),
    ("INVALID_STATUS", dict(booking_status="completed ")),
    ("INVALID_STATUS", dict(booking_status="ON_THE_WAY")),
    ("INVALID_LOCATION", dict(pickup_location="Mysore")),
    ("INVALID_LOCATION", dict(destination_location="Chennai")),
    ("INVALID_LOCATION", dict(pickup_location="Whitefield", destination_location="Whitefield")),
    ("FARE_RANGE", dict(fare="-50")),
    ("FARE_RANGE", dict(fare="0")),
    ("FARE_RANGE", dict(fare="99999")),
    ("INVALID_STATE", dict(booking_status="REQUESTED", driver_id="15")),
    ("INVALID_STATE", dict(booking_status="COMPLETED", driver_id="")),
    ("INVALID_STATE", dict(booking_status="STARTED", driver_id="")),
    ("TIME_ORDER", dict(created_at="10-03-2026 11.00", updated_at="10-03-2026 10.00")),
    ("TIME_ORDER", dict(created_at="11-03-2026 00.10", updated_at="10-03-2026 23.50")),
]


def build_rows():
    rows = []
    for i, (code, override) in enumerate(CASES, start=1):
        bid = 900000 + i
        row = {"booking_id": str(bid), **GOOD, "idempotency_key": f"BAD-{bid}", **override}
        rows.append((code, row))
    return rows


def main():
    out = Path(__file__).with_name("bookings_bad.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER, lineterminator="\n")
        w.writeheader()
        for _, row in build_rows():
            w.writerow(row)
    print(f"wrote {len(CASES)} bad rows to {out}")


if __name__ == "__main__":
    main()
