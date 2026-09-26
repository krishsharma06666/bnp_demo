"""Validation rules. Pure Python, no Kafka - easy to unit test. Owner: Person 4.

validate(row) -> (clean_row or None, errors, warnings)
A row is REJECTED if errors is not empty. Warnings are logged but the row is accepted.
"""
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from common.geo import AREAS

IST = ZoneInfo("Asia/Kolkata")
DATE_FMT = "%d-%m-%Y %H.%M"          # our CSV: 08-12-2026 22.24

STATUSES = {"REQUESTED", "DRIVER_ASSIGNED", "CONFIRMED", "STARTED", "COMPLETED", "CANCELLED", "FAILED"}
NEEDS_DRIVER = {"DRIVER_ASSIGNED", "CONFIRMED", "STARTED", "COMPLETED"}
LOCATIONS = set(AREAS)                 # 18 service areas (common/geo.py, table `locations`)
REQUIRED = ["booking_id", "user_id", "pickup_location", "destination_location",
            "booking_status", "fare", "idempotency_key", "created_at", "updated_at"]
MAX_FARE = Decimal("5000")


def parse_ts(value: str) -> datetime:
    return datetime.strptime(value.strip(), DATE_FMT).replace(tzinfo=IST)


def validate(row: dict, now: datetime | None = None):
    now = now or datetime.now(IST)
    r = {k: (v or "").strip() for k, v in row.items()}
    errors: list[str] = []
    warnings: list[str] = []

    # 1. required fields
    missing = [f for f in REQUIRED if not r.get(f)]
    if missing:
        return None, [f"MISSING_FIELD:{','.join(missing)}"], warnings

    # 2. datatypes
    try:
        booking_id = int(r["booking_id"])
        user_id = int(r["user_id"])
        driver_id = int(r["driver_id"]) if r.get("driver_id") else None
        fare = Decimal(r["fare"])
        if not fare.is_finite():
            raise InvalidOperation
        created_at = parse_ts(r["created_at"])
        updated_at = parse_ts(r["updated_at"])
    except (ValueError, InvalidOperation) as e:
        return None, [f"SCHEMA_TYPE:{type(e).__name__}"], warnings

    # 3. business rules
    status = r["booking_status"]
    if status not in STATUSES:
        errors.append(f"INVALID_STATUS:{status}")
    pickup, dest = r["pickup_location"], r["destination_location"]
    if pickup not in LOCATIONS or dest not in LOCATIONS or pickup == dest:
        errors.append(f"INVALID_LOCATION:{pickup}->{dest}")
    if not (Decimal("0") < fare <= MAX_FARE):
        errors.append(f"FARE_RANGE:{fare}")
    if status in NEEDS_DRIVER and driver_id is None:
        errors.append(f"INVALID_STATE:{status} needs a driver")
    if status == "REQUESTED" and driver_id is not None:
        errors.append(f"INVALID_STATE:REQUESTED must not have driver {driver_id}")
    if updated_at < created_at:
        errors.append("TIME_ORDER:updated_at before created_at")

    # 4. warnings (accepted, but logged)
    if created_at > now:
        warnings.append("FUTURE_TIMESTAMP")
    if status == "FAILED" and driver_id is not None:
        warnings.append("DRIVER_ON_FAILED")

    if errors:
        return None, errors, warnings

    clean = {
        "booking_id": booking_id,
        "user_id": user_id,
        "driver_id": driver_id,
        "pickup_location": pickup,
        "destination_location": dest,
        "booking_status": status,
        "fare": str(fare.quantize(Decimal("0.01"))),
        "idempotency_key": r["idempotency_key"],
        "created_at": created_at.isoformat(),
        "updated_at": updated_at.isoformat(),
        "surge_multiplier": _surge(r.get("surge_multiplier")),
    }
    return clean, [], warnings


def _surge(v) -> str:
    try:
        m = Decimal(v or "1.0")
        return str(m if Decimal("1.0") <= m <= Decimal("3.0") else Decimal("1.0"))
    except InvalidOperation:
        return "1.0"


def error_code(error: str) -> str:
    """'INVALID_STATE:REQUESTED must not...' -> 'INVALID_STATE'"""
    return error.split(":", 1)[0]
