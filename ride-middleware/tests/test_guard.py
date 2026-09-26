"""Velocity guard (Person 4): max 5 API bookings per user per minute, Redis shared by all validators."""
import fakeredis

import pytest

from services.validator import guard as guard_mod
from services.validator.guard import VelocityGuard
from services.validator.validator import process


@pytest.fixture(autouse=True)
def frozen_minute(monkeypatch):
    """Counters are per minute: freeze the clock so a test never straddles a minute boundary."""
    monkeypatch.setattr(guard_mod.time, "time", lambda: 1_790_000_000.0)


def test_velocity_guard_counts_per_user_per_minute():
    g = VelocityGuard(fakeredis.FakeRedis(), limit=5)
    assert [g.too_fast(7) for _ in range(6)] == [False] * 5 + [True]
    assert g.too_fast(8) is False                                       # other users unaffected


def test_velocity_guard_skipped_when_redis_down():
    class Dead:
        def pipeline(self):
            raise ConnectionError("down")
    assert VelocityGuard(Dead()).too_fast(7) is False
    assert VelocityGuard(None).count(7) is None


def test_validator_rejects_api_flood_but_not_csv_rows():
    from services.api.main import csv_time
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from common.events import booking_raw_event

    now = csv_time(datetime.now(ZoneInfo("Asia/Kolkata")))
    guard = VelocityGuard(fakeredis.FakeRedis(), limit=2)

    def row(i, source):
        r = {"booking_id": str(9000 + i), "user_id": "5", "driver_id": "", "pickup_location": "Koramangala",
             "destination_location": "Whitefield", "booking_status": "REQUESTED", "fare": "300.00",
             "idempotency_key": f"V-{source}-{i}", "created_at": now, "updated_at": now}
        return booking_raw_event(r, source=source).to_bytes()

    api = [process(row(i, "booking-api"), guard=guard) for i in range(3)]
    csv = [process(row(i, "producer"), guard=guard) for i in range(3)]
    reasons = [str(r) for r in api]
    assert "USER_VELOCITY" in reasons[2] and "USER_VELOCITY" not in reasons[0]
    assert all("USER_VELOCITY" not in str(r) for r in csv)
