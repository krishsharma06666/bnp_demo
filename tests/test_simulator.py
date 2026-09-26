"""Driver simulator decisions (Person 6): drivers accept / reject / ignore like humans, riders book."""
import random

from services.simulator.simulator import Brain

OFFER = {"booking_id": 11, "round": 1, "drivers": [{"driver_id": d, "km": 1.0} for d in range(1, 1001)]}


def test_driver_mix_of_accept_reject_ignore():
    b = Brain(rng=random.Random(7))
    acts = b.on_offer(OFFER, now=100.0)
    answers = [a.body["action"] for a in acts]
    accept, reject = answers.count("ACCEPT"), answers.count("REJECT")
    ignored = 1000 - len(acts)
    assert 550 < accept < 650 and 200 < reject < 300 and 100 < ignored < 200
    assert all(100.5 <= a.at <= 104.0 for a in acts)                 # human-like delay
    assert all(a.path == f"/v1/drivers/{int(a.path.split('/')[3])}/respond" for a in acts)


def test_assigned_driver_starts_then_completes():
    b = Brain(rng=random.Random(1))
    start, done = b.on_ride_event({"event": "ASSIGNED", "booking_id": 11, "driver_id": 42}, now=0)
    assert start.path == "/v1/drivers/42/trips/11/start" and done.path == "/v1/drivers/42/trips/11/complete"
    assert 2 <= start.at <= 5 and done.at > start.at
    assert b.on_ride_event({"event": "LOST", "booking_id": 11, "driver_id": 43}, now=0) == []


def test_rider_booking_is_valid_and_unique():
    b = Brain(rng=random.Random(3))
    a1, a2 = b.new_booking(0), b.new_booking(0)
    assert a1.role == "rider" and a1.path == "/v1/bookings"
    assert a1.body["pickup_location"] != a1.body["destination_location"]
    assert a1.headers["Idempotency-Key"] != a2.headers["Idempotency-Key"]
