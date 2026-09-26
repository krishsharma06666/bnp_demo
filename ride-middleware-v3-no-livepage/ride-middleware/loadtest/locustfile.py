"""Traffic generator for the whole ride platform. Owner: Person 6.

    locust -f loadtest/locustfile.py --host http://localhost:8000
    -> open http://localhost:8089, e.g. 500 users, spawn rate 50

Headless (for the report):
    locust -f loadtest/locustfile.py --host http://localhost:8000 --headless -u 500 -r 50 -t 2m --csv results/load

Keys come from the same .env as the services (RIDER_API_KEY / DRIVER_API_KEY).
Rider (95 % of users):
  - books rides with a unique Idempotency-Key                 -> 202
  - 3 % re-sends a previous key (network retry)               -> 200, NO second booking
  - 2 % sends an unknown area                                 -> 202, then REJECTED by the validator (stored)
  - checks the status of its rides                            -> GET /v1/bookings/{id}
Driver (5 % of users): answers random offers and finishes trips (the simulator does the realistic version).
Rate limit is per X-Client-Id, so a flooding rider gets 429 and nobody else is affected.
"""
import random
import uuid

from locust import HttpUser, between, task

from common.config import DRIVER_API_KEY, RIDER_API_KEY
from common.geo import AREA_NAMES as LOCS


class Rider(HttpUser):
    weight = 19
    wait_time = between(0.05, 0.3)

    def on_start(self):
        self.client_id = f"rider-{uuid.uuid4().hex[:8]}"
        self.user_id = random.randint(1, 1200)
        self.sent_keys: list[str] = []
        self.booked: list[int] = []
        self.h = {"X-API-Key": RIDER_API_KEY, "X-Client-Id": self.client_id}

    def _book(self, key: str, pickup: str, dest: str):
        with self.client.post("/v1/bookings", name="/v1/bookings",
                              json={"user_id": self.user_id, "pickup_location": pickup, "destination_location": dest},
                              headers={**self.h, "Idempotency-Key": key}, catch_response=True) as r:
            if r.status_code in (200, 202):
                r.success()
                if r.status_code == 202:
                    self.booked.append(r.json()["booking_id"])
            elif r.status_code in (409, 429):
                r.success()                     # throttling / in-flight duplicate is expected behaviour
            else:
                r.failure(f"HTTP {r.status_code}")

    @task(90)
    def book_ride(self):
        roll = random.random()
        if roll < 0.03 and self.sent_keys:                       # network retry: same key again
            key = random.choice(self.sent_keys)
            pickup, dest = random.sample(LOCS, 2)
        elif roll < 0.05:                                         # bad data -> Validator rejects it
            key, pickup, dest = f"LT-{uuid.uuid4().hex}", "Mysore", random.choice(LOCS)
        else:
            key = f"LT-{uuid.uuid4().hex}"
            pickup, dest = random.sample(LOCS, 2)
        self.sent_keys.append(key)
        self._book(key, pickup, dest)

    @task(10)
    def check_status(self):
        if self.booked:
            with self.client.get(f"/v1/bookings/{random.choice(self.booked)}", name="/v1/bookings/{id}",
                                 headers=self.h, catch_response=True) as r:
                if r.status_code in (200, 404):       # 404 = not stored YET (async) - expected for a moment
                    r.success()


class Driver(HttpUser):
    weight = 1
    wait_time = between(0.5, 2)

    def on_start(self):
        self.driver_id = random.randint(1, 500)
        self.h = {"X-API-Key": DRIVER_API_KEY, "X-Client-Id": f"driver-{self.driver_id}"}

    @task
    def answer_random_offer(self):
        # unsolicited answers are stored too and never assign a ride that wasn't offered to this driver
        self.client.post(f"/v1/drivers/{self.driver_id}/respond", name="/v1/drivers/{id}/respond",
                         json={"booking_id": random.randint(1, 541), "action": random.choice(["ACCEPT", "REJECT"])},
                         headers=self.h)
