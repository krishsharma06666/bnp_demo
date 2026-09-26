"""City simulator: riders who book and drivers who answer, through the REAL API. Owner: Person 6.

    python -m services.simulator.simulator            (runs as the `simulator` container)

Riders  : RIDER_RPS bookings per second from random areas (users 1..1200) -> POST /v1/bookings
Drivers : read driver.offers; each offered driver, after a human-like delay (0.5-4 s):
            ACCEPT  ~ACCEPT_P   (60 %)  -> POST /v1/drivers/{id}/respond
            REJECT  ~REJECT_P   (25 %)
            ignore  the rest    (15 %)  -> the dispatcher's 15 s timeout handles it
          read ride.events; when THEIR ride is ASSIGNED: start after 2-5 s, complete after TRIP_S.
Everything goes through HAProxy -> API -> Kafka, so the simulator exercises the whole system
and fills the Grafana dashboard. Set RIDER_RPS=0 to only simulate drivers (e.g. with Locust).
"""
import heapq
import itertools
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass, field

from common.health import beat
from common.config import API_URL, DRIVER_API_KEY, RIDER_API_KEY
from common.events import Envelope
from common.geo import AREA_NAMES
from common.logger import get_logger
from common.topics import DRIVER_OFFERS, RIDE_EVENTS

SERVICE = "simulator"
log = get_logger(SERVICE)


@dataclass(order=True)
class Action:
    at: float
    seq: int
    path: str = field(compare=False)
    body: dict | None = field(compare=False, default=None)
    role: str = field(compare=False, default="driver")        # which API key to use
    headers: dict = field(compare=False, default_factory=dict)


class Brain:
    """All decisions, no I/O - so it can be unit-tested with a seeded random generator."""

    def __init__(self, rng: random.Random | None = None, accept_p=0.60, reject_p=0.25,
                 think=(0.5, 4.0), start_after=(2.0, 5.0), trip_s=(5.0, 15.0)):
        self.rng = rng or random.Random()
        self.accept_p, self.reject_p = accept_p, reject_p
        self.think, self.start_after, self.trip_s = think, start_after, trip_s
        self._seq = itertools.count()

    def _act(self, at, path, body=None, role="driver", headers=None):
        return Action(at, next(self._seq), path, body, role, headers or {})

    def on_offer(self, payload: dict, now: float) -> list[Action]:
        out = []
        for d in payload.get("drivers", []):
            roll = self.rng.random()
            if roll < self.accept_p:
                answer = "ACCEPT"
            elif roll < self.accept_p + self.reject_p:
                answer = "REJECT"
            else:
                continue                                          # driver ignores it -> TIMEOUT
            out.append(self._act(now + self.rng.uniform(*self.think), f"/v1/drivers/{d['driver_id']}/respond",
                                 {"booking_id": payload["booking_id"], "action": answer}))
        return out

    def on_ride_event(self, payload: dict, now: float) -> list[Action]:
        if payload.get("event") != "ASSIGNED" or not payload.get("driver_id"):
            return []
        b, d = payload["booking_id"], payload["driver_id"]
        start = now + self.rng.uniform(*self.start_after)
        done = start + self.rng.uniform(*self.trip_s)
        return [self._act(start, f"/v1/drivers/{d}/trips/{b}/start"),
                self._act(done, f"/v1/drivers/{d}/trips/{b}/complete")]

    def new_booking(self, now: float) -> Action:
        pickup = self.rng.choice(AREA_NAMES)
        dest = self.rng.choice([a for a in AREA_NAMES if a != pickup])
        return self._act(now, "/v1/bookings", {"user_id": self.rng.randint(1, 1200), "pickup_location": pickup,
                                               "destination_location": dest},
                         role="rider", headers={"Idempotency-Key": f"SIM-{uuid.uuid4().hex[:16]}"})


def run():
    import httpx

    from common.kafka_client import make_consumer

    rider_rps = float(os.getenv("RIDER_RPS", "2"))
    brain = Brain(accept_p=float(os.getenv("ACCEPT_P", "0.60")), reject_p=float(os.getenv("REJECT_P", "0.25")))
    keys = {"rider": RIDER_API_KEY, "driver": DRIVER_API_KEY}
    http = httpx.Client(base_url=API_URL, timeout=5)
    # its own consumer group: reading offers here does NOT take them away from any other service
    consumer = make_consumer("driver-simulator", [DRIVER_OFFERS, RIDE_EVENTS],
                             **{"auto.offset.reset": "latest", "enable.auto.commit": True})
    queue: list[Action] = []
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", api=API_URL, rider_rps=rider_rps, accept_p=brain.accept_p, reject_p=brain.reject_p)
    next_booking = time.time()
    while running:
        beat()
        for m in consumer.consume(num_messages=200, timeout=0.2):
            if m.error():
                continue
            try:
                ev = Envelope.from_bytes(m.value())
            except Exception:
                continue
            now = time.time()
            acts = brain.on_offer(ev.payload, now) if m.topic() == DRIVER_OFFERS else brain.on_ride_event(ev.payload, now)
            for a in acts:
                heapq.heappush(queue, a)

        now = time.time()
        if rider_rps > 0:
            while next_booking <= now:
                heapq.heappush(queue, brain.new_booking(now))
                next_booking += 1.0 / rider_rps

        while queue and queue[0].at <= now:
            a = heapq.heappop(queue)
            try:
                r = http.post(a.path, json=a.body, headers={"X-API-Key": keys[a.role], **a.headers})
                if r.status_code >= 400:
                    log.warning("api refused", path=a.path, status=r.status_code, detail=r.text[:120])
                else:
                    log.info("sent", path=a.path, body=a.body, replica=r.headers.get("X-Replica"))
            except httpx.HTTPError as e:
                log.retrying("api not reachable - will try again", path=a.path, error=str(e)[:80])
                a.at = now + 1.0
                heapq.heappush(queue, a)
                break
    consumer.close()


if __name__ == "__main__":
    run()
