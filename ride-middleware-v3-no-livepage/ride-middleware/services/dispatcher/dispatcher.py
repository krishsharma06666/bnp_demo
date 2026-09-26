"""Dispatcher service: ride.requested -> offers to the nearest drivers. Owner: Person 4.

    python -m services.dispatcher.dispatcher

Loop:
  - new ride.requested events      -> dispatch()  -> driver.offers + ride.events OFFERED
  - every SWEEP_INTERVAL seconds   -> sweep()     -> TIMEOUT events, next rounds, NO_DRIVER
  - every 10 seconds               -> available drivers per area -> Redis (surge) + Prometheus gauge
Scale with `docker compose up -d --scale dispatcher=2`: Kafka splits ride.requested partitions,
and the sweep uses FOR UPDATE SKIP LOCKED so copies never work on the same booking.
"""
import os
import signal
import time

from prometheus_client import start_http_server

from common.health import beat
from common.config import METRICS_PORT, OFFER_TIMEOUT_S, REDIS_URL
from common.db import with_db_retry
from common.events import Envelope, ride_event
from common.kafka_client import make_consumer, make_producer, publish
from common.logger import get_logger
from common.metrics import AVAILABLE_DRIVERS, OFFERS, RECEIVED, RIDE_EVENTS as RIDE_EV
from common.topics import DRIVER_OFFERS, RIDE_EVENTS, RIDE_REQUESTED
from services.dispatcher.dispatch import DispatchResult, available_by_area, dispatch, sweep

SERVICE = "dispatcher"
log = get_logger(SERVICE)
SWEEP_INTERVAL = float(os.getenv("SWEEP_INTERVAL_S", "2"))


def announce(producer, r: DispatchResult):
    """Publish what a dispatch decided."""
    if r.status == "OFFERED":
        offer = Envelope(event_type="DriverOffer", booking_id=str(r.booking_id), source=SERVICE,
                         payload={"booking_id": r.booking_id, "round": r.round, "pickup": r.pickup,
                                  "expires_in_s": OFFER_TIMEOUT_S,
                                  "drivers": [{"driver_id": d, "km": km} for d, km in r.drivers]})
        publish(producer, DRIVER_OFFERS, offer.kafka_key(), offer.to_bytes())
        ev = ride_event(r.booking_id, "OFFERED", SERVICE, round=r.round, drivers=[d for d, _ in r.drivers])
        OFFERS.labels(SERVICE).inc(len(r.drivers))
        log.info("offered to nearest drivers", booking_id=r.booking_id, round=r.round, drivers=r.drivers)
    elif r.status == "NO_DRIVER":
        ev = ride_event(r.booking_id, "NO_DRIVER", SERVICE, rounds=r.round)
        log.warning("no driver found - booking closed", booking_id=r.booking_id, rounds=r.round)
    elif r.status == "NO_CANDIDATES":
        log.warning("no available driver right now, will retry", booking_id=r.booking_id, round=r.round)
        return
    else:
        return
    RIDE_EV.labels(SERVICE, ev.payload["event"]).inc()
    publish(producer, RIDE_EVENTS, ev.kafka_key(), ev.to_bytes())


def publish_supply(conn, redis_client):
    supply = available_by_area(conn)
    for area, n in supply.items():
        AVAILABLE_DRIVERS.labels(area).set(n)
    if redis_client is not None:
        try:
            redis_client.hset("supply:available", mapping={a: n for a, n in supply.items()} or {"_": 0})
            redis_client.expire("supply:available", 60)
        except Exception as e:
            log.warning("redis unavailable - surge falls back to 1.0", error=str(e)[:100])


def run():
    start_http_server(METRICS_PORT)
    try:
        import redis
        rds = redis.Redis.from_url(REDIS_URL, socket_timeout=0.5, socket_connect_timeout=0.5)
    except Exception:
        rds = None
    consumer = make_consumer("dispatcher", [RIDE_REQUESTED], **{"max.poll.interval.ms": 900000})
    producer = make_producer()
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", reads=RIDE_REQUESTED, offer_timeout_s=OFFER_TIMEOUT_S, sweep_s=SWEEP_INTERVAL)
    last_sweep = last_supply = 0.0
    while running:
        beat()
        msgs = [m for m in consumer.consume(num_messages=100, timeout=0.5) if not m.error()]
        ids = []
        for m in msgs:
            RECEIVED.labels(SERVICE).inc()
            try:
                ids.append(int(Envelope.from_bytes(m.value()).payload["booking_id"]))
            except Exception as e:
                log.error("unreadable ride.requested skipped", error=str(e))
        if ids:
            for r in with_db_retry(lambda c: [dispatch(c, b) for b in ids], log, SERVICE):
                announce(producer, r)

        now = time.time()
        if now - last_sweep >= SWEEP_INTERVAL:
            last_sweep = now
            expired, results = with_db_retry(lambda c: sweep(c, OFFER_TIMEOUT_S), log, SERVICE)
            for booking_id, driver_id in expired:
                ev = ride_event(booking_id, "TIMEOUT", SERVICE, driver_id=driver_id)
                publish(producer, RIDE_EVENTS, ev.kafka_key(), ev.to_bytes())
                RIDE_EV.labels(SERVICE, "TIMEOUT").inc()
            for r in results:
                announce(producer, r)
        if now - last_supply >= 10:
            last_supply = now
            with_db_retry(lambda c: publish_supply(c, rds), log, SERVICE)

        if producer.flush(30) != 0:
            log.error("events not confirmed by Kafka - restarting")
            raise SystemExit(1)
        if msgs:
            consumer.commit(asynchronous=False)
    consumer.close()


if __name__ == "__main__":
    run()
