"""Trip consumer: trip.updates (START / COMPLETE / CANCEL) -> bookings + drivers. Owner: Person 5.

    python -m services.trip.trip_consumer

COMPLETE -> booking COMPLETED, driver AVAILABLE again at the destination, payment.requested sent.
Every change is also published to ride.events.
"""
import signal

from prometheus_client import start_http_server

from common.health import beat
from common.config import METRICS_PORT
from common.db import with_db_retry
from common.events import Envelope, payment_requested_event, ride_event
from common.kafka_client import make_consumer, make_producer, publish
from common.logger import get_logger
from common.metrics import RECEIVED, RIDE_EVENTS as RIDE_EV
from common.topics import PAYMENT_REQUESTED, RIDE_EVENTS, TRIP_UPDATES
from services.trip.trip import apply

SERVICE = "trip-consumer"
log = get_logger(SERVICE)


def handle_batch(conn, events):
    out = []
    for ev in events:
        p = ev.payload
        out.append((ev, apply(conn, int(p["booking_id"]), p["action"], p.get("driver_id"))))
    return out


def run():
    start_http_server(METRICS_PORT)
    consumer = make_consumer("trip-writer", [TRIP_UPDATES], **{"max.poll.interval.ms": 900000})
    producer = make_producer()
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", reads=TRIP_UPDATES)
    while running:
        beat()
        msgs = [m for m in consumer.consume(num_messages=100, timeout=1.0) if not m.error()]
        if not msgs:
            continue
        events = []
        for m in msgs:
            RECEIVED.labels(SERVICE).inc()
            try:
                events.append(Envelope.from_bytes(m.value()))
            except Exception as e:
                log.error("unreadable trip update skipped", error=str(e))
        for ev, res in with_db_retry(lambda c: handle_batch(c, events), log, SERVICE):
            b, result = ev.payload["booking_id"], res["result"]
            if result in ("STARTED", "COMPLETED", "CANCELLED"):
                e = ride_event(b, result, SERVICE, driver_id=ev.payload.get("driver_id"))
                publish(producer, RIDE_EVENTS, e.kafka_key(), e.to_bytes())
                RIDE_EV.labels(SERVICE, result).inc()
                log.info(f"trip {result.lower()}", booking_id=b)
                if result == "COMPLETED":
                    pay = payment_requested_event(b, res["fare"], SERVICE, user_id=res["user_id"])
                    publish(producer, PAYMENT_REQUESTED, pay.kafka_key(), pay.to_bytes())
            else:
                log.warning("trip update ignored", booking_id=b, action=ev.payload["action"], **res)
        if producer.flush(30) != 0:
            raise SystemExit(1)
        consumer.commit(asynchronous=False)
    consumer.close()


if __name__ == "__main__":
    run()
