"""Driver-response consumer: driver.response -> driver_responses + bookings + drivers. Owner: Person 5.

    python -m services.driver.driver_consumer

Every ACCEPT and every REJECT is stored in the database (driver_responses) AND published to
Kafka ride.events (ACCEPTED/REJECTED, then ASSIGNED or LOST), so both sides always have the full story.
Key = booking_id -> all answers for one booking arrive in order on one partition; the conditional
UPDATEs in responses.py make the result correct even if two copies ever overlap.
"""
import signal

from prometheus_client import start_http_server

from common.health import beat
from common.config import METRICS_PORT
from common.db import with_db_retry
from common.events import Envelope, ride_event
from common.kafka_client import make_consumer, make_producer, publish
from common.logger import get_logger
from common.metrics import DISPATCH_SECONDS, DRIVER_RESPONSES, RECEIVED, RIDE_EVENTS as RIDE_EV
from common.topics import DRIVER_RESPONSE, RIDE_EVENTS
from services.driver.responses import record_response

SERVICE = "driver-consumer"
log = get_logger(SERVICE)


def handle_batch(conn, events: list[Envelope]):
    out = []
    for ev in events:
        p = ev.payload
        if p.get("action") not in ("ACCEPT", "REJECT"):
            continue
        outcome, waited = record_response(conn, int(p["booking_id"]), int(p["driver_id"]), p["action"])
        out.append((ev, outcome, waited))
    return out


def run():
    start_http_server(METRICS_PORT)
    consumer = make_consumer("driver-responder", [DRIVER_RESPONSE], **{"max.poll.interval.ms": 900000})
    producer = make_producer()
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", reads=DRIVER_RESPONSE)
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
                log.error("unreadable driver response skipped", error=str(e))
        for ev, outcome, waited in with_db_retry(lambda c: handle_batch(c, events), log, SERVICE):
            p = ev.payload
            answer = "ACCEPTED" if p["action"] == "ACCEPT" else "REJECTED"
            DRIVER_RESPONSES.labels(SERVICE, answer, outcome).inc()
            fields = dict(booking_id=p["booking_id"], driver_id=p["driver_id"], outcome=outcome)
            for name in (answer, outcome if outcome in ("ASSIGNED", "LOST") else None):
                if name:
                    e = ride_event(p["booking_id"], name, SERVICE, driver_id=p["driver_id"], outcome=outcome)
                    publish(producer, RIDE_EVENTS, e.kafka_key(), e.to_bytes())
                    RIDE_EV.labels(SERVICE, name).inc()
            if outcome == "ASSIGNED":
                if waited is not None:
                    DISPATCH_SECONDS.labels(SERVICE).observe(waited)
                log.info("driver assigned (won the race)", **fields, seconds_to_assign=waited)
            elif outcome == "DECLINED":
                log.info("driver rejected the ride", **fields)
            else:
                log.warning("driver not assigned", **fields)
        if producer.flush(30) != 0:
            raise SystemExit(1)
        consumer.commit(asynchronous=False)
    consumer.close()


if __name__ == "__main__":
    run()
