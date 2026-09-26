"""Booking consumer: booking.validated -> PostgreSQL (each booking stored exactly once).
Owner: Person 5.

    python -m services.consumer.consumer

Per batch: poll up to 200 -> write all in ONE DB transaction (savepoint per row)
-> commit DB -> send duplicates/DB-rejections to booking.rejected
-> new REQUESTED booking: ride.requested (dispatcher finds a driver) + ride.events
-> new COMPLETED booking (historical CSV): payment.requested (eventual consistency)
-> commit Kafka offsets.
The Kafka offset is committed only AFTER the DB commit, so a crash never loses a row.
Run several copies (docker compose up --scale consumer=3): Kafka splits the 6 partitions
between them; the DB constraints keep them from ever double-writing.
"""
import signal
import time

from confluent_kafka import KafkaException
from prometheus_client import start_http_server

from common.health import beat
from common.config import METRICS_PORT
from common.events import Envelope, payment_requested_event, ride_event
from common.kafka_client import make_consumer, make_producer, publish
from common.logger import get_logger
from common.metrics import BATCH_SECONDS, INSERTED, RECEIVED, REJECTED, REPLAYED, RIDE_EVENTS as RIDE_EV
from common.db import with_db_retry
from common.topics import BOOKING_REJECTED, BOOKING_VALIDATED, PAYMENT_REQUESTED, RIDE_EVENTS, RIDE_REQUESTED
from services.consumer.db_writer import Item, write_bookings

SERVICE = "consumer"
log = get_logger(SERVICE)


def rejection_event(ev: Envelope, code: str, message: str) -> Envelope:
    return ev.model_copy(update={
        "event_type": "BookingRejected", "source": SERVICE,
        "payload": {"row": ev.payload, "errors": [f"{code}:{message}"], "error_code": code, "stage": SERVICE},
    })


def run():
    start_http_server(METRICS_PORT)
    # large max.poll.interval so a long DB outage does not kick us out of the group
    consumer = make_consumer("booking-writer", [BOOKING_VALIDATED], **{"max.poll.interval.ms": 900000})
    producer = make_producer()
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", reads=BOOKING_VALIDATED, metrics_port=METRICS_PORT)
    while running:
        beat()
        msgs = consumer.consume(num_messages=200, timeout=1.0)
        if not msgs:
            continue
        items: list[Item] = []
        for m in msgs:
            if m.error():
                log.error("kafka error", error=str(m.error()))
                continue
            RECEIVED.labels(SERVICE).inc()
            try:
                items.append(Item(Envelope.from_bytes(m.value()), m.topic(), m.partition(), m.offset()))
            except Exception as e:
                log.error("unreadable message skipped", partition=m.partition(), offset=m.offset(), error=str(e))
        if not items:
            consumer.commit(asynchronous=False)
            continue

        t0 = time.time()
        results = with_db_retry(lambda conn: write_bookings(conn, items), log, SERVICE)
        BATCH_SECONDS.labels(SERVICE).observe(time.time() - t0)

        for r in results:
            ev = r.item.event
            if r.outcome == "INSERTED":
                INSERTED.labels(SERVICE).inc()
                log.info("inserted", booking_id=ev.booking_id, status=ev.payload.get("booking_status"))
                p = ev.payload
                if p.get("booking_status") == "REQUESTED":        # live ride -> find a driver
                    req = ride_event(p["booking_id"], "REQUESTED", SERVICE, pickup=p["pickup_location"],
                                     destination=p["destination_location"], fare=p["fare"])
                    publish(producer, RIDE_REQUESTED, req.kafka_key(), req.to_bytes())
                    publish(producer, RIDE_EVENTS, req.kafka_key(), req.to_bytes())
                    RIDE_EV.labels(SERVICE, "REQUESTED").inc()
                elif p.get("booking_status") == "COMPLETED":      # historical completed ride -> charge it
                    pay = payment_requested_event(p["booking_id"], p["fare"], SERVICE, user_id=p["user_id"])
                    publish(producer, PAYMENT_REQUESTED, pay.kafka_key(), pay.to_bytes())
            elif r.outcome == "REPLAY":
                REPLAYED.labels(SERVICE).inc()
                log.info("already processed, skipped", booking_id=ev.booking_id, event_id=ev.event_id)
            else:
                REJECTED.labels(SERVICE, r.error_code).inc()
                log.warning("rejected", booking_id=ev.booking_id, error_code=r.error_code, message=r.message)
                rej = rejection_event(ev, r.error_code, r.message)
                publish(producer, BOOKING_REJECTED, rej.kafka_key(), rej.to_bytes())
        if producer.flush(30) != 0:
            # payment/rejection events not confirmed: don't commit, restart and redo (REPLAY-safe;
            # the reconciler also re-sends any missing payment.requested)
            log.error("follow-up events not confirmed by Kafka - restarting")
            raise SystemExit(1)
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
            # e.g. group rebalanced during a long DB outage: messages will be re-read, and
            # processed_events makes the re-read harmless (outcome REPLAY)
            log.warning("offset commit failed, batch will be re-read safely", error=str(e))
    producer.flush(10)
    consumer.close()
    log.info("stopped")


if __name__ == "__main__":
    run()
