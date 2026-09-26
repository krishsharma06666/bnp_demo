"""Payment consumer: payment.requested -> payments table -> payment.completed + ride.events PAID.
Owner: Person 5.

    python -m services.payment.payment_consumer

Partial-failure handling (brief: "Payment DB goes down"):
    DB down -> RETRYING 1 (wait 1 s) -> RETRYING 2 (2 s) -> RETRYING 3 (4 s)
            -> still down: every message of the batch -> payment.requested.dlq  (ERROR log)
            -> offsets committed, the service keeps running and takes the next batch
Bookings are NOT affected: they were already stored by the booking consumer.
When the DB is back:  python -m services.payment.dlq_replay  -> payments go through.
The reconciler would also re-send them automatically after its grace period.
"""
import signal

from prometheus_client import start_http_server

from common.health import beat
from common.config import MAX_RETRIES, METRICS_PORT, PAYMENT_DATABASE_URL
from common.db import RetriesExhausted, with_db_retry
from common.events import Envelope, ride_event
from common.kafka_client import make_consumer, make_producer, publish
from common.logger import get_logger
from common.metrics import DLQ_SENT, PAYMENTS, RECEIVED
from common.topics import PAYMENT_COMPLETED, PAYMENT_DLQ, PAYMENT_REQUESTED, RIDE_EVENTS
from services.payment.writer import PayItem, write_payments

SERVICE = "payment-consumer"
log = get_logger(SERVICE)


def to_dlq(producer, msg, error: str):
    headers = [("error", error[:500].encode()), ("attempts", str(MAX_RETRIES + 1).encode()),
               ("original_topic", msg.topic().encode()), ("original_partition", str(msg.partition()).encode()),
               ("original_offset", str(msg.offset()).encode())]
    publish(producer, PAYMENT_DLQ, msg.key(), msg.value(), headers=headers)


def process_batch(msgs, producer, url: str = PAYMENT_DATABASE_URL) -> dict:
    """Write one batch of payment.requested messages. Never raises for DB outages."""
    summary = {"PAID": 0, "ALREADY_PAID": 0, "REPLAY": 0, "UNKNOWN_BOOKING": 0, "DLQ": 0}
    pairs = []
    for m in msgs:
        RECEIVED.labels(SERVICE).inc()
        try:
            pairs.append((m, PayItem(Envelope.from_bytes(m.value()), m.topic(), m.partition(), m.offset())))
        except Exception as e:                                      # unreadable -> DLQ at once
            to_dlq(producer, m, f"POISON:{e}")
            summary["DLQ"] += 1
    try:
        results = with_db_retry(lambda c: write_payments(c, [it for _, it in pairs]), log, SERVICE,
                                url=url, max_retries=MAX_RETRIES)
    except RetriesExhausted as e:
        for m, _ in pairs:
            to_dlq(producer, m, str(e))
        summary["DLQ"] += len(pairs)
        log.error("payment DB still down after retries - batch moved to DLQ",
                  messages=len(pairs), dlq=PAYMENT_DLQ, error=str(e))
        results = []
    for it, outcome in results:
        summary[outcome] += 1
        PAYMENTS.labels(SERVICE, outcome).inc()
        if outcome == "PAID":
            done = it.event.model_copy(update={"event_type": "PaymentCompleted", "source": SERVICE})
            publish(producer, PAYMENT_COMPLETED, done.kafka_key(), done.to_bytes())
            paid = ride_event(it.event.booking_id, "PAID", SERVICE, amount=it.event.payload["amount"])
            publish(producer, RIDE_EVENTS, paid.kafka_key(), paid.to_bytes())
            log.info("payment completed", booking_id=it.event.booking_id, amount=it.event.payload["amount"])
        else:
            log.info("payment skipped", booking_id=it.event.booking_id, outcome=outcome)
    DLQ_SENT.labels(SERVICE).inc(summary["DLQ"])
    return summary


def run():
    start_http_server(METRICS_PORT)
    consumer = make_consumer("payment-writer", [PAYMENT_REQUESTED], **{"max.poll.interval.ms": 900000})
    producer = make_producer()
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", reads=PAYMENT_REQUESTED, max_retries=MAX_RETRIES, dlq=PAYMENT_DLQ)
    while running:
        beat()
        msgs = [m for m in consumer.consume(num_messages=100, timeout=1.0) if not m.error()]
        if not msgs:
            continue
        process_batch(msgs, producer)
        if producer.flush(30) != 0:
            log.error("DLQ/completed events not confirmed by Kafka - restarting")
            raise SystemExit(1)
        consumer.commit(asynchronous=False)
    consumer.close()


if __name__ == "__main__":
    run()
