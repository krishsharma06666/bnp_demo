"""Validator service: booking.raw -> booking.validated (good) / booking.rejected (bad).
Owner: Person 4.

    python -m services.validator.validator

Flow per batch:  poll up to 200 msgs -> validate each -> produce results -> wait for
Kafka to confirm -> commit offsets.  If the service crashes before the commit, the
batch is re-read and re-sent with the SAME event_id, so the consumer's dedup table
ignores the repeat.  Nothing is lost, nothing is stored twice.
"""
import signal

from prometheus_client import start_http_server

from common.health import beat
from common.config import METRICS_PORT
from common.events import Envelope
from common.kafka_client import make_consumer, make_producer, publish
from common.logger import get_logger
from common.metrics import RECEIVED, REJECTED, VALIDATED, WARNINGS
from common.topics import BOOKING_RAW, BOOKING_REJECTED, BOOKING_VALIDATED
from common.config import REDIS_URL
from services.validator.guard import VelocityGuard
from services.validator.rules import error_code, validate

log = get_logger("validator")

SERVICE = "validator"


def process(raw_value: bytes, headers: dict | None = None, guard: VelocityGuard | None = None):
    """Pure function: one Kafka message in -> (topic, Envelope) out, or None to skip."""
    headers = headers or {}
    if "run_id" in headers:                      # Kafka smoke-test message, not real data
        return None
    try:
        ev = Envelope.from_bytes(raw_value)
    except Exception as e:                       # unreadable message: reject, never crash
        bad = Envelope(event_type="BookingRejected", booking_id="unknown", source=SERVICE,
                       payload={"errors": [f"POISON:{type(e).__name__}"], "error_code": "POISON",
                                "stage": SERVICE, "row": raw_value.decode("utf-8", "replace")[:2000]})
        return BOOKING_REJECTED, bad

    clean, errors, warnings = validate(ev.payload)
    # abuse guard: only for live API traffic (bulk CSV loads legitimately repeat users)
    if not errors and guard is not None and ev.source == "booking-api" and guard.too_fast(clean["user_id"]):
        errors = [f"USER_VELOCITY:more than {guard.limit} bookings per minute"]
    for w in warnings:
        WARNINGS.labels(SERVICE, w).inc()
        log.warning("accepted with warning", booking_id=ev.booking_id, code=w)

    if errors:
        code = error_code(errors[0])
        out = ev.model_copy(update={
            "event_type": "BookingRejected", "source": SERVICE,
            "payload": {"row": ev.payload, "errors": errors, "error_code": code, "stage": SERVICE},
        })
        return BOOKING_REJECTED, out

    out = ev.model_copy(update={"event_type": "BookingValidated", "source": SERVICE,
                                "payload": {**clean, "warnings": warnings}})
    return BOOKING_VALIDATED, out


def run():
    start_http_server(METRICS_PORT)
    guard = VelocityGuard.from_url(REDIS_URL)
    consumer = make_consumer("validator", [BOOKING_RAW])
    producer = make_producer()
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", reads=BOOKING_RAW, writes=[BOOKING_VALIDATED, BOOKING_REJECTED], metrics_port=METRICS_PORT)
    while running:
        beat()
        msgs = consumer.consume(num_messages=200, timeout=1.0)
        if not msgs:
            continue
        for msg in msgs:
            if msg.error():
                log.error("kafka error", error=str(msg.error()))
                continue
            RECEIVED.labels(SERVICE).inc()
            result = process(msg.value(), dict(msg.headers() or []), guard)
            if result is None:
                continue
            topic, ev = result
            publish(producer, topic, ev.kafka_key(), ev.to_bytes())
            if topic == BOOKING_VALIDATED:
                VALIDATED.labels(SERVICE).inc()
                log.info("validated", booking_id=ev.booking_id, key=ev.idempotency_key)
            else:
                REJECTED.labels(SERVICE, ev.payload["error_code"]).inc()
                log.warning("rejected", booking_id=ev.booking_id, error_code=ev.payload["error_code"],
                            errors=ev.payload["errors"])
        if producer.flush(30) == 0:              # all results confirmed by Kafka...
            consumer.commit(asynchronous=False)  # ...only then mark the input as done
        else:
            # exit without committing: docker restarts us and the batch is re-read
            log.error("results not confirmed by Kafka - restarting to re-read batch")
            raise SystemExit(1)
    producer.flush(10)
    consumer.close()
    log.info("stopped")


if __name__ == "__main__":
    run()
