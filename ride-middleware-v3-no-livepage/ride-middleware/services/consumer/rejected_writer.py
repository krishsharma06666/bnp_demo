"""Rejected writer: booking.rejected -> rejected_records table (feeds the dashboard).
Owner: Person 5.

    python -m services.consumer.rejected_writer

Every rejection (from the Validator or from the Consumer) goes through Kafka topic
booking.rejected and lands here exactly once (UNIQUE event_id).
"""
import signal

from prometheus_client import start_http_server

from common.health import beat
from common.config import METRICS_PORT
from common.events import Envelope
from common.kafka_client import make_consumer
from common.logger import get_logger
from common.metrics import REJECTIONS_STORED
from common.topics import BOOKING_REJECTED
from common.db import with_db_retry
from services.consumer.db_writer import write_rejections

SERVICE = "rejected-writer"
log = get_logger(SERVICE)


def run():
    start_http_server(METRICS_PORT)
    consumer = make_consumer("rejected-writer", [BOOKING_REJECTED], **{"max.poll.interval.ms": 900000})
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", reads=BOOKING_REJECTED, metrics_port=METRICS_PORT)
    while running:
        beat()
        msgs = consumer.consume(num_messages=200, timeout=1.0)
        if not msgs:
            continue
        events = []
        for m in msgs:
            if m.error():
                continue
            try:
                events.append(Envelope.from_bytes(m.value()))
            except Exception as e:
                log.error("unreadable rejected message skipped", error=str(e))
        if events:
            new = with_db_retry(lambda conn: write_rejections(conn, events), log, SERVICE)
            REJECTIONS_STORED.labels(SERVICE).inc(new)
            log.info("rejections stored", new=new, batch=len(events))
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            log.warning("offset commit failed, batch will be re-read safely", error=str(e))
    consumer.close()


if __name__ == "__main__":
    run()
