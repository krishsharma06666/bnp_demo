"""Reconciler: makes bookings and payments EVENTUALLY match. Owner: Person 6 (was Person 4).

    python -m services.reconciler.reconciler

Every RECONCILE_INTERVAL seconds:
  - finds COMPLETED bookings that have no payment row  -> gauge reconciliation_mismatches
  - for those older than GRACE seconds (so normal in-flight payments aren't disturbed)
    it re-sends payment.requested.  The payment consumer is idempotent (UNIQUE booking_id),
    so re-sending is always safe.
This is the safety net for "Order and Inventory update eventually match, even if one service fails".
"""
import os
import signal
import time

from prometheus_client import start_http_server

from common.health import beat
from common.config import DATABASE_URL, METRICS_PORT
from common.db import with_db_retry
from common.events import payment_requested_event
from common.kafka_client import make_producer, publish
from common.logger import get_logger
from common.metrics import MISMATCHES, REPUBLISHED
from common.topics import PAYMENT_REQUESTED

SERVICE = "reconciler"
log = get_logger(SERVICE)
INTERVAL = float(os.getenv("RECONCILE_INTERVAL", "30"))
GRACE = float(os.getenv("RECONCILE_GRACE_SECONDS", "120"))


MISSING = """
SELECT b.booking_id, b.fare, b.user_id,
       b.inserted_at < now() - make_interval(secs => %s) AS old_enough     -- re-sending is always safe
  FROM bookings b
 WHERE b.booking_status = 'COMPLETED'
   AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.booking_id = b.booking_id)
"""


def find_missing(conn, grace_seconds: float = 0):
    """-> (all_missing_count, [(booking_id, fare, user_id) older than grace])"""
    rows = conn.execute(MISSING, (grace_seconds,)).fetchall()
    return len(rows), [(b, f, u) for b, f, u, old in rows if old]


def run():
    start_http_server(METRICS_PORT)
    producer = make_producer()
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", interval_s=INTERVAL, grace_s=GRACE)
    while running:
        beat()
        count, to_resend = with_db_retry(lambda c: find_missing(c, GRACE), log, SERVICE, url=DATABASE_URL)
        MISMATCHES.labels(SERVICE).set(count)
        for booking_id, fare, user_id in to_resend:
            ev = payment_requested_event(booking_id, fare, SERVICE, user_id=user_id)
            publish(producer, PAYMENT_REQUESTED, ev.kafka_key(), ev.to_bytes())
        producer.flush(30)
        if to_resend:
            REPUBLISHED.labels(SERVICE).inc(len(to_resend))
            log.warning("bookings without payment - payment.requested re-sent",
                        mismatches=count, resent=len(to_resend))
        else:
            log.info("check done", mismatches=count)
        for _ in range(int(INTERVAL * 10)):
            if not running:
                break
            time.sleep(0.1)


if __name__ == "__main__":
    run()
