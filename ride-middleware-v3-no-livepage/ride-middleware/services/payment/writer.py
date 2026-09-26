"""SQL for the team's `payments` table. No Kafka here -> tested against a real PostgreSQL. Owner: Person 5.

One payment per booking is guaranteed by the table itself:
    payment_id      = booking_id           (PRIMARY KEY)
    idempotency_key = 'PAY-<booking_id>'   (UNIQUE)
so a re-sent payment.requested (reconciler, DLQ replay, Kafka re-delivery) can never charge twice.

Outcomes: PAID (new row) · ALREADY_PAID (booking already has its payment) · REPLAY (same event again)
"""
import uuid
from dataclasses import dataclass

from common.events import Envelope


@dataclass
class PayItem:
    event: Envelope
    topic: str
    partition: int
    offset: int


MARK = """INSERT INTO processed_events (event_id, topic, kafka_partition, kafka_offset, outcome)
          VALUES (%s, %s, %s, %s, 'PENDING') ON CONFLICT (event_id) DO NOTHING RETURNING event_id"""
SET = "UPDATE processed_events SET outcome = %s WHERE event_id = %s"
PAY = """
INSERT INTO payments (payment_id, booking_id, user_id, amount, payment_method, payment_status,
                      idempotency_key, transaction_reference, created_at)
SELECT %(booking_id)s, %(booking_id)s, COALESCE(%(user_id)s, b.user_id), %(amount)s, %(payment_method)s,
       'COMPLETED', 'PAY-' || %(booking_id)s, %(txn)s, now()
  FROM bookings b WHERE b.booking_id = %(booking_id)s
ON CONFLICT DO NOTHING
RETURNING payment_id
"""
EXISTS = "SELECT 1 FROM payments WHERE payment_id = %s"


def write_payments(conn, items: list[PayItem]) -> list[tuple[PayItem, str]]:
    out = []
    for it in items:
        ev = it.event
        p = ev.payload
        with conn.transaction():                                  # savepoint per payment
            if conn.execute(MARK, (ev.event_id, it.topic, it.partition, it.offset)).fetchone() is None:
                out.append((it, "REPLAY"))
                continue
            params = {"booking_id": int(p["booking_id"]), "user_id": p.get("user_id"), "amount": p["amount"],
                      "payment_method": p.get("payment_method") or "UPI",
                      "txn": f"TXN-{p['booking_id']}-{uuid.uuid4().hex[:10]}"}
            paid = conn.execute(PAY, params).fetchone()
            if paid:
                outcome = "PAID"
            elif conn.execute(EXISTS, (params["booking_id"],)).fetchone():
                outcome = "ALREADY_PAID"
            else:
                outcome = "UNKNOWN_BOOKING"                        # booking not stored (yet)
            conn.execute(SET, (outcome, ev.event_id))
            out.append((it, outcome))
    return out
