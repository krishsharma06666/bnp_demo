"""All SQL for the consumer. No Kafka here, so it can be tested against a real Postgres.
Owner: Person 5.

write_bookings(conn, items)  -> one transaction for the whole batch, one SAVEPOINT per row
write_rejections(conn, items)-> store rejected rows (dedup by event_id)

Outcomes per row:
  INSERTED   new booking stored
  DUPLICATE  idempotency_key (or booking_id) already stored -> not stored again
  REPLAY     this exact event was already processed earlier (Kafka re-delivery) -> skipped
  REJECTED   DB refused it (unknown user/driver/location, check constraint)
"""
import json
from dataclasses import dataclass

import psycopg
from psycopg import errors as pgerr

from common.events import Envelope


@dataclass
class Item:
    event: Envelope
    topic: str
    partition: int
    offset: int


@dataclass
class Result:
    item: Item
    outcome: str
    error_code: str | None = None
    message: str | None = None


INSERT_EVENT = """
INSERT INTO processed_events (event_id, topic, kafka_partition, kafka_offset, outcome)
VALUES (%s, %s, %s, %s, 'PENDING')
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id
"""
SET_OUTCOME = "UPDATE processed_events SET outcome = %s WHERE event_id = %s"
INSERT_BOOKING = """
INSERT INTO bookings (booking_id, user_id, driver_id, pickup_location, destination_location,
                      booking_status, fare, idempotency_key, created_at, updated_at, surge_multiplier)
VALUES (%(booking_id)s, %(user_id)s, %(driver_id)s, %(pickup_location)s, %(destination_location)s,
        %(booking_status)s, %(fare)s, %(idempotency_key)s, %(created_at)s, %(updated_at)s,
        COALESCE(%(surge_multiplier)s, 1.0))
ON CONFLICT DO NOTHING
RETURNING booking_id
"""
INSERT_REJECTED = """
INSERT INTO rejected_records (event_id, booking_id, idempotency_key, stage, error_code, error_message, raw_payload)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
RETURNING id
"""


def write_bookings(conn: psycopg.Connection, items: list[Item]) -> list[Result]:
    """Caller commits. Raises psycopg.OperationalError if the DB is unreachable (-> retry)."""
    results: list[Result] = []
    with conn.cursor() as cur:
        for it in items:
            ev = it.event
            try:
                with conn.transaction():                        # SAVEPOINT for this row
                    cur.execute(INSERT_EVENT, (ev.event_id, it.topic, it.partition, it.offset))
                    if cur.fetchone() is None:
                        results.append(Result(it, "REPLAY"))
                        continue
                    cur.execute(INSERT_BOOKING, {"surge_multiplier": None, **ev.payload})
                    if cur.fetchone() is None:
                        cur.execute(SET_OUTCOME, ("DUPLICATE", ev.event_id))
                        results.append(Result(it, "DUPLICATE", "DUPLICATE_IDEMPOTENCY_KEY",
                                              f"{ev.idempotency_key} already stored"))
                    else:
                        cur.execute(SET_OUTCOME, ("INSERTED", ev.event_id))
                        results.append(Result(it, "INSERTED"))
            except (pgerr.ForeignKeyViolation, pgerr.CheckViolation, pgerr.NotNullViolation,
                    pgerr.InvalidTextRepresentation, pgerr.NumericValueOutOfRange) as e:
                # savepoint rolled back -> record that we handled this event, then reject it
                code = "UNKNOWN_REFERENCE" if isinstance(e, pgerr.ForeignKeyViolation) else "DB_CONSTRAINT"
                with conn.transaction():
                    cur.execute(INSERT_EVENT, (ev.event_id, it.topic, it.partition, it.offset))
                    cur.execute(SET_OUTCOME, ("REJECTED", ev.event_id))
                results.append(Result(it, "REJECTED", code, str(e).splitlines()[0]))
    return results


def write_rejections(conn: psycopg.Connection, events: list[Envelope]) -> int:
    """Store rejected events. Returns how many were NEW (repeats are ignored)."""
    new = 0
    with conn.cursor() as cur:
        for ev in events:
            p = ev.payload
            row = p.get("row") if isinstance(p.get("row"), dict) else {}
            cur.execute(INSERT_REJECTED, (
                ev.event_id,
                ev.booking_id,
                ev.idempotency_key or row.get("idempotency_key"),
                p.get("stage", "unknown"),
                p.get("error_code", "UNKNOWN"),
                "; ".join(p.get("errors", [])) or None,
                json.dumps(p.get("row", p)),
            ))
            new += cur.fetchone() is not None
    return new
