"""The message format every service sends and receives (the team contract).

Changing this file needs approval from Person 1 and every service owner.
"""
from datetime import datetime, timezone
from uuid import uuid4

from pydantic import BaseModel, Field


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Envelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))  # unique per message (used for dedup)
    event_type: str                                              # BookingRaw, BookingValidated, BookingRejected
    booking_id: str                                              # str: bad rows may have non-numeric ids
    idempotency_key: str | None = None                           # e.g. BOOK-0000539
    occurred_at: datetime = Field(default_factory=_now)
    source: str                                                  # producer, validator, consumer, ...
    attempt: int = 0                                             # used by retry logic (v3)
    schema_version: int = 1
    payload: dict                                                # the booking row itself

    def kafka_key(self) -> str:
        """Partition key. Same idempotency_key -> same partition -> strict order.

        This is why rows 539, 540, 541 (all BOOK-0000539) always arrive in file
        order, so booking 539 is always the one stored and 540/541 are duplicates.
        """
        return self.idempotency_key or self.booking_id

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> "Envelope":
        return cls.model_validate_json(raw)


def booking_raw_event(row: dict, source: str = "producer") -> Envelope:
    """Wrap one CSV row (all values as raw strings) into a BookingRaw event."""
    return Envelope(
        event_type="BookingRaw",
        booking_id=(row.get("booking_id") or "").strip(),
        idempotency_key=(row.get("idempotency_key") or "").strip() or None,
        source=source,
        payload=row,
    )


PAYMENT_METHODS = ("UPI", "CARD", "CASH", "WALLET")


def payment_requested_event(booking_id: int, amount, source: str, user_id: int | None = None,
                            method: str | None = None) -> Envelope:
    """Ask the payment service to charge a COMPLETED booking. Key = booking_id."""
    return Envelope(
        event_type="PaymentRequested",
        booking_id=str(booking_id),
        source=source,
        payload={"booking_id": int(booking_id), "amount": str(amount),
                 "user_id": int(user_id) if user_id is not None else None,
                 "payment_method": method or PAYMENT_METHODS[int(booking_id) % 4]},
    )


def ride_event(booking_id, event: str, source: str, **detail) -> Envelope:
    """One entry of the ride's life story on ride.events (key = booking_id).
    event: REQUESTED, OFFERED, ACCEPTED, REJECTED, TIMEOUT, ASSIGNED, LOST, STARTED, COMPLETED,
           CANCELLED, NO_DRIVER, PAID"""
    return Envelope(event_type=f"Ride{event.title().replace('_', '')}", booking_id=str(booking_id),
                    source=source, payload={"event": event, "booking_id": int(booking_id), **detail})
