"""Trip lifecycle after a driver is assigned. Owner: Person 5.  (Tested on real Postgres.)

    DRIVER_ASSIGNED --START (driver)--> STARTED --COMPLETE (driver)--> COMPLETED  -> payment.requested
    REQUESTED / DRIVER_ASSIGNED --CANCEL (rider)--> CANCELLED

Every transition is a conditional UPDATE on the expected current status, so a late, repeated
or out-of-order command can never move a ride backwards. COMPLETE and CANCEL free the driver
(availability_status -> 'AVAILABLE', version_no + 1; after COMPLETE the driver is at the destination).
"""
START = """UPDATE bookings SET booking_status = 'STARTED', version_no = version_no + 1, updated_at = now()
            WHERE booking_id = %(b)s AND driver_id = %(d)s AND booking_status = 'DRIVER_ASSIGNED'
           RETURNING booking_id"""
COMPLETE = """UPDATE bookings SET booking_status = 'COMPLETED', version_no = version_no + 1, updated_at = now()
               WHERE booking_id = %(b)s AND driver_id = %(d)s AND booking_status = 'STARTED'
              RETURNING user_id, fare, destination_location"""
CANCEL = """UPDATE bookings SET booking_status = 'CANCELLED', version_no = version_no + 1, updated_at = now()
             WHERE booking_id = %(b)s AND booking_status IN ('REQUESTED', 'DRIVER_ASSIGNED')
            RETURNING driver_id"""
RELEASE = """UPDATE drivers SET availability_status = 'AVAILABLE', version_no = version_no + 1,
                    current_location = COALESCE(%(loc)s, current_location), updated_at = now()
              WHERE driver_id = %(d)s AND availability_status = 'ON_TRIP'"""
CLOSE_OFFERS = """UPDATE driver_responses SET response = 'TIMEOUT', outcome = 'EXPIRED', responded_at = now()
                   WHERE booking_id = %s AND response = 'OFFERED'"""
STATUS = "SELECT booking_status, driver_id FROM bookings WHERE booking_id = %s"


def apply(conn, booking_id: int, action: str, driver_id: int | None = None) -> dict:
    """-> {"result": STARTED|COMPLETED|CANCELLED|REJECTED_TRANSITION|UNKNOWN_BOOKING, ...}. Caller commits."""
    p = {"b": booking_id, "d": driver_id}
    if action == "START":
        if conn.execute(START, p).fetchone():
            return {"result": "STARTED"}
    elif action == "COMPLETE":
        row = conn.execute(COMPLETE, p).fetchone()
        if row:
            user_id, fare, dest = row
            conn.execute(RELEASE, {"d": driver_id, "loc": dest})
            return {"result": "COMPLETED", "user_id": user_id, "fare": str(fare)}
    elif action == "CANCEL":
        row = conn.execute(CANCEL, p).fetchone()
        if row:
            if row[0] is not None:
                conn.execute(RELEASE, {"d": row[0], "loc": None})
            conn.execute(CLOSE_OFFERS, (booking_id,))
            return {"result": "CANCELLED", "driver_id": row[0]}
    else:
        raise ValueError(action)
    cur = conn.execute(STATUS, (booking_id,)).fetchone()
    if cur is None:
        return {"result": "UNKNOWN_BOOKING"}
    return {"result": "REJECTED_TRANSITION", "status": cur[0], "driver_id": cur[1]}
