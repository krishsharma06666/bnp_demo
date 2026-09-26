"""What happens when a driver presses ACCEPT or REJECT. Owner: Person 5.  (Tested on real Postgres.)

Both answers are STORED (driver_responses) - and the consumer also publishes them to Kafka ride.events.

ACCEPT -> in ONE transaction, always locking booking first, then driver (fixed order = no deadlocks):
    1. UPDATE bookings SET driver_id, 'DRIVER_ASSIGNED', version_no+1
         WHERE booking_id = B AND booking_status = 'REQUESTED'          <- only ONE driver can win
    2. UPDATE drivers SET 'ON_TRIP', version_no+1
         WHERE driver_id = D AND availability_status = 'AVAILABLE'       <- a driver can't win TWO rides
    both succeed -> outcome ASSIGNED; either fails -> savepoint rolled back -> outcome LOST
REJECT -> outcome DECLINED (the dispatcher's sweep starts the next round when nobody is left).
Answer from a driver who was never OFFERED this ride -> stored with outcome NOT_OFFERED, never wins
(only the nearest drivers chosen by the dispatcher can get the ride).

Replays are safe: an answer already recorded is not recorded again; the winner stays the winner.
"""
from psycopg import errors as pgerr

UPSERT = """
INSERT INTO driver_responses (booking_id, driver_id, dispatch_round, response, responded_at)
VALUES (%(b)s, %(d)s, 0, %(resp)s, now())
ON CONFLICT (booking_id, driver_id) DO UPDATE
   SET response = EXCLUDED.response, responded_at = now()
 WHERE driver_responses.response = 'OFFERED'
RETURNING (xmax = 0) AS unsolicited          -- true = new row = this driver was never offered the ride
"""
TAKE_BOOKING = """
UPDATE bookings SET driver_id = %(d)s, booking_status = 'DRIVER_ASSIGNED', version_no = version_no + 1,
                    assigned_at = now(), updated_at = now()
 WHERE booking_id = %(b)s AND booking_status = 'REQUESTED'
RETURNING version_no, extract(epoch FROM now() - inserted_at)
"""
TAKE_DRIVER = """
UPDATE drivers SET availability_status = 'ON_TRIP', version_no = version_no + 1, updated_at = now()
 WHERE driver_id = %(d)s AND availability_status = 'AVAILABLE'
RETURNING version_no
"""
SET_OUTCOME = "UPDATE driver_responses SET outcome = %s WHERE booking_id = %s AND driver_id = %s"
BOOKING = "SELECT booking_status, driver_id FROM bookings WHERE booking_id = %s"


def record_response(conn, booking_id: int, driver_id: int, action: str) -> tuple[str, float | None]:
    """-> (outcome, seconds_to_assign). outcome: ASSIGNED, LOST, DECLINED, NOT_OFFERED, ALREADY_ANSWERED,
    UNKNOWN_BOOKING, UNKNOWN_DRIVER. Caller commits."""
    resp = {"ACCEPT": "ACCEPTED", "REJECT": "REJECTED"}[action]
    b = conn.execute(BOOKING, (booking_id,)).fetchone()
    if b is None:
        return "UNKNOWN_BOOKING", None
    try:
        with conn.transaction():
            fresh = conn.execute(UPSERT, {"b": booking_id, "d": driver_id, "resp": resp}).fetchone()
    except pgerr.ForeignKeyViolation:
        return "UNKNOWN_DRIVER", None
    if fresh is None:                                   # already answered or timed out (replay / late answer)
        return ("ASSIGNED" if b[1] == driver_id else "ALREADY_ANSWERED"), None   # the winner stays the winner
    if fresh[0]:                                        # no offer existed: keep the answer, never assign
        conn.execute(SET_OUTCOME, ("NOT_OFFERED", booking_id, driver_id))
        return "NOT_OFFERED", None

    if resp == "REJECTED":
        conn.execute(SET_OUTCOME, ("DECLINED", booking_id, driver_id))
        return "DECLINED", None

    outcome, waited = "LOST", None
    try:
        with conn.transaction():                        # savepoint: both updates or neither
            won = conn.execute(TAKE_BOOKING, {"b": booking_id, "d": driver_id}).fetchone()
            if won:
                if not conn.execute(TAKE_DRIVER, {"d": driver_id}).fetchone():
                    raise _DriverBusy()                 # booking was free but this driver is on another trip
                outcome, waited = "ASSIGNED", float(won[1])
    except _DriverBusy:
        pass                                            # savepoint rolled back: booking still REQUESTED
    conn.execute(SET_OUTCOME, (outcome, booking_id, driver_id))
    return outcome, waited


class _DriverBusy(Exception):
    pass
