"""Smart dispatch rules, in SQL + Python. Owner: Person 4.  (No Kafka here -> tested on real Postgres.)

dispatch(conn, booking_id)
    Offer a REQUESTED booking to the DRIVERS_PER_OFFER nearest AVAILABLE drivers that
      - have not been offered this booking before, and
      - do not already hold another open offer (one offer per driver at a time; the driver row is
        locked with SKIP LOCKED, so two dispatcher copies can never offer the same driver at once).
    Every offer is stored in driver_responses (response = 'OFFERED').
    After MAX_DISPATCH_ROUNDS rounds without a driver -> booking_status = 'NO_DRIVER'.

sweep(conn, timeout_s)
    1. offers older than timeout_s with no answer -> response 'TIMEOUT', outcome 'EXPIRED' (stored)
    2. REQUESTED bookings whose current round is finished (everyone rejected / timed out),
       or that were never dispatched (dispatch_round = 0 for > 10 s: self-healing if an event was lost)
       -> next round.  FOR UPDATE SKIP LOCKED lets several dispatcher copies share the work safely.
"""
from dataclasses import dataclass, field

from common.config import DRIVERS_PER_OFFER, MAX_DISPATCH_ROUNDS
from common.geo import AREAS, km_between

LOCK_BOOKING = """SELECT booking_status, pickup_location, dispatch_round
                    FROM bookings WHERE booking_id = %s FOR UPDATE"""
CANDIDATES = """
SELECT d.driver_id, d.current_location
  FROM drivers d
 WHERE d.availability_status = 'AVAILABLE'
   AND NOT EXISTS (SELECT 1 FROM driver_responses r WHERE r.booking_id = %(b)s AND r.driver_id = d.driver_id)
   AND NOT EXISTS (SELECT 1 FROM driver_responses o WHERE o.driver_id = d.driver_id AND o.response = 'OFFERED')
"""
CLAIM_DRIVER = """SELECT 1 FROM drivers d
                   WHERE d.driver_id = %s AND d.availability_status = 'AVAILABLE'
                     AND NOT EXISTS (SELECT 1 FROM driver_responses o WHERE o.driver_id = d.driver_id
                                                                      AND o.response = 'OFFERED')
                   FOR UPDATE SKIP LOCKED"""
OFFER = """INSERT INTO driver_responses (booking_id, driver_id, dispatch_round, response)
           VALUES (%s, %s, %s, 'OFFERED')
           ON CONFLICT (booking_id, driver_id) DO UPDATE          -- driver had sent an unsolicited answer:
              SET response = 'OFFERED', outcome = NULL, dispatch_round = EXCLUDED.dispatch_round,
                  offered_at = now(), responded_at = NULL       -- now it is a real offer
            WHERE driver_responses.outcome = 'NOT_OFFERED'
           RETURNING driver_id"""
NEXT_ROUND = "UPDATE bookings SET dispatch_round = %s, updated_at = now() WHERE booking_id = %s"
NO_DRIVER = """UPDATE bookings SET booking_status = 'NO_DRIVER', version_no = version_no + 1, updated_at = now()
                WHERE booking_id = %s AND booking_status = 'REQUESTED'"""
OPEN_OFFERS = "SELECT count(*) FROM driver_responses WHERE booking_id = %s AND response = 'OFFERED'"

EXPIRE = """
UPDATE driver_responses SET response = 'TIMEOUT', outcome = 'EXPIRED', responded_at = now()
 WHERE response = 'OFFERED' AND offered_at < now() - make_interval(secs => %s)
RETURNING booking_id, driver_id
"""
DUE = """
SELECT b.booking_id FROM bookings b
 WHERE b.booking_status = 'REQUESTED'
   AND ((b.dispatch_round > 0
         AND NOT EXISTS (SELECT 1 FROM driver_responses r WHERE r.booking_id = b.booking_id AND r.response = 'OFFERED'))
     OR (b.dispatch_round = 0 AND b.inserted_at < now() - interval '10 seconds'))
 ORDER BY b.inserted_at
 LIMIT %s
 FOR UPDATE SKIP LOCKED
"""


@dataclass
class DispatchResult:
    booking_id: int
    status: str                     # OFFERED / NO_CANDIDATES / NO_DRIVER / SKIPPED / UNKNOWN_BOOKING / WAITING
    round: int = 0
    drivers: list[tuple[int, float]] = field(default_factory=list)   # (driver_id, km away)
    pickup: str | None = None


def dispatch(conn, booking_id: int, per_offer: int = DRIVERS_PER_OFFER,
             max_rounds: int = MAX_DISPATCH_ROUNDS) -> DispatchResult:
    """Caller commits. Safe to call twice for the same booking (second call sees open offers)."""
    row = conn.execute(LOCK_BOOKING, (booking_id,)).fetchone()
    if row is None:
        return DispatchResult(booking_id, "UNKNOWN_BOOKING")
    status, pickup, rnd = row
    if status != "REQUESTED":
        return DispatchResult(booking_id, "SKIPPED", rnd)
    if conn.execute(OPEN_OFFERS, (booking_id,)).fetchone()[0] > 0:
        return DispatchResult(booking_id, "WAITING", rnd)           # a round is still running
    if rnd >= max_rounds:
        conn.execute(NO_DRIVER, (booking_id,))
        return DispatchResult(booking_id, "NO_DRIVER", rnd, pickup=pickup)

    cands = conn.execute(CANDIDATES, {"b": booking_id}).fetchall()
    ranked = sorted(((d, km_between(pickup, loc) if pickup in AREAS and loc in AREAS else 99.0)
                     for d, loc in cands), key=lambda x: (x[1], x[0]))
    new_round = rnd + 1
    offered = []
    for driver_id, km in ranked:                    # nearest first
        if len(offered) == per_offer:
            break
        # lock the driver row; if another dispatcher copy is offering this driver right now, skip him
        if conn.execute(CLAIM_DRIVER, (driver_id,)).fetchone() is None:
            continue
        if conn.execute(OFFER, (booking_id, driver_id, new_round)).fetchone():
            offered.append((driver_id, round(km, 1)))
    conn.execute(NEXT_ROUND, (new_round, booking_id))
    return DispatchResult(booking_id, "OFFERED" if offered else "NO_CANDIDATES", new_round, offered, pickup)


def sweep(conn, timeout_s: float, batch: int = 100):
    """-> (expired offers [(booking_id, driver_id)], dispatch results for bookings that needed a new round)"""
    expired = conn.execute(EXPIRE, (timeout_s,)).fetchall()
    due = [r[0] for r in conn.execute(DUE, (batch,)).fetchall()]
    return expired, [dispatch(conn, b) for b in due]


def available_by_area(conn) -> dict[str, int]:
    """Supply per area - used for surge pricing and the Grafana 'drivers available' panel."""
    rows = conn.execute("""SELECT current_location, count(*) FROM drivers
                            WHERE availability_status = 'AVAILABLE' GROUP BY 1""").fetchall()
    return {area: n for area, n in rows if area}
