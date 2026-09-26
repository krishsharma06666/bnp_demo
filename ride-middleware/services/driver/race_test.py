"""Live driver-race demo through the REAL system. Owner: Person 4.

    python -m services.driver.race_test                 # 1 fresh booking
    python -m services.driver.race_test --rounds 20     # 20 bookings, all offered drivers accept together

What happens per round:
  1. POST /v1/bookings (through HAProxy -> API -> Kafka -> validator -> consumer -> DB)
  2. wait until the dispatcher has OFFERED it to the nearest drivers (driver_responses rows)
  3. ALL offered drivers press ACCEPT at the same instant (POST /v1/drivers/{id}/respond)
  4. check the database: exactly ONE driver is assigned, every other answer is LOST
Stop the simulator first (`docker compose stop simulator`) so only this script answers offers.
"""
import argparse
import sys
import threading
import time
import uuid

import httpx
import psycopg

from common.config import API_URL, DATABASE_URL, DRIVER_API_KEY, RIDER_API_KEY


def wait_for(conn, sql, args, cond, timeout=40):
    end = time.time() + timeout
    while time.time() < end:
        rows = conn.execute(sql, args).fetchall()
        conn.rollback()
        if cond(rows):
            return rows
        time.sleep(0.3)
    return None


def fire_accepts(http, booking_id: int, drivers: list[int]):
    barrier = threading.Barrier(len(drivers))

    def accept(d):
        barrier.wait()                                        # everyone presses ACCEPT together
        http.post(f"/v1/drivers/{d}/respond", json={"booking_id": booking_id, "action": "ACCEPT"},
                  headers={"X-API-Key": DRIVER_API_KEY})

    ths = [threading.Thread(target=accept, args=(d,)) for d in drivers]
    [t.start() for t in ths]
    [t.join() for t in ths]


def one_round(http, conn) -> bool:
    r = http.post("/v1/bookings", headers={"X-API-Key": RIDER_API_KEY, "Idempotency-Key": f"RACE-{uuid.uuid4().hex[:12]}"},
                  json={"user_id": 1049, "pickup_location": "Indiranagar", "destination_location": "Jayanagar"})
    r.raise_for_status()
    booking_id = r.json()["booking_id"]

    offered = wait_for(conn, "SELECT driver_id FROM driver_responses WHERE booking_id=%s AND response='OFFERED'",
                       (booking_id,), lambda rows: len(rows) > 0)
    if not offered:
        print(f"booking {booking_id}: never offered (is the dispatcher running?) -> FAIL")
        return False
    drivers = [d for (d,) in offered]
    fire_accepts(http, booking_id, drivers)

    rows = wait_for(conn, "SELECT driver_id, response, outcome FROM driver_responses WHERE booking_id=%s",
                    (booking_id,), lambda rows: all(o is not None for _, _, o in rows) and
                    sum(1 for _, _, o in rows if o == "ASSIGNED") == 1)
    b = conn.execute("SELECT booking_status, driver_id FROM bookings WHERE booking_id=%s", (booking_id,)).fetchone()
    conn.rollback()
    if rows is None:
        print(f"booking {booking_id}: answers not settled in time -> FAIL")
        return False
    winners = [d for d, _, o in rows if o == "ASSIGNED"]
    ok = len(winners) == 1 and b[0] == "DRIVER_ASSIGNED" and b[1] == winners[0]
    print(f"booking {booking_id}: offered={drivers} winner={winners} "
          f"losers={[d for d, _, o in rows if o == 'LOST']} status={b[0]} -> {'OK' if ok else 'FAIL'}")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=1)
    args = ap.parse_args()
    with httpx.Client(base_url=API_URL, timeout=10) as http, psycopg.connect(DATABASE_URL, prepare_threshold=None) as conn:
        results = [one_round(http, conn) for _ in range(args.rounds)]
    print(f"\n{sum(results)}/{len(results)} races had exactly one winner")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
