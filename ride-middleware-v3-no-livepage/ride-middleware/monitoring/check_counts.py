"""Print pipeline totals from the database and compare with what we expect. Owner: Person 6.

    python -m monitoring.check_counts                                  # our dataset: 530 / 11 / 316 payments
    python -m monitoring.check_counts --inserted 530 --rejected 34     # after also sending bookings_bad.csv
    python -m monitoring.check_counts --any                            # just print (e.g. after a load test)

Checks: inserted, rejected, no duplicate keys in bookings, EVENTUAL CONSISTENCY (every COMPLETED
booking has exactly one payment), and the DRIVER RACE (no ride with 2 drivers, no driver on 2 rides).
Exit code 0 = PASS, 1 = FAIL.  Waits up to --wait seconds for the pipeline to catch up.
"""
import argparse
import sys
import time

import psycopg

from common.config import DATABASE_URL


def snapshot():
    with psycopg.connect(DATABASE_URL, prepare_threshold=None) as b:
        one = lambda q: b.execute(q).fetchone()[0]                          # noqa: E731
        inserted = one("SELECT count(*) FROM bookings")
        rejected = one("SELECT count(*) FROM rejected_records")
        by_code = b.execute("SELECT error_code, count(*) FROM rejected_records GROUP BY 1 ORDER BY 2 DESC").fetchall()
        dupes = one("SELECT count(*) FROM (SELECT idempotency_key FROM bookings GROUP BY 1 HAVING count(*) > 1) d")
        payments = one("SELECT count(*) FROM payments")
        mismatches = one("SELECT count(*) FROM bookings b WHERE b.booking_status = 'COMPLETED' "
                         "AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.booking_id = b.booking_id)")
        answers = dict(b.execute("SELECT response || '/' || coalesce(outcome, 'WAITING'), count(*) "
                                 "FROM driver_responses GROUP BY 1").fetchall())
        double = one("SELECT count(*) FROM (SELECT booking_id FROM driver_responses WHERE outcome = 'ASSIGNED' "
                     "GROUP BY 1 HAVING count(*) > 1) d")
        # live rides only (assigned by the dispatcher) - historical CSV rows may repeat a driver
        busy_twice = one("SELECT count(*) FROM (SELECT b.driver_id FROM bookings b JOIN driver_responses r "
                         "ON r.booking_id = b.booking_id AND r.driver_id = b.driver_id AND r.outcome = 'ASSIGNED' "
                         "WHERE b.booking_status IN ('DRIVER_ASSIGNED', 'STARTED') GROUP BY 1 HAVING count(*) > 1) d")
    return dict(inserted=inserted, rejected=rejected, by_code=by_code, dupes=dupes, payments=payments,
                mismatches=mismatches, answers=answers, double_winners=double, driver_on_two=busy_twice)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inserted", type=int, default=530)
    ap.add_argument("--rejected", type=int, default=11)
    ap.add_argument("--payments", type=int, default=None, help="default: = COMPLETED bookings")
    ap.add_argument("--any", action="store_true", help="don't compare totals, only check consistency")
    ap.add_argument("--wait", type=int, default=90)
    a = ap.parse_args()

    deadline = time.time() + a.wait
    while True:
        s = snapshot()
        totals_ok = a.any or (s["inserted"] == a.inserted and s["rejected"] == a.rejected)
        pay_ok = s["mismatches"] == 0 and (a.payments is None or s["payments"] == a.payments)
        ok = totals_ok and pay_ok and s["dupes"] == 0 and s["double_winners"] == 0 and s["driver_on_two"] == 0
        if ok or time.time() > deadline:
            break
        time.sleep(3)

    exp = "" if a.any else f"   expected {a.inserted}"
    print("=== Pipeline totals ===")
    print(f"inserted bookings : {s['inserted']:>7}{exp}")
    print(f"rejected records  : {s['rejected']:>7}{'' if a.any else f'   expected {a.rejected}'}")
    for code, n in s["by_code"]:
        print(f"    {code:<28}{n:>7}")
    print(f"payments          : {s['payments']:>7}")
    print(f"COMPLETED bookings without payment : {s['mismatches']} (must be 0 - eventual consistency)")
    print(f"duplicate keys in bookings         : {s['dupes']} (must be 0 - idempotency)")
    print(f"rides with 2+ assigned drivers     : {s['double_winners']} (must be 0 - driver race)")
    print(f"drivers on 2+ live rides           : {s['driver_on_two']} (must be 0 - driver lock)")
    print(f"driver answers                     : {s['answers']}")
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
