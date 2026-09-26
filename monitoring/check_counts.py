"""Print pipeline totals from the database and compare with what we expect.
Owner: Person 6.

    python -m monitoring.check_counts                          # expects our dataset: 530 / 11
    python -m monitoring.check_counts --inserted 530 --rejected 34   # after also sending bookings_bad.csv

Exit code 0 = numbers match, 1 = they don't (handy in CI and in the demo).
"""
import argparse
import sys
import time

import psycopg

from common.config import DATABASE_URL


def snapshot(conn):
    inserted = conn.execute("SELECT count(*) FROM bookings").fetchone()[0]
    rejected = conn.execute("SELECT count(*) FROM rejected_records").fetchone()[0]
    by_code = conn.execute("SELECT error_code, count(*) FROM rejected_records GROUP BY 1 ORDER BY 2 DESC").fetchall()
    dupes_in_table = conn.execute(
        "SELECT count(*) FROM (SELECT idempotency_key FROM bookings GROUP BY 1 HAVING count(*) > 1) d").fetchone()[0]
    return inserted, rejected, by_code, dupes_in_table


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inserted", type=int, default=530)
    ap.add_argument("--rejected", type=int, default=11)
    ap.add_argument("--wait", type=int, default=60, help="seconds to wait for the pipeline to catch up")
    args = ap.parse_args()

    deadline = time.time() + args.wait
    with psycopg.connect(DATABASE_URL) as conn:
        while True:
            inserted, rejected, by_code, dupes = snapshot(conn)
            conn.rollback()
            done = inserted == args.inserted and rejected == args.rejected
            if done or time.time() > deadline:
                break
            time.sleep(2)

    print("=== Pipeline totals (from PostgreSQL) ===")
    print(f"inserted : {inserted:>6}   expected {args.inserted}")
    print(f"rejected : {rejected:>6}   expected {args.rejected}")
    for code, n in by_code:
        print(f"   {code:<28}{n:>6}")
    print(f"duplicate keys inside bookings table: {dupes} (must be 0)")
    ok = done and dupes == 0
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
