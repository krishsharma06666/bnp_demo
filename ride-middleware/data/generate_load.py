"""Generate a big CSV for traffic tests (default 100,000 rows). Owner: Person 2.

    python data/generate_load.py                 # -> data/generated/load_100000.csv
    python data/generate_load.py --rows 20000

Same shape and status mix as bookings.csv, plus on purpose:
  ~3% duplicate idempotency keys (must be rejected as DUPLICATE)
  ~2% bad rows (bad fare / unknown status / REQUESTED with a driver)
Deterministic (seed 42): same file every time, so expected numbers are known.
"""
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

HEADER = ["booking_id", "user_id", "driver_id", "pickup_location", "destination_location",
          "booking_status", "fare", "idempotency_key", "created_at", "updated_at"]
LOCS = ["Airport", "BTM Layout", "Banashankari", "Bellandur", "Electronic City", "HSR Layout", "Hebbal",
        "Indiranagar", "JP Nagar", "Jayanagar", "KR Puram", "Koramangala", "MG Road", "Majestic",
        "Marathahalli", "Rajajinagar", "Whitefield", "Yelahanka"]
# status mix of our 541-row file
STATUS_W = {"COMPLETED": 316, "DRIVER_ASSIGNED": 69, "CONFIRMED": 49, "CANCELLED": 40,
            "REQUESTED": 27, "STARTED": 23, "FAILED": 17}
NO_DRIVER_OK = {"REQUESTED", "FAILED", "CANCELLED"}
START_ID = 1_000_000          # never clashes with bookings.csv (1..541)


def fmt(ts: datetime) -> str:
    return f"{ts:%d-%m-%Y} {ts.hour}.{ts:%M}"


def make_rows(n: int, seed: int = 42):
    rnd = random.Random(seed)
    statuses, weights = zip(*STATUS_W.items())
    base = datetime(2026, 1, 1)
    rows, expected = [], {"valid": 0, "duplicate": 0, "bad": 0}
    for i in range(n):
        bid = START_ID + i
        status = rnd.choices(statuses, weights)[0]
        driver = "" if status in ("REQUESTED",) or (status in NO_DRIVER_OK and rnd.random() < 0.8) \
            else str(rnd.randint(1, 500))
        p, d = rnd.sample(LOCS, 2)
        created = base + timedelta(minutes=rnd.randint(0, 260 * 24 * 60))
        row = {"booking_id": str(bid), "user_id": str(rnd.randint(1, 1200)), "driver_id": driver,
               "pickup_location": p, "destination_location": d, "booking_status": status,
               "fare": f"{rnd.uniform(80, 1500):.2f}", "idempotency_key": f"LOAD-{bid}",
               "created_at": fmt(created), "updated_at": fmt(created + timedelta(minutes=rnd.randint(1, 180)))}
        r = rnd.random()
        if r < 0.03 and rows:                                   # duplicate of an earlier key
            row["idempotency_key"] = rnd.choice(rows)["idempotency_key"]
            expected["duplicate"] += 1
        elif r < 0.05:                                          # bad row
            kind = rnd.randint(0, 2)
            if kind == 0:
                row["fare"] = "abc"
            elif kind == 1:
                row["booking_status"] = "DONE"
            else:
                row["booking_status"], row["driver_id"] = "REQUESTED", "7"
            expected["bad"] += 1
        else:
            expected["valid"] += 1
        rows.append(row)
    return rows, expected


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100_000)
    args = ap.parse_args()
    rows, exp = make_rows(args.rows)
    out = Path(__file__).parent / "generated" / f"load_{args.rows}.csv"
    out.parent.mkdir(exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} rows to {out}")
    print(f"planted: ~{exp['duplicate']} duplicate keys, ~{exp['bad']} bad rows "
          f"(exact numbers: run the pipeline and check Grafana / check_counts)")


if __name__ == "__main__":
    main()
