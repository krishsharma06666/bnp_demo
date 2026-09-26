"""Kafka v1 smoke test: send bookings.csv to booking.raw, read it back, verify.

Run on your laptop after `make up`:
    python -m scripts.kafka_smoke_test --file data/bookings.csv

Checks (all must PASS):
  1. required topics exist
  2. every row is delivered to Kafka (acks=all)
  3. every row is read back - nothing lost
  4. nothing is read twice
  5. same idempotency_key -> same partition, in file order (rows 539, 540, 541)

Each run is tagged with a run_id header, so old messages from earlier runs are ignored.
"""
import argparse
import csv
import sys
import time
import uuid
from collections import Counter, defaultdict

from common.events import Envelope, booking_raw_event
from common.kafka_client import check_topics_exist, make_consumer, make_producer, publish
from common.topics import BOOKING_RAW, V1_TOPICS


def load_rows(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def produce_all(rows: list[dict], run_id: str) -> tuple[int, int]:
    producer = make_producer()
    stats = {"ok": 0, "failed": 0}

    def on_delivery(err, msg):
        if err:
            stats["failed"] += 1
            print(f"  DELIVERY FAILED key={msg.key()}: {err}")
        else:
            stats["ok"] += 1

    headers = [("run_id", run_id.encode())]
    for row in rows:
        ev = booking_raw_event(row, source="smoke-test")
        publish(producer, BOOKING_RAW, ev.kafka_key(), ev.to_bytes(), headers=headers, on_delivery=on_delivery)
    remaining = producer.flush(60)
    if remaining:
        stats["failed"] += remaining
    return stats["ok"], stats["failed"]


def consume_run(run_id: str, expected: int, timeout_s: float) -> list[tuple[int, int, Envelope]]:
    consumer = make_consumer(group_id=f"smoke-{run_id}", topics=[BOOKING_RAW])
    got: list[tuple[int, int, Envelope]] = []
    deadline = time.time() + timeout_s
    try:
        while len(got) < expected and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"  consumer error: {msg.error()}")
                continue
            hdrs = dict(msg.headers() or [])
            if hdrs.get("run_id", b"").decode() == run_id:
                got.append((msg.partition(), msg.offset(), Envelope.from_bytes(msg.value())))
        if got:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
    return got


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="data/bookings.csv")
    ap.add_argument("--timeout", type=float, default=60.0)
    args = ap.parse_args()

    results: list[tuple[str, bool, str]] = []

    missing = check_topics_exist(V1_TOPICS)
    results.append(("1. topics exist", not missing, f"missing: {missing}" if missing else ", ".join(V1_TOPICS)))
    if missing:
        return report(results)

    rows = load_rows(args.file)
    run_id = uuid.uuid4().hex[:8]
    print(f"run_id={run_id}  rows in file={len(rows)}")

    t0 = time.time()
    ok, failed = produce_all(rows, run_id)
    results.append(("2. all rows delivered", ok == len(rows) and failed == 0,
                    f"sent {ok}/{len(rows)}, failed {failed}, {time.time() - t0:.2f}s"))

    got = consume_run(run_id, expected=len(rows), timeout_s=args.timeout)
    ids = [ev.event_id for _, _, ev in got]
    results.append(("3. nothing lost", len(set(ids)) == len(rows), f"read back {len(set(ids))}/{len(rows)}"))
    dupes = len(ids) - len(set(ids))
    results.append(("4. nothing read twice", dupes == 0, f"duplicate deliveries: {dupes}"))

    by_key = defaultdict(list)
    for part, off, ev in got:
        by_key[ev.idempotency_key].append((part, off, ev.booking_id))
    shared = {k: v for k, v in by_key.items() if len(v) > 1}
    order_ok = True
    detail = []
    for key, items in shared.items():
        parts = {p for p, _, _ in items}
        in_offset_order = [b for _, _, b in sorted(items, key=lambda x: x[1])]
        in_file_order = sorted(in_offset_order, key=int)
        same_part = len(parts) == 1
        ordered = in_offset_order == in_file_order
        order_ok &= same_part and ordered
        detail.append(f"{key}: bookings {in_offset_order} on partition {sorted(parts)}")
    results.append(("5. same key -> same partition, in order", order_ok, "; ".join(detail) or "no shared keys"))

    dist = Counter(p for p, _, _ in got)
    print("messages per partition:", dict(sorted(dist.items())))
    return report(results)


def report(results) -> int:
    print("\n=== Kafka v1 smoke test ===")
    for name, passed, detail in results:
        print(f"[{'PASS' if passed else 'FAIL'}] {name:42s} {detail}")
    all_ok = all(p for _, p, _ in results)
    print("RESULT:", "ALL PASSED" if all_ok else "FAILED")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
