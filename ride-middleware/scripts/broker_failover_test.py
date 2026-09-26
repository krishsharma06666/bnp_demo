"""Kafka v2 failover test: kill one of the 3 brokers while sending, prove nothing is lost.
Owner: Person 1.   Run on your laptop after `docker compose up -d --wait`:

    python -m scripts.broker_failover_test --kill kafka2

What happens:
  1. prints which broker leads each partition of booking.raw
  2. sends 3,000 messages over ~10 s (acks=all)
  3. after 3 s: `docker stop kafka2`  (a broker dies mid-traffic)
  4. prints the new leaders (Kafka moved leadership to the surviving brokers)
  5. reads everything back: all 3,000 must arrive, 0 failed sends
  6. `docker start kafka2` (it catches up automatically)

Messages carry a run_id header, so the Validator ignores them (they are test traffic).
"""
import argparse
import csv
import subprocess
import sys
import threading
import time
import uuid

from common.events import booking_raw_event
from common.kafka_client import make_admin, make_consumer, make_producer, publish
from common.topics import BOOKING_RAW


def leaders(topic: str) -> dict[int, int]:
    md = make_admin().list_topics(topic, timeout=10)
    return {p: part.leader for p, part in sorted(md.topics[topic].partitions.items())}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kill", default="kafka2", help="container to stop mid-test")
    ap.add_argument("--count", type=int, default=3000)
    ap.add_argument("--seconds", type=float, default=10.0)
    args = ap.parse_args()

    with open("data/bookings.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    run_id = uuid.uuid4().hex[:8]
    print(f"run_id={run_id}")
    print("leaders before:", leaders(BOOKING_RAW), "(partition -> broker id)")

    stats = {"ok": 0, "failed": 0}

    def on_delivery(err, _msg):
        stats["failed" if err else "ok"] += 1

    def send():
        p = make_producer()
        delay = args.seconds / args.count
        for i in range(args.count):
            ev = booking_raw_event(rows[i % len(rows)], source="failover-test")
            publish(p, BOOKING_RAW, ev.kafka_key(), ev.to_bytes(),
                    headers=[("run_id", run_id.encode())], on_delivery=on_delivery)
            time.sleep(delay)
        p.flush(120)

    t = threading.Thread(target=send)
    t.start()
    time.sleep(3)
    print(f"--> stopping {args.kill} while traffic is flowing")
    subprocess.run(["docker", "stop", args.kill], check=False, capture_output=True)
    t.join()
    print("leaders after :", leaders(BOOKING_RAW))

    c = make_consumer(f"failover-{run_id}", [BOOKING_RAW])
    seen, deadline = set(), time.time() + 90
    while len(seen) < args.count and time.time() < deadline:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        if dict(m.headers() or []).get("run_id", b"").decode() == run_id:
            seen.add((m.partition(), m.offset()))
    c.close()

    print(f"--> starting {args.kill} again")
    subprocess.run(["docker", "start", args.kill], check=False, capture_output=True)

    ok = stats["ok"] == args.count and stats["failed"] == 0 and len(seen) == args.count
    print(f"\nsent ok {stats['ok']}/{args.count}, failed {stats['failed']}, read back {len(seen)}")
    print("RESULT:", "PASS - broker died, no message lost" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
