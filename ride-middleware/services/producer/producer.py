"""Producer: reads a CSV file and publishes every row to Kafka topic booking.raw.
Owner: Person 3.

    python -m services.producer.producer --file data/bookings.csv
    python -m services.producer.producer --file data/bookings.csv --repeat 2      # duplicate test
    python -m services.producer.producer --file data/bookings.csv --rate 100      # 100 msgs/sec

It does NOT validate or change the data (that is the Validator's job): every value
is sent as the raw string from the file.
"""
import argparse
import csv
import sys
import time

from common.events import booking_raw_event
from common.kafka_client import check_topics_exist, make_producer, publish
from common.logger import get_logger
from common.topics import BOOKING_RAW

log = get_logger("producer")


def read_rows(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run(path: str, repeat: int = 1, rate: float = 0.0) -> dict:
    if check_topics_exist([BOOKING_RAW]):
        log.error("topic missing - run kafka-init first", topic=BOOKING_RAW)
        sys.exit(1)

    rows = read_rows(path)
    producer = make_producer()
    stats = {"sent": 0, "delivered": 0, "failed": 0}

    def on_delivery(err, msg):
        if err:
            stats["failed"] += 1
            log.error("delivery failed", key=msg.key().decode() if msg.key() else None, error=str(err))
        else:
            stats["delivered"] += 1

    log.info("start", file=path, rows=len(rows), repeat=repeat, rate=rate or "max")
    t0 = time.time()
    delay = 1.0 / rate if rate > 0 else 0.0
    for _ in range(repeat):
        for row in rows:
            ev = booking_raw_event(row, source="producer")
            publish(producer, BOOKING_RAW, ev.kafka_key(), ev.to_bytes(),
                    headers=[("source_file", path.encode())], on_delivery=on_delivery)
            stats["sent"] += 1
            if delay:
                time.sleep(delay)
    producer.flush(60)
    stats["seconds"] = round(time.time() - t0, 2)
    log.info("done", **stats)
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description="Publish CSV rows to booking.raw")
    ap.add_argument("--file", default="data/bookings.csv")
    ap.add_argument("--repeat", type=int, default=1, help="send the file N times (duplicate test)")
    ap.add_argument("--rate", type=float, default=0.0, help="messages per second, 0 = as fast as possible")
    args = ap.parse_args()
    stats = run(args.file, args.repeat, args.rate)
    return 0 if stats["failed"] == 0 and stats["delivered"] == stats["sent"] else 1


if __name__ == "__main__":
    sys.exit(main())
