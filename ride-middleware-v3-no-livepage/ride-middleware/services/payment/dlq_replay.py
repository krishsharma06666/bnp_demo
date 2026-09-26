"""DLQ replay: send everything waiting in payment.requested.dlq back to payment.requested.
Owner: Person 5.   Run AFTER the payment DB is healthy again:

    python -m services.payment.dlq_replay                 (laptop)
    docker compose run --rm dlq-replay                     (docker)

Safe to run any number of times: payments are UNIQUE per booking, so nothing is charged twice.
Stops by itself when the DLQ has been empty for 5 seconds.
"""
import sys
import time

from common.kafka_client import make_consumer, make_producer, publish
from common.logger import get_logger
from common.topics import PAYMENT_DLQ, PAYMENT_REQUESTED

log = get_logger("dlq-replay")


def main() -> int:
    consumer = make_consumer("dlq-replayer", [PAYMENT_DLQ])
    producer = make_producer()
    replayed, idle_since = 0, time.time()
    while time.time() - idle_since < 5:
        msgs = [m for m in consumer.consume(num_messages=200, timeout=1.0) if not m.error()]
        if not msgs:
            continue
        idle_since = time.time()
        for m in msgs:
            publish(producer, PAYMENT_REQUESTED, m.key(), m.value(), headers=[("replayed_from", b"dlq")])
        if producer.flush(30) == 0:
            consumer.commit(asynchronous=False)          # only after they are safely back in the topic
            replayed += len(msgs)
            log.info("replayed batch", count=len(msgs))
        else:
            log.error("replay not confirmed by Kafka - stopping, run again")
            break
    consumer.close()
    log.info("done", replayed=replayed)
    print(f"replayed {replayed} message(s) from {PAYMENT_DLQ} to {PAYMENT_REQUESTED}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
