"""Kafka producer/consumer with safe settings baked in. Every service uses these.

Producer: acks=all + idempotence  -> no lost and no duplicated writes to Kafka.
Consumer: manual commit           -> commit ONLY after your work (e.g. DB insert) succeeded.
"""
import logging

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient

from common.config import KAFKA_BOOTSTRAP

log = logging.getLogger(__name__)

PRODUCER_DEFAULTS = {
    "acks": "all",                    # leader waits for all in-sync replicas
    "enable.idempotence": True,       # Kafka drops duplicates caused by producer retries
    "max.in.flight.requests.per.connection": 5,
    "linger.ms": 5,                   # small batching window -> higher throughput
    "compression.type": "lz4",
    "delivery.timeout.ms": 120000,    # keep retrying for up to 2 min if a broker is down
}

CONSUMER_DEFAULTS = {
    "enable.auto.commit": False,      # we commit manually after processing
    "auto.offset.reset": "earliest",  # new group starts from the oldest message
    "isolation.level": "read_committed",
    "partition.assignment.strategy": "cooperative-sticky",
    "session.timeout.ms": 45000,
}


def make_producer(**overrides) -> Producer:
    conf = {"bootstrap.servers": KAFKA_BOOTSTRAP, **PRODUCER_DEFAULTS, **overrides}
    return Producer(conf)


def make_consumer(group_id: str, topics: list[str], **overrides) -> Consumer:
    conf = {"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": group_id, **CONSUMER_DEFAULTS, **overrides}
    consumer = Consumer(conf)
    consumer.subscribe(topics)
    return consumer


def make_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})


def publish(producer: Producer, topic: str, key: str, value: bytes, headers=None, on_delivery=None) -> None:
    """Send one message. If the local buffer is full, wait for deliveries and retry."""
    while True:
        try:
            producer.produce(topic, key=key, value=value, headers=headers, on_delivery=on_delivery)
            break
        except BufferError:
            producer.poll(0.5)
    producer.poll(0)


def check_topics_exist(required: list[str], timeout: float = 10.0) -> list[str]:
    """Return the required topics that are missing (empty list = all good)."""
    try:
        md = make_admin().list_topics(timeout=timeout)
    except KafkaException as e:
        raise SystemExit(f"Cannot reach Kafka at {KAFKA_BOOTSTRAP}: {e}") from e
    return [t for t in required if t not in md.topics]
