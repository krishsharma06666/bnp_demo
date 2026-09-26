"""Send one message and WAIT until Kafka confirms it (acks=all), with a timeout.
Owner: Person 3.

The API replies 202 only after this returns True -> an accepted booking is never lost.
A background thread keeps calling producer.poll() so delivery callbacks fire.
"""
import threading
import time

from common.kafka_client import make_producer


class KafkaSender:
    def __init__(self):
        self._producer = make_producer()
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll_loop, daemon=True)
        self._poller.start()

    def _poll_loop(self):
        while not self._stop.is_set():
            self._producer.poll(0.05)

    def send(self, topic: str, key: str, value: bytes, timeout: float = 5.0) -> bool:
        done = threading.Event()
        result = {"ok": False}

        def on_delivery(err, _msg):
            result["ok"] = err is None
            done.set()

        while True:
            try:
                self._producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:                      # local queue full under heavy traffic:
                time.sleep(0.01)                     # the poller thread is draining it
        return done.wait(timeout) and result["ok"]

    def close(self):
        self._producer.flush(10)
        self._stop.set()
