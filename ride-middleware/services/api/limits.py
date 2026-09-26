"""Traffic guards for the Booking API. Owner: Person 3.

TokenBucket       - per-client rate limit (a rider/app spamming gets HTTP 429, others unaffected)
IdempotencyCache  - same Idempotency-Key again -> same answer, no second booking
BookingIdGenerator- unique 64-bit ids without asking the database (DB may be slow!)
"""
import threading
import time
from collections import OrderedDict


class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self.rate, self.burst = rate_per_sec, burst
        self._state: dict[str, tuple[float, float]] = {}      # client -> (tokens, last_time)
        self._lock = threading.Lock()

    def allow(self, client: str) -> bool:
        now = time.monotonic()
        with self._lock:
            tokens, last = self._state.get(client, (self.burst, now))
            tokens = min(self.burst, tokens + (now - last) * self.rate)
            if tokens < 1:
                self._state[client] = (tokens, now)
                return False
            self._state[client] = (tokens - 1, now)
            if len(self._state) > 100_000:                     # forget idle clients
                self._state.pop(next(iter(self._state)))
            return True


class IdempotencyCache:
    """LRU of Idempotency-Key -> response. (Multi-instance: swap for Redis SETNX.)
    The database UNIQUE(idempotency_key) is the final safety net anyway."""

    def __init__(self, max_items: int = 200_000):
        self._data: OrderedDict[str, dict] = OrderedDict()
        self._max = max_items
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
                return self._data[key]
            return None

    def put(self, key: str, response: dict):
        with self._lock:
            self._data[key] = response
            self._data.move_to_end(key)
            if len(self._data) > self._max:
                self._data.popitem(last=False)


class BookingIdGenerator:
    """Snowflake-style ids, unique across ALL API copies without asking the database:

        | 41 bits: ms since 2024-01-01 | 5 bits: node id | 7 bits: sequence |   = 53 bits

    53 bits keeps ids exact in JavaScript (browser / live page) and in a BIGINT.
    Each API copy holds its own node id (a Redis lease, see NodeLease), so two copies can never
    produce the same booking_id, even in the same millisecond. 128 ids per ms per copy.
    """
    EPOCH_MS = 1_704_067_200_000          # 2024-01-01T00:00:00Z
    NODE_BITS, SEQ_BITS = 5, 7
    MAX_NODE, MAX_SEQ = (1 << NODE_BITS) - 1, (1 << SEQ_BITS) - 1

    def __init__(self, node_id: int = 0):
        if not 0 <= node_id <= self.MAX_NODE:
            raise ValueError(f"node_id must be 0..{self.MAX_NODE}")
        self.node_id = node_id
        self._lock = threading.Lock()
        self._last_ms, self._seq = -1, 0

    def next(self) -> int:
        with self._lock:
            ms = max(int(time.time() * 1000), self._last_ms)     # never go back if the clock steps back
            if ms == self._last_ms:
                self._seq = (self._seq + 1) & self.MAX_SEQ
                if self._seq == 0:                             # 128 ids in 1 ms: wait for the next ms
                    while ms <= self._last_ms:
                        ms = int(time.time() * 1000)
            else:
                self._seq = 0
            self._last_ms = ms
            return ((ms - self.EPOCH_MS) << (self.NODE_BITS + self.SEQ_BITS)) | (self.node_id << self.SEQ_BITS) | self._seq


class NodeLease:
    """Claims a free node id 0..31 in Redis (SET NX with a 5-minute lease, renewed every minute).
    A copy that dies frees its id when the lease runs out. Redis down at start-up -> hash of the
    hostname (logged as a WARNING; the database UNIQUE constraints still protect the data)."""
    TTL = 300

    def __init__(self, redis_client, name: str, log=None):
        self.r, self.name, self.log = redis_client, name, log
        self.node_id = self._claim()

    def _claim(self) -> int:
        if self.r is not None:
            try:
                for n in range(BookingIdGenerator.MAX_NODE + 1):
                    if self.r.set(f"api:node:{n}", self.name, nx=True, ex=self.TTL):
                        return n
            except Exception:
                pass
        import zlib
        if self.log:
            self.log.warning("no node-id lease from redis - using hostname hash", replica=self.name)
        return zlib.crc32(self.name.encode()) % (BookingIdGenerator.MAX_NODE + 1)

    def renew(self) -> bool:
        if self.r is None:
            return False
        try:
            key = f"api:node:{self.node_id}"
            if self.r.get(key) in (self.name, self.name.encode()):
                return bool(self.r.expire(key, self.TTL))
            return bool(self.r.set(key, self.name, nx=True, ex=self.TTL))
        except Exception:
            return False

    def release(self):
        try:
            key = f"api:node:{self.node_id}"
            if self.r is not None and self.r.get(key) in (self.name, self.name.encode()):
                self.r.delete(key)
        except Exception:
            pass
