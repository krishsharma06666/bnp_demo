"""Idempotency-Key handling shared by ALL API copies (Redis), with a local fallback. Owner: Person 3.

begin(key) -> ("NEW", None)        first time: go ahead (key is reserved for 30 s)
           -> ("DONE", response)    already answered: return the same answer (HTTP 200)
           -> ("BUSY", None)        same key is being processed right now by another copy (HTTP 409)
complete(key, response)  store the answer for 24 h
abort(key)               Kafka didn't confirm -> free the key so the client can retry
The database UNIQUE(idempotency_key) remains the final guarantee if Redis is down.
"""
import json

from services.api.limits import IdempotencyCache

PENDING = "__pending__"


class IdempotencyStore:
    def __init__(self, redis_client=None, log=None):
        self.r, self.local, self.log = redis_client, IdempotencyCache(), log

    def _redis_ok(self, fn):
        if self.r is None:
            return False, None
        try:
            return True, fn()
        except Exception as e:
            if self.log:
                self.log.warning("redis unavailable - using local idempotency cache", error=str(e)[:100])
            return False, None

    def begin(self, key: str):
        ok, got = self._redis_ok(lambda: self.r.set(f"idem:{key}", PENDING, nx=True, ex=30))
        if ok:
            if got:
                return "NEW", None
            ok, val = self._redis_ok(lambda: self.r.get(f"idem:{key}"))
            if val is None or val in (PENDING, PENDING.encode()):
                return "BUSY", None
            return "DONE", json.loads(val)
        cached = self.local.get(key)
        return ("DONE", cached) if cached else ("NEW", None)

    def complete(self, key: str, response: dict):
        self.local.put(key, response)
        self._redis_ok(lambda: self.r.set(f"idem:{key}", json.dumps(response), ex=86400))

    def abort(self, key: str):
        self._redis_ok(lambda: self.r.delete(f"idem:{key}"))
