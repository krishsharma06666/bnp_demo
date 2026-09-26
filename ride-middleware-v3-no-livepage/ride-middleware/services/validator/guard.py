"""Fraud / abuse guard: a rider can't create more than VELOCITY_LIMIT bookings per minute via the API.
Owner: Person 4.  Uses Redis (shared by all validator copies). If Redis is down the check is
skipped (logged) - the booking flow never stops because of the guard.
"""
import os
import time

VELOCITY_LIMIT = int(os.getenv("VELOCITY_LIMIT", "5"))


class VelocityGuard:
    def __init__(self, redis_client=None, limit: int = VELOCITY_LIMIT):
        self.r, self.limit = redis_client, limit

    @classmethod
    def from_url(cls, url: str):
        try:
            import redis
            client = redis.Redis.from_url(url, socket_timeout=0.3, socket_connect_timeout=0.3)
            return cls(client)
        except Exception:
            return cls(None)

    def count(self, user_id) -> int | None:
        """Bookings by this user in the current minute (including this one). None = unknown."""
        if self.r is None:
            return None
        key = f"velocity:{user_id}:{int(time.time() // 60)}"
        try:
            pipe = self.r.pipeline()
            pipe.incr(key)
            pipe.expire(key, 120)
            return int(pipe.execute()[0])
        except Exception:
            return None

    def too_fast(self, user_id) -> bool:
        n = self.count(user_id)
        return n is not None and n > self.limit
