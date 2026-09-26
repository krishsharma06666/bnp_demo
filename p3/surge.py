"""Surge pricing from live demand and supply. Owner: Person 3.

demand = bookings requested from an area in the last 5 minutes   (Redis counters, one per minute)
supply = AVAILABLE drivers in that area                          (published by the dispatcher every 10 s)
multiplier = 1.0 + 0.5 x (demand/supply - 1), between 1.0 and 2.5, rounded to 0.1
If Redis is unreachable or supply is unknown -> 1.0 (never block a booking because of pricing).
"""
import time

from common.geo import AREA_NAMES

WINDOW_MIN = 5
MAX_SURGE = 2.5


def multiplier(demand: int, supply: int | None) -> float:
    if supply is None:
        return 1.0
    ratio = demand / max(supply, 1)
    return round(min(MAX_SURGE, max(1.0, 1.0 + 0.5 * (ratio - 1))), 1)


class SurgeEngine:
    def __init__(self, redis_client=None):
        self.r = redis_client

    def _minute(self) -> int:
        return int(time.time() // 60)

    def record_demand(self, area: str):
        if self.r is None:
            return
        try:
            key = f"demand:{area}:{self._minute()}"
            pipe = self.r.pipeline()
            pipe.incr(key)
            pipe.expire(key, WINDOW_MIN * 60 + 60)
            pipe.execute()
        except Exception:
            pass

    def demand(self, area: str) -> int:
        if self.r is None:
            return 0
        now = self._minute()
        try:
            vals = self.r.mget([f"demand:{area}:{m}" for m in range(now - WINDOW_MIN + 1, now + 1)])
            return sum(int(v) for v in vals if v)
        except Exception:
            return 0

    def supply(self, area: str) -> int | None:
        if self.r is None:
            return None
        try:
            v = self.r.hget("supply:available", area)
            return int(v) if v is not None else (0 if self.r.exists("supply:available") else None)
        except Exception:
            return None

    def quote(self, area: str) -> float:
        return multiplier(self.demand(area), self.supply(area))

    def board(self) -> dict[str, dict]:
        return {a: {"demand_5min": self.demand(a), "available_drivers": self.supply(a), "multiplier": self.quote(a)}
                for a in AREA_NAMES}
