"""Autopilot decisions - pure logic, no Docker, no network (unit-tested). Owner: Person 1.

For every scalable service, every check:
  lag  > scale_up_lag     and copies < max  -> SCALE_UP   (one copy at a time, then cooldown)
  lag  < scale_down_lag   for `calm_checks` checks in a row and copies > min -> SCALE_DOWN
  API: requests/s per copy > api_rps_per_copy -> SCALE_UP ; < 1/4 of it (calm) -> SCALE_DOWN
Max copies of a Kafka consumer = number of partitions (6): a 7th copy would sit idle.
"""
from dataclasses import dataclass, field

# Kafka consumer group -> compose service that runs it
GROUP_TO_SERVICE = {
    "validator": "validator",
    "booking-writer": "consumer",
    "dispatcher": "dispatcher",
    "driver-responder": "driver-consumer",
    "trip-writer": "trip-consumer",
    "payment-writer": "payment-consumer",
}


@dataclass
class Limits:
    min: int = 1
    max: int = 6


@dataclass
class PolicyConfig:
    scale_up_lag: int = 1000
    scale_down_lag: int = 50
    calm_checks: int = 4               # e.g. 4 x 15 s = 1 minute of calm before removing a copy
    cooldown_s: float = 60.0           # wait after an action on the same service
    api_rps_per_copy: float = 300.0
    limits: dict[str, Limits] = field(default_factory=lambda: {
        **{s: Limits(1, 6) for s in GROUP_TO_SERVICE.values()},
        "api": Limits(3, 6),
    })


@dataclass
class Decision:
    service: str
    action: str                        # SCALE_UP | SCALE_DOWN
    reason: str


class Policy:
    def __init__(self, cfg: PolicyConfig | None = None):
        self.cfg = cfg or PolicyConfig()
        self.calm: dict[str, int] = {}
        self.last_action: dict[str, float] = {}

    def _pressure(self, service: str, load: float) -> tuple[bool, bool, str]:
        c = self.cfg
        if service == "api":
            return load > c.api_rps_per_copy, load < c.api_rps_per_copy / 4, f"{load:.0f} req/s per copy"
        return load > c.scale_up_lag, load < c.scale_down_lag, f"lag {load:.0f}"

    def decide(self, load: dict[str, float], replicas: dict[str, int], now: float) -> list[Decision]:
        """load: service -> consumer lag (or API req/s per copy). replicas: service -> running copies."""
        out = []
        for service, lim in self.cfg.limits.items():
            n = replicas.get(service, 0)
            if n == 0 or service not in load:
                continue                                    # not running / no data: never guess
            hot, cold, why = self._pressure(service, load[service])
            self.calm[service] = self.calm.get(service, 0) + 1 if cold else 0
            if now - self.last_action.get(service, -1e9) < self.cfg.cooldown_s:
                continue
            if (hot and n < lim.max) or n < lim.min:
                out.append(Decision(service, "SCALE_UP", why if hot else f"below minimum {lim.min}"))
            elif n > lim.max or (self.calm[service] >= self.cfg.calm_checks and n > lim.min):
                out.append(Decision(service, "SCALE_DOWN", f"{why} (calm {self.calm[service]} checks)"))
            else:
                continue
            self.last_action[service] = now
            self.calm[service] = 0
        return out


def lag_by_service(group_lag: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for group, lag in group_lag.items():
        svc = GROUP_TO_SERVICE.get(group)
        if svc:
            out[svc] = out.get(svc, 0.0) + float(lag)
    return out
