"""Liveness heartbeat: "is the main loop still turning?"  Owner: Person 1.

Each service calls beat() on every loop turn (also while waiting for a database that is down).
Docker healthcheck:  python -m common.health      -> exit 1 if no beat for HEARTBEAT_MAX_AGE_S
A crashed process is restarted by Docker (restart: unless-stopped); a HUNG process stays "running"
but turns "unhealthy" here, and the Autopilot restarts it.
"""
import os
import sys
import time

HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", "/tmp/heartbeat")
MAX_AGE_S = float(os.getenv("HEARTBEAT_MAX_AGE_S", "60"))
_last = 0.0


def beat(now: float | None = None, path: str = HEARTBEAT_FILE):
    global _last
    now = time.time() if now is None else now
    if now - _last < 2:                      # at most one write every 2 s
        return
    _last = now
    try:
        with open(path, "w") as f:
            f.write(str(now))
    except OSError:
        pass                                 # read-only / no /tmp: healthcheck will say so


def age(path: str = HEARTBEAT_FILE, now: float | None = None) -> float:
    now = time.time() if now is None else now
    try:
        with open(path) as f:
            return now - float(f.read().strip() or 0)
    except (OSError, ValueError):
        return float("inf")


def main() -> int:
    a = age()
    if a > MAX_AGE_S:
        print(f"unhealthy: last heartbeat {a:.0f}s ago (limit {MAX_AGE_S:.0f}s)")
        return 1
    print(f"healthy: last heartbeat {a:.1f}s ago")
    return 0


if __name__ == "__main__":
    sys.exit(main())
