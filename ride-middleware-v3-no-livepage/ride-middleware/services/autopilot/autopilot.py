"""AUTOPILOT - self-driving operations for the ride platform. Owner: Person 1.

    python -m services.autopilot.autopilot          (runs as the `autopilot` container)

Every AUTOPILOT_INTERVAL_S (15 s):
  1. SENSE  - Prometheus: Kafka consumer lag per group, API requests/s     (what is the load?)
              Docker    : running copies per service, unhealthy containers
  2. HEAL   - restart every container whose healthcheck says "unhealthy"
              (Docker's restart policy only covers crashes - a hung process stays up; we fix that)
  3. DECIDE - policy.py: lag high -> add a copy, calm for a minute -> remove the extra copy
  4. ACT    - clone / remove containers through the Docker API
  5. TELL   - every action -> Kafka `ops.events` + Prometheus autopilot_* metrics (Grafana panel)
AUTOPILOT_DRY_RUN=true -> decide and report, but don't touch containers.
"""
import os
import signal
import time

from prometheus_client import start_http_server

from common.health import beat
from common.config import METRICS_PORT
from common.events import Envelope
from common.logger import get_logger
from common.metrics import AUTOPILOT_ACTIONS, AUTOPILOT_REPLICAS
from common.topics import OPS_EVENTS
from services.autopilot.policy import Policy, PolicyConfig, lag_by_service

SERVICE = "autopilot"
log = get_logger(SERVICE)

LAG_QUERY = "sum by (consumergroup) (kafka_consumergroup_lag)"
API_RPS_QUERY = 'sum(rate(api_requests_total{endpoint!~"/health|/ready"}[1m]))'


class Prometheus:
    def __init__(self, url: str, http=None):
        import httpx
        self.url, self.http = url.rstrip("/"), http or httpx.Client(timeout=5)

    def query(self, q: str) -> list[dict]:
        r = self.http.get(f"{self.url}/api/v1/query", params={"query": q})
        r.raise_for_status()
        return r.json()["data"]["result"]

    def load(self, api_copies: int) -> dict[str, float]:
        lag = {row["metric"].get("consumergroup", ""): float(row["value"][1]) for row in self.query(LAG_QUERY)}
        out = lag_by_service(lag)
        rps = self.query(API_RPS_QUERY)
        if rps and api_copies:
            out["api"] = float(rps[0]["value"][1]) / api_copies
        return out


class Autopilot:
    def __init__(self, docker_ops, prometheus, policy: Policy, emit, dry_run=False):
        self.docker, self.prom, self.policy, self.emit, self.dry = docker_ops, prometheus, policy, emit, dry_run

    def _report(self, action, service, **detail):
        AUTOPILOT_ACTIONS.labels(action, service).inc()
        self.emit(action, service, detail)

    def tick(self, now: float) -> list[tuple[str, str]]:
        done = []
        # HEAL first: a hung container is worse than a slow one
        for c in self.docker.unhealthy():
            service = c.labels.get("com.docker.compose.service", c.name)
            log.warning("container unhealthy - restarting", container=c.name, service=service)
            if not self.dry:
                self.docker.restart(c)
            self._report("HEAL", service, container=c.name, dry_run=self.dry)
            done.append(("HEAL", service))

        services = list(self.policy.cfg.limits)
        replicas = self.docker.replicas(services)
        for s, n in replicas.items():
            AUTOPILOT_REPLICAS.labels(s).set(n)
        try:
            load = self.prom.load(replicas.get("api", 0))
        except Exception as e:
            log.warning("prometheus unavailable - scaling paused, healing continues", error=str(e)[:120])
            return done

        for d in self.policy.decide(load, replicas, now):
            try:
                if d.action == "SCALE_UP":
                    name = self.docker.clone(d.service) if not self.dry else "(dry-run)"
                else:
                    name = self.docker.remove_one(d.service) if not self.dry else "(dry-run)"
                    if name is None:
                        continue                              # only compose copies left: leave them
                log.info("autopilot action", action=d.action, service=d.service, reason=d.reason,
                         container=name, copies_before=replicas.get(d.service))
                self._report(d.action, d.service, reason=d.reason, container=name, dry_run=self.dry,
                             copies_before=replicas.get(d.service))
                done.append((d.action, d.service))
            except Exception as e:
                log.error("autopilot action failed", action=d.action, service=d.service, error=str(e)[:200])
        return done


def run():
    from common.kafka_client import make_producer, publish
    from services.autopilot.docker_ops import DockerOps

    start_http_server(METRICS_PORT)
    interval = float(os.getenv("AUTOPILOT_INTERVAL_S", "15"))
    cfg = PolicyConfig(scale_up_lag=int(os.getenv("AUTOPILOT_SCALE_UP_LAG", "1000")),
                       scale_down_lag=int(os.getenv("AUTOPILOT_SCALE_DOWN_LAG", "50")),
                       cooldown_s=float(os.getenv("AUTOPILOT_COOLDOWN_S", "60")),
                       api_rps_per_copy=float(os.getenv("AUTOPILOT_API_RPS_PER_COPY", "300")))
    producer = make_producer()

    def emit(action, service, detail):
        ev = Envelope(event_type="OpsAction", booking_id="", source=SERVICE,
                      payload={"action": action, "service": service, **detail})
        publish(producer, OPS_EVENTS, service, ev.to_bytes())
        producer.flush(10)

    pilot = Autopilot(DockerOps(os.getenv("COMPOSE_PROJECT", "ride-middleware")),
                      Prometheus(os.getenv("PROMETHEUS_URL", "http://prometheus:9090")),
                      Policy(cfg), emit, dry_run=os.getenv("AUTOPILOT_DRY_RUN", "false").lower() == "true")
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("started", interval_s=interval, dry_run=pilot.dry, scale_up_lag=cfg.scale_up_lag,
             api_rps_per_copy=cfg.api_rps_per_copy)
    while running:
        beat()
        try:
            pilot.tick(time.time())
        except Exception as e:                                  # docker socket hiccup etc. - keep flying
            log.error("autopilot tick failed", error=str(e)[:200])
        for _ in range(int(interval * 10)):
            if not running:
                break
            time.sleep(0.1)


if __name__ == "__main__":
    run()
