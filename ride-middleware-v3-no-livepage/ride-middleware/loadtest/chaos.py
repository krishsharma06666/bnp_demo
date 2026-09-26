"""Failure injection for the demo. Owner: Person 6. Works on Windows PowerShell and Mac/Linux.

    python -m loadtest.chaos broker-down --seconds 60     # one Kafka broker dies        -> nothing lost
    python -m loadtest.chaos api-crash                     # one API copy crashes         -> LB routes around it
    python -m loadtest.chaos consumer-hang                 # a consumer HANGS (frozen)    -> Autopilot heals it
    python -m loadtest.chaos traffic-spike --seconds 120   # 25 bookings/s                -> Autopilot scales out
    python -m loadtest.chaos db-down --seconds 60          # PostgreSQL stops (local DB)  -> RETRYING, lag, drain
    python -m loadtest.chaos db-slow --seconds 60          # inserts blocked (CPU spike)  -> lag grows then drains
    python -m loadtest.chaos payment-db-down --seconds 60  # ONLY payments fail           -> 3 retries -> DLQ
    python -m loadtest.chaos consumer-kill                 # kill one consumer copy       -> rebalance, restart

Watch Grafana while it runs: lag rises, RETRYING / DLQ / Autopilot actions appear, then everything drains.
db-down / db-slow / payment-db-down need the LOCAL postgres container (not Supabase).
"""
import argparse
import os
import subprocess
import sys
import time


def sh(*args, env=None):
    print("  $", " ".join(args))
    return subprocess.run(list(args), capture_output=True, text=True, env=env)


def psql(sql: str, db: str = "postgres"):
    return sh("docker", "exec", "postgres", "psql", "-U", "app", "-d", db, "-c", sql)


def containers(service: str) -> list[str]:
    return sh("docker", "compose", "ps", "-q", service).stdout.split()


def countdown(seconds: int, what: str):
    for left in range(seconds, 0, -5):
        print(f"  {what} ... {left}s left (watch Grafana)")
        time.sleep(min(5, left))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("scenario", choices=["broker-down", "api-crash", "consumer-hang", "traffic-spike", "db-down",
                                         "db-slow", "payment-db-down", "consumer-kill"])
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--broker", default="kafka2")
    a = ap.parse_args()

    if a.scenario == "broker-down":
        print(f"Broker {a.broker} dies. Expect: new partition leaders, no failed sends, nothing lost (min.insync=2).")
        sh("docker", "stop", a.broker)
        countdown(a.seconds, f"{a.broker} down")
        sh("docker", "start", a.broker)
        print(f"{a.broker} back; it re-syncs its copies automatically.")

    elif a.scenario == "api-crash":
        ids = containers("api")
        if not ids:
            print("no api running"); return 1
        print("One API copy crashes. Expect: HAProxy marks it DOWN within ~4 s (http://localhost:8404/stats),")
        print("requests keep getting 202 from the other copies, Docker restarts the crashed copy.")
        sh("docker", "exec", ids[0], "python", "-c", "import os,signal; os.kill(1, signal.SIGTERM)")

    elif a.scenario == "consumer-hang":
        ids = containers("consumer")
        if not ids:
            print("no consumer running"); return 1
        print("A booking consumer FREEZES (process stopped, container still 'running' - Docker can't see it).")
        print("Expect in ~2 min: healthcheck -> unhealthy -> Autopilot HEAL -> restart (Grafana 'Heals').")
        print("Meanwhile Kafka gives its partitions to the other copies after the session timeout (45 s).")
        sh("docker", "kill", "--signal", "SIGSTOP", ids[0])

    elif a.scenario == "traffic-spike":
        print("Traffic spike through the simulator. Expect: lag rises -> Autopilot SCALE_UP -> lag drains ->")
        print("a minute of calm -> SCALE_DOWN. Watch 'Running copies per service'.")
        env = {**os.environ, "RIDER_RPS": "25"}
        print(sh("docker", "compose", "up", "-d", "--no-deps", "simulator", env=env).stderr[-300:])
        countdown(a.seconds, "spike")
        print(sh("docker", "compose", "up", "-d", "--no-deps", "simulator").stderr[-300:])
        print("Back to normal traffic (RIDER_RPS from .env).")

    elif a.scenario == "db-down":
        print("PostgreSQL goes DOWN. Expect: API still 202, writers RETRYING, lag grows, nothing lost.")
        sh("docker", "stop", "postgres")
        countdown(a.seconds, "DB down")
        sh("docker", "start", "postgres")
        print("DB back. Expect: lag drains to 0 (python -m monitoring.check_counts --any).")

    elif a.scenario == "db-slow":
        print("DB is 'slow': an exclusive lock on bookings makes every insert wait (like a CPU spike).")
        p = subprocess.Popen(["docker", "exec", "-i", "postgres", "psql", "-U", "app", "-d", "ridehail"],
                             stdin=subprocess.PIPE, text=True)
        p.stdin.write(f"BEGIN; LOCK TABLE bookings IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep({a.seconds}); COMMIT;\n")
        p.stdin.flush()
        countdown(a.seconds, "DB slow")
        p.stdin.close()
        p.wait(timeout=30)
        print("DB fast again. Expect: lag drains to 0.")

    elif a.scenario == "payment-db-down":
        print("PARTIAL failure: only the payment writer loses its database. Bookings, drivers, trips keep working.")
        print("Expect: payment-consumer RETRYING x3 -> 'moved to DLQ'; reconciler shows mismatches.")
        bad = {**os.environ, "DOCKER_PAYMENT_DATABASE_URL": "postgresql://app:app@postgres:5432/payments_offline"}
        print(sh("docker", "compose", "up", "-d", "--no-deps", "payment-consumer", env=bad).stderr[-300:])
        countdown(a.seconds, "payment DB down")
        env = {k: v for k, v in os.environ.items() if k != "DOCKER_PAYMENT_DATABASE_URL"}
        print(sh("docker", "compose", "up", "-d", "--no-deps", "payment-consumer", env=env).stderr[-300:])
        print("Payments DB back. Now:  docker compose run --rm dlq-replay   (or wait: the reconciler re-requests)")

    elif a.scenario == "consumer-kill":
        ids = containers("consumer")
        if not ids:
            print("no consumer running"); return 1
        sh("docker", "exec", ids[0], "python", "-c", "import os,signal; os.kill(1, signal.SIGTERM)")
        print("Stopped one consumer. Expect: Kafka rebalances its partitions; Docker restarts it; "
              "no duplicates (processed_events).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
