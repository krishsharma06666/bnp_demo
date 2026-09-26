"""Monitoring (Person 6): every Grafana SQL panel runs on the real schema; configs are consistent."""
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DASH = json.loads((ROOT / "monitoring/grafana/dashboards/pipeline.json").read_text())


def sql_panels():
    for p in DASH["panels"]:
        for t in p.get("targets", []):
            if "rawSql" in t:
                yield p["title"], t["rawSql"]


def test_dashboard_is_up_to_date_with_its_generator():
    from monitoring.grafana.build_dashboard import dashboard
    assert json.loads(json.dumps(dashboard)) == DASH, "run: python -m monitoring.grafana.build_dashboard"


def test_every_sql_panel_runs_on_the_schema(db):
    assert len(list(sql_panels())) >= 12
    for title, q in sql_panels():
        try:
            db.execute(q).fetchall()
        except Exception as e:                                   # pragma: no cover - message for humans
            raise AssertionError(f"Grafana panel '{title}' broken: {e}") from e
        db.rollback()


def test_every_metric_in_the_dashboard_exists_in_code():
    import re

    import common.metrics as m
    from prometheus_client import REGISTRY
    ours = {n for c in vars(m).values() if hasattr(c, "_name") for n in (c._name, c._name + "_total",
                                                                          c._name + "_bucket")}
    external = ("kafka_", "haproxy_", "ALERTS", "up")
    exprs = [t["expr"] for p in DASH["panels"] for t in p.get("targets", []) if "expr" in t]
    for e in exprs:
        for name in re.findall(r"\b([a-zA-Z_]+(?:_total|_bucket|_seconds|_multiplier|_available|_replicas|"
                               r"_mismatches))\b", e):
            if not name.startswith(external):
                assert name in ours, f"dashboard uses unknown metric {name}"
    assert REGISTRY is not None


def test_prometheus_scrapes_every_service_with_metrics():
    prom = yaml.safe_load((ROOT / "monitoring/prometheus/prometheus.yml").read_text())
    names = prom["scrape_configs"][0]["dns_sd_configs"][0]["names"]
    for svc in ["api", "validator", "consumer", "dispatcher", "driver-consumer", "trip-consumer",
                "payment-consumer", "rejected-writer", "reconciler", "autopilot"]:
        assert svc in names
    rules = yaml.safe_load((ROOT / "monitoring/prometheus/alerts.yml").read_text())
    assert len(rules["groups"][0]["rules"]) >= 6


def test_autopilot_knows_every_consumer_group():
    import re
    from services.autopilot.policy import GROUP_TO_SERVICE
    groups = set()
    for f in (ROOT / "services").rglob("*.py"):
        groups |= set(re.findall(r'make_consumer\("([a-z-]+)"', f.read_text()))
    not_scaled = {"rejected-writer", "dlq-replayer", "driver-simulator"}     # tiny / one-off / test traffic
    assert groups - not_scaled == set(GROUP_TO_SERVICE)
