"""Builds monitoring/grafana/dashboards/pipeline.json.  Owner: Person 6.

    python -m monitoring.grafana.build_dashboard

Editing Python is easier to review than 2,000 lines of JSON; commit BOTH files.
Data sources: "prom" = Prometheus (live rates), "pg" = RideDB (exact counts from the tables).
"""
import json
from pathlib import Path

PROM = {"type": "prometheus", "uid": "prom"}
PG = {"type": "postgres", "uid": "pg"}
OUT = Path(__file__).parent / "dashboards" / "pipeline.json"

panels, _id, _y = [], [0], [0]


def nid():
    _id[0] += 1
    return _id[0]


def row(title):
    panels.append({"id": nid(), "type": "row", "title": title, "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": _y[0]}, "panels": []})
    _y[0] += 1


def place(panel, w, h, x):
    panel["gridPos"] = {"h": h, "w": w, "x": x, "y": _y[0]}
    panels.append(panel)


def prom(expr, legend="", instant=False):
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "refId": chr(65 + len(legend) % 26),
            "instant": instant, "range": not instant}


def sql(q, fmt="table"):
    return {"datasource": PG, "rawSql": q, "format": fmt, "rawQuery": True, "editorMode": "code", "refId": "A"}


def stat(title, target, color="blue", unit="short", thresholds=None, desc=""):
    steps = thresholds or [{"color": color, "value": None}]
    return {"id": nid(), "type": "stat", "title": title, "description": desc,
            "datasource": target["datasource"], "targets": [target],
            "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "thresholds"},
                                         "thresholds": {"mode": "absolute", "steps": steps}}, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "background", "graphMode": "none", "textMode": "value", "justifyMode": "center"}}


def ts(title, targets, unit="short", stack=False, desc=""):
    return {"id": nid(), "type": "timeseries", "title": title, "description": desc, "datasource": PROM,
            "targets": [dict(t, refId=chr(65 + i)) for i, t in enumerate(targets)],
            "fieldConfig": {"defaults": {"unit": unit, "custom": {
                "drawStyle": "line", "lineWidth": 2, "fillOpacity": 15, "showPoints": "never",
                "stacking": {"mode": "normal" if stack else "none", "group": "A"}}}, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}}


def bars(title, target, desc="", horizontal=True):
    return {"id": nid(), "type": "barchart", "title": title, "description": desc,
            "datasource": target["datasource"], "targets": [target],
            "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
            "options": {"orientation": "horizontal" if horizontal else "vertical", "showValue": "always",
                        "legend": {"showLegend": False}, "xTickLabelRotation": 0}}


def gauge_bars(title, target, unit="short", max_=None, desc=""):
    d = {"unit": unit, "min": 0, "color": {"mode": "continuous-GrYlRd"}}
    if max_ is not None:
        d["max"] = max_
    return {"id": nid(), "type": "bargauge", "title": title, "description": desc, "datasource": PROM,
            "targets": [target], "fieldConfig": {"defaults": d, "overrides": []},
            "options": {"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}}


def pie(title, target, desc=""):
    return {"id": nid(), "type": "piechart", "title": title, "description": desc,
            "datasource": target["datasource"], "targets": [target],
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True},
                        "pieType": "donut", "legend": {"displayMode": "table", "placement": "right",
                                                       "values": ["value", "percent"]}}}


def table(title, target, desc=""):
    return {"id": nid(), "type": "table", "title": title, "description": desc,
            "datasource": target["datasource"], "targets": [target],
            "options": {"showHeader": True, "cellHeight": "sm"}, "fieldConfig": {"defaults": {}, "overrides": []}}


GREEN, RED = "green", "red"
ok_if_zero = [{"color": GREEN, "value": None}, {"color": RED, "value": 1}]

# ------------------------------------------------------------------ 1. the ride funnel
row("1. Rides - from request to payment (exact counts from the database)")
funnel = [
    ("Requested (API 202)", prom('sum(api_requests_total{endpoint="/v1/bookings",status="202"})', instant=True), "blue"),
    ("Stored (inserted)", sql("SELECT count(*) AS inserted FROM bookings"), "green"),
    ("Rejected", sql("SELECT count(*) AS rejected FROM rejected_records"), "orange"),
    ("Driver assigned", sql("SELECT count(*) FROM bookings WHERE driver_id IS NOT NULL"), "purple"),
    ("Completed trips", sql("SELECT count(*) FROM bookings WHERE booking_status='COMPLETED'"), "green"),
    ("Paid", sql("SELECT count(*) FROM payments WHERE payment_status='COMPLETED'"), "green"),
]
for i, (t, q, c) in enumerate(funnel):
    place(stat(t, q, c), 4, 4, i * 4)
_y[0] += 4
place(bars("Bookings by status", sql("SELECT booking_status AS status, count(*) AS rides FROM bookings "
                                      "GROUP BY 1 ORDER BY 2 DESC")), 8, 8, 0)
place(ts("Ride events per second (ride.events topic)",
         [prom("sum by (event) (rate(ride_events_total[1m]))", "{{event}}")], "ops",
         desc="Every state change of every ride is also an event in Kafka"), 16, 8, 8)
_y[0] += 8

# ------------------------------------------------------------------ 2. drivers
row("2. Drivers - offers, accepts, rejects (stored in driver_responses AND ride.events)")
place(pie("Driver answers", sql("SELECT response, count(*) AS answers FROM driver_responses GROUP BY 1")), 6, 8, 0)
place(pie("Outcome of answers", sql("SELECT coalesce(outcome,'WAITING') AS outcome, count(*) AS answers "
                                    "FROM driver_responses GROUP BY 1"),
          desc="ASSIGNED = won the ride, LOST = another driver was faster, DECLINED = rejected, EXPIRED = timeout"),
      6, 8, 6)
place(ts("Driver answers per second", [prom("sum by (response, outcome) (rate(driver_responses_total[1m]))",
                                            "{{response}} / {{outcome}}")], "ops"), 8, 8, 12)
place(stat("Time to find a driver (p95)",
           prom("histogram_quantile(0.95, sum by (le) (rate(dispatch_seconds_bucket[5m])))", instant=True),
           unit="s", thresholds=[{"color": GREEN, "value": None}, {"color": "orange", "value": 15},
                                 {"color": RED, "value": 30}]), 4, 4, 20)
place(stat("Race losers (LOST)", sql("SELECT count(*) FROM driver_responses WHERE outcome='LOST'"), "purple",
           desc="Drivers who pressed ACCEPT after someone else won - proof that only one driver gets a ride"),
      4, 4, 20)
panels[-1]["gridPos"]["y"] += 4
_y[0] += 8
place(table("Latest driver answers", sql(
    "SELECT r.responded_at AS time, r.booking_id, r.driver_id, d.driver_name, r.dispatch_round AS round, "
    "r.response, r.outcome FROM driver_responses r JOIN drivers d USING (driver_id) "
    "WHERE r.responded_at IS NOT NULL ORDER BY r.responded_at DESC LIMIT 25")), 24, 8, 0)
_y[0] += 8

# ------------------------------------------------------------------ 3. traffic + HA
row("3. Traffic and high availability - load balancer, API copies, Kafka")
place(stat("API copies UP (HAProxy)", prom('sum(haproxy_backend_active_servers{proxy="api"})', instant=True),
           thresholds=[{"color": RED, "value": None}, {"color": "orange", "value": 2}, {"color": GREEN, "value": 3}]),
      4, 4, 0)
place(stat("Kafka brokers up", prom("kafka_brokers", instant=True),
           thresholds=[{"color": RED, "value": None}, {"color": "orange", "value": 2}, {"color": GREEN, "value": 3}]),
      4, 4, 4)
place(stat("Rate-limited (429)", prom('sum(api_requests_total{status="429"})', instant=True), "orange"), 4, 4, 8)
place(stat("API p95 latency", prom('histogram_quantile(0.95, sum by (le) (rate(api_request_seconds_bucket'
                                   '{endpoint="/v1/bookings"}[1m])))', instant=True), unit="s",
           thresholds=[{"color": GREEN, "value": None}, {"color": "orange", "value": 0.25}, {"color": RED, "value": 1}]),
      4, 4, 12)
place(stat("Consumer lag (total)", prom("sum(kafka_consumergroup_lag)", instant=True),
           thresholds=[{"color": GREEN, "value": None}, {"color": "orange", "value": 1000},
                       {"color": RED, "value": 10000}]), 4, 4, 16)
place(stat("Firing alerts", prom('count(ALERTS{alertstate="firing"}) or vector(0)', instant=True),
           thresholds=ok_if_zero), 4, 4, 20)
_y[0] += 4
place(ts("API requests / s per copy (load balancing)",
         [prom("sum by (instance) (rate(api_requests_total[1m]))", "{{instance}}")], "reqps", stack=True), 8, 8, 0)
place(ts("API requests / s by status", [prom("sum by (status) (rate(api_requests_total[1m]))", "{{status}}")],
         "reqps"), 8, 8, 8)
place(ts("Consumer lag per group (Kafka is the shock absorber)",
         [prom("sum by (consumergroup) (kafka_consumergroup_lag)", "{{consumergroup}}")]), 8, 8, 16)
_y[0] += 8

# ------------------------------------------------------------------ 4. surge
row("4. Surge pricing - demand vs available drivers per area")
place(gauge_bars("Surge multiplier per area", prom("max by (area) (surge_multiplier)", "{{area}}", instant=True),
                 max_=2.5), 12, 10, 0)
place(gauge_bars("Available drivers per area", prom("max by (area) (drivers_available)", "{{area}}",
                                                    instant=True)), 12, 10, 12)
_y[0] += 10

# ------------------------------------------------------------------ 5. autopilot
row("5. Autopilot - self-scaling and self-healing")
place(ts("Running copies per service", [prom("max by (service) (autopilot_replicas)", "{{service}}")]), 12, 8, 0)
place(ts("Autopilot actions", [prom("sum by (action, service) (increase(autopilot_actions_total[5m]))",
                                    "{{action}} {{service}}")], stack=True), 8, 8, 12)
place(stat("Heals (1h)", prom('sum(increase(autopilot_actions_total{action="HEAL"}[1h])) or vector(0)',
                              instant=True), "purple"), 4, 4, 20)
place(stat("Scale-ups (1h)", prom('sum(increase(autopilot_actions_total{action="SCALE_UP"}[1h])) or vector(0)',
                                  instant=True), "blue"), 4, 4, 20)
panels[-1]["gridPos"]["y"] += 4
_y[0] += 8

# ------------------------------------------------------------------ 6. data quality + resilience
row("6. Data quality and resilience - rejections, retries, DLQ, consistency")
place(stat("Duplicates blocked", sql("SELECT count(*) FROM rejected_records "
                                     "WHERE error_code='DUPLICATE_IDEMPOTENCY_KEY'"), "orange"), 4, 4, 0)
place(stat("Replays skipped", prom("sum(records_replayed_total) or vector(0)", instant=True), "blue",
           desc="Re-delivered Kafka messages recognised by processed_events and skipped"), 4, 4, 4)
place(stat("DB retries", prom("sum(db_retries_total) or vector(0)", instant=True), "orange"), 4, 4, 8)
place(stat("Messages in DLQ", prom("sum(records_dlq_total) or vector(0)", instant=True), thresholds=ok_if_zero),
      4, 4, 12)
place(stat("Completed rides without payment", prom("max(reconciliation_mismatches) or vector(0)", instant=True),
           thresholds=ok_if_zero), 4, 4, 16)
place(stat("Rides with 2 drivers (must be 0)", sql(
    "SELECT count(*) FROM (SELECT booking_id FROM driver_responses WHERE outcome='ASSIGNED' "
    "GROUP BY 1 HAVING count(*) > 1) x"), thresholds=ok_if_zero), 4, 4, 20)
_y[0] += 4
place(bars("Rejected by reason", sql("SELECT error_code AS reason, count(*) AS rows FROM rejected_records "
                                     "GROUP BY 1 ORDER BY 2 DESC")), 8, 8, 0)
place(ts("Rows per second", [prom("sum(rate(records_inserted_total[1m]))", "inserted"),
                             prom("sum(rate(records_rejected_total[1m]))", "rejected"),
                             prom('sum(rate(records_received_total{service="validator"}[1m]))', "read by validator")],
         "ops"), 8, 8, 8)
place(ts("Retries, DLQ and replays", [prom("sum by (service) (increase(records_retried_total[1m]))", "retry {{service}}"),
                                      prom("sum(increase(records_dlq_total[1m]))", "to DLQ"),
                                      prom("sum(increase(records_replayed_total[1m]))", "replay skipped")]),
      8, 8, 16)
_y[0] += 8
place(table("Latest rejections (messaging log)", sql(
    "SELECT rejected_at AS time, booking_id, idempotency_key, stage, error_code, error_message "
    "FROM rejected_records ORDER BY rejected_at DESC LIMIT 25")), 14, 8, 0)
place(table("Firing alerts", prom('ALERTS{alertstate="firing"}', instant=True) | {"format": "table"}), 10, 8, 14)
_y[0] += 8

dashboard = {
    "uid": "ride-middleware", "title": "Ride Middleware", "tags": ["kafka", "rides"], "timezone": "browser",
    "schemaVersion": 39, "version": 3, "refresh": "5s", "time": {"from": "now-30m", "to": "now"},
    "editable": True, "panels": panels, "templating": {"list": []}, "annotations": {"list": []},
}

if __name__ == "__main__":
    OUT.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {OUT} ({len(panels)} panels)")
