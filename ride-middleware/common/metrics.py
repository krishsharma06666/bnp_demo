"""Every Prometheus metric name in ONE place, so the Grafana dashboard and the code agree.
Each service labels its numbers with service="<name>".  Owner: Person 6 (via Person 1's common/).
"""
from prometheus_client import Counter, Gauge, Histogram

RECEIVED = Counter("records_received_total", "Messages read from Kafka", ["service"])
VALIDATED = Counter("records_validated_total", "Rows that passed validation", ["service"])
INSERTED = Counter("records_inserted_total", "Bookings stored in the DB", ["service"])
REJECTED = Counter("records_rejected_total", "Rows rejected, by reason", ["service", "reason"])
WARNINGS = Counter("records_warning_total", "Rows accepted with a warning", ["service", "code"])
REPLAYED = Counter("records_replayed_total", "Re-delivered events skipped (already processed)", ["service"])
REJECTIONS_STORED = Counter("rejections_stored_total", "Rows written to rejected_records", ["service"])
DB_RETRIES = Counter("db_retries_total", "Batch retried because the DB failed", ["service"])
BATCH_SECONDS = Histogram("batch_write_seconds", "Time to write one batch to the DB", ["service"])

# --- v2 additions ---------------------------------------------------------------
API_REQUESTS = Counter("api_requests_total", "HTTP requests to the Booking API", ["endpoint", "status"])
API_LATENCY = Histogram("api_request_seconds", "Booking API latency", ["endpoint"],
                        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
DLQ_SENT = Counter("records_dlq_total", "Messages moved to a DLQ after MAX_RETRIES", ["service"])
RETRIES = Counter("records_retried_total", "Retry attempts before success or DLQ", ["service", "attempt"])
PAYMENTS = Counter("payments_total", "Payments processed", ["service", "outcome"])
RACE = Counter("driver_race_total", "Driver accept outcomes", ["service", "outcome"])
MISMATCHES = Gauge("reconciliation_mismatches", "COMPLETED bookings without a payment", ["service"])
REPUBLISHED = Counter("reconciliation_republished_total", "payment.requested re-sent by the reconciler", ["service"])

# --- v3: ride workflow, surge, autopilot ------------------------------------------
RIDE_EVENTS = Counter("ride_events_total", "Ride state changes", ["service", "event"])
DRIVER_RESPONSES = Counter("driver_responses_total", "Driver answers to offers", ["service", "response", "outcome"])
OFFERS = Counter("driver_offers_total", "Offers sent to drivers", ["service"])
DISPATCH_SECONDS = Histogram("dispatch_seconds", "Booking stored -> driver assigned", ["service"],
                             buckets=(0.5, 1, 2, 3, 5, 8, 13, 21, 34, 60))
SURGE = Gauge("surge_multiplier", "Current surge multiplier per pickup area", ["area"])
AVAILABLE_DRIVERS = Gauge("drivers_available", "Available drivers per area", ["area"])
AUTOPILOT_ACTIONS = Counter("autopilot_actions_total", "Autopilot actions", ["action", "service"])
AUTOPILOT_REPLICAS = Gauge("autopilot_replicas", "Running copies per scalable service", ["service"])
