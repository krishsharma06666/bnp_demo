"""Single source of truth for topic names. Import these; never type topic strings by hand."""

BOOKING_RAW = "booking.raw"                  # API / Producer          -> Validator
BOOKING_VALIDATED = "booking.validated"      # Validator               -> Booking writer
BOOKING_REJECTED = "booking.rejected"        # Validator / writers     -> Rejected writer
RIDE_REQUESTED = "ride.requested"            # Booking writer / Reconciler -> Dispatcher
DRIVER_OFFERS = "driver.offers"              # Dispatcher              -> driver apps (simulator)
DRIVER_RESPONSE = "driver.response"          # API (driver accept/reject) -> Driver-response consumer
TRIP_UPDATES = "trip.updates"                # API (start/complete/cancel) -> Trip consumer
RIDE_EVENTS = "ride.events"                  # every state change of every ride (audit / live view)
PAYMENT_REQUESTED = "payment.requested"      # Trip consumer / Booking writer / Reconciler -> Payment
PAYMENT_COMPLETED = "payment.completed"      # Payment consumer        -> downstream
PAYMENT_DLQ = "payment.requested.dlq"        # Payment consumer after MAX_RETRIES -> DLQ replay
OPS_EVENTS = "ops.events"                    # Autopilot actions (scale up/down, heal)

V1_TOPICS = [BOOKING_RAW, BOOKING_VALIDATED, BOOKING_REJECTED]
ALL_TOPICS = V1_TOPICS + [RIDE_REQUESTED, DRIVER_OFFERS, DRIVER_RESPONSE, TRIP_UPDATES, RIDE_EVENTS,
                          PAYMENT_REQUESTED, PAYMENT_COMPLETED, PAYMENT_DLQ, OPS_EVENTS]
