#!/usr/bin/env bash
# Creates every topic the project needs. Safe to run many times (--if-not-exists).
# Owner: Person 1 (Kafka)
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:29092}"
PARTITIONS="${PARTITIONS:-6}"
RF="${REPLICATION_FACTOR:-3}"
MIN_ISR="${MIN_ISR:-2}"
KT=/opt/kafka/bin/kafka-topics.sh

# topic name                  key               written by -> read by
TOPICS=(
  booking.raw                 # idempotency_key  API / producer -> validator
  booking.validated           # idempotency_key  validator -> booking writer
  booking.rejected            # idempotency_key  validator / writers -> rejected writer
  ride.requested              # booking_id       booking writer / reconciler -> dispatcher
  driver.offers               # booking_id       dispatcher -> driver apps (simulator)
  driver.response             # booking_id       API: driver ACCEPT / REJECT -> driver-response consumer
  trip.updates                # booking_id       API: start / complete / cancel -> trip consumer
  ride.events                 # booking_id       every ride state change (audit, live tracking)
  payment.requested           # booking_id       trip consumer / reconciler -> payment consumer
  payment.completed           # booking_id       payment consumer -> downstream
  payment.requested.dlq       # booking_id       payment consumer after 3 failed retries -> dlq replay
  ops.events                  # service          autopilot actions (scale / heal)
)

echo "Creating topics on ${BOOTSTRAP} (partitions=${PARTITIONS}, rf=${RF}, min.insync=${MIN_ISR})"
for t in "${TOPICS[@]}"; do
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$t" --partitions "$PARTITIONS" --replication-factor "$RF" \
      --config min.insync.replicas="$MIN_ISR" \
      --config retention.ms=604800000
done

echo "Topics now on the cluster:"
$KT --bootstrap-server "$BOOTSTRAP" --list
echo "kafka-init done"
