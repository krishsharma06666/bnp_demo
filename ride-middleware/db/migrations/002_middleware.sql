-- 002: what the middleware needs on top of the team schema (Person 2). ADDITIVE ONLY:
-- new tables, new nullable/defaulted columns, and CHECK constraints added NOT VALID
-- (they protect every new row without failing on rows that already exist).

-- Service areas with coordinates: validation, fares, surge, "nearest driver" dispatch
CREATE TABLE IF NOT EXISTS locations (
    name VARCHAR(255) PRIMARY KEY,
    lat  NUMERIC(9, 6) NOT NULL,
    lng  NUMERIC(9, 6) NOT NULL
);

-- Drivers: where they are now (dispatch picks the nearest AVAILABLE ones)
ALTER TABLE drivers ADD COLUMN IF NOT EXISTS current_location VARCHAR(255);
ALTER TABLE drivers ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
CREATE INDEX IF NOT EXISTS idx_drivers_available ON drivers (availability_status, current_location);

-- Bookings: concurrency + dispatch bookkeeping
ALTER TABLE bookings ADD COLUMN IF NOT EXISTS version_no       INT NOT NULL DEFAULT 1;
ALTER TABLE bookings ADD COLUMN IF NOT EXISTS surge_multiplier NUMERIC(3, 1) NOT NULL DEFAULT 1.0;
ALTER TABLE bookings ADD COLUMN IF NOT EXISTS dispatch_round   INT NOT NULL DEFAULT 0;
ALTER TABLE bookings ADD COLUMN IF NOT EXISTS assigned_at      TIMESTAMPTZ;
ALTER TABLE bookings ADD COLUMN IF NOT EXISTS inserted_at      TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS idx_bookings_driver ON bookings (driver_id);

-- Every offer a driver gets and what they did with it: ACCEPTED and REJECTED are both stored
CREATE TABLE IF NOT EXISTS driver_responses (
    response_id    BIGSERIAL PRIMARY KEY,
    booking_id     BIGINT NOT NULL REFERENCES bookings(booking_id),
    driver_id      BIGINT NOT NULL REFERENCES drivers(driver_id),
    dispatch_round INT NOT NULL DEFAULT 1,
    response       VARCHAR(20) NOT NULL CHECK (response IN ('OFFERED', 'ACCEPTED', 'REJECTED', 'TIMEOUT')),
    outcome        VARCHAR(20) CHECK (outcome IN ('ASSIGNED', 'LOST', 'DECLINED', 'EXPIRED')),
    offered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    responded_at   TIMESTAMPTZ,
    UNIQUE (booking_id, driver_id)                  -- one offer per driver per booking
);
CREATE INDEX IF NOT EXISTS idx_driver_responses_open ON driver_responses (booking_id) WHERE response = 'OFFERED';
CREATE INDEX IF NOT EXISTS idx_driver_responses_driver_open ON driver_responses (driver_id) WHERE response = 'OFFERED';

-- Consumer idempotency: one row per Kafka event ever processed
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        VARCHAR(64) PRIMARY KEY,
    topic           VARCHAR(100) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    outcome         VARCHAR(30) NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every rejected record with its reason (Grafana "rejected" + messaging log)
CREATE TABLE IF NOT EXISTS rejected_records (
    id              BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(64) NOT NULL UNIQUE,
    booking_id      VARCHAR(64),
    idempotency_key VARCHAR(100),
    stage           VARCHAR(30) NOT NULL,
    error_code      VARCHAR(50) NOT NULL,
    error_message   TEXT,
    raw_payload     JSONB,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_rejected_code ON rejected_records (error_code);

-- Normalise values that may already exist (e.g. 'Available' from a spreadsheet import),
-- so the guards below never block updates of old rows.
UPDATE drivers SET availability_status = CASE
         WHEN availability_status IS NULL THEN 'AVAILABLE'
         WHEN upper(trim(availability_status)) IN ('AVAILABLE', 'ACTIVE', 'ONLINE', 'FREE') THEN 'AVAILABLE'
         WHEN upper(trim(availability_status)) IN ('ON_TRIP', 'ON TRIP', 'BUSY', 'ENGAGED') THEN 'ON_TRIP'
         ELSE 'OFFLINE' END
 WHERE availability_status IS NULL OR availability_status NOT IN ('AVAILABLE', 'ON_TRIP', 'OFFLINE');
UPDATE bookings SET booking_status = upper(trim(booking_status))
 WHERE booking_status IS NOT NULL AND booking_status <> upper(trim(booking_status));

-- Data-quality guards (NOT VALID = enforced for new rows, existing rows untouched)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_bookings_fare') THEN
    ALTER TABLE bookings ADD CONSTRAINT chk_bookings_fare CHECK (fare > 0 AND fare <= 5000) NOT VALID;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_bookings_status') THEN
    ALTER TABLE bookings ADD CONSTRAINT chk_bookings_status CHECK (booking_status IN
      ('REQUESTED','DRIVER_ASSIGNED','CONFIRMED','STARTED','COMPLETED','CANCELLED','FAILED','NO_DRIVER')) NOT VALID;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_bookings_route') THEN
    ALTER TABLE bookings ADD CONSTRAINT chk_bookings_route CHECK (pickup_location <> destination_location) NOT VALID;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_drivers_availability') THEN
    ALTER TABLE drivers ADD CONSTRAINT chk_drivers_availability CHECK (availability_status IN
      ('AVAILABLE','ON_TRIP','OFFLINE')) NOT VALID;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_payments_amount') THEN
    ALTER TABLE payments ADD CONSTRAINT chk_payments_amount CHECK (amount > 0) NOT VALID;
  END IF;
END $$;
