-- Base-layer schema for booking_db. Owner: Person 2 (Database).
-- Runs automatically the FIRST time the postgres container starts
-- (files in /docker-entrypoint-initdb.d run in name order).

-- Reference tables ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    user_id     BIGINT PRIMARY KEY,
    name        TEXT NOT NULL,
    phone       TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id   BIGINT PRIMARY KEY,
    name        TEXT NOT NULL,
    vehicle_no  TEXT UNIQUE,
    status      TEXT NOT NULL DEFAULT 'AVAILABLE'
                CHECK (status IN ('AVAILABLE', 'BUSY', 'OFFLINE'))
);

CREATE TABLE IF NOT EXISTS locations (
    name        TEXT PRIMARY KEY
);

-- Main table: columns match data/bookings.csv exactly (+ version) -----------
CREATE TABLE IF NOT EXISTS bookings (
    booking_id           BIGINT PRIMARY KEY,
    user_id              BIGINT NOT NULL REFERENCES users(user_id),
    driver_id            BIGINT REFERENCES drivers(driver_id),
    pickup_location      TEXT NOT NULL REFERENCES locations(name),
    destination_location TEXT NOT NULL REFERENCES locations(name),
    booking_status       TEXT NOT NULL CHECK (booking_status IN
        ('REQUESTED','DRIVER_ASSIGNED','CONFIRMED','STARTED','COMPLETED','CANCELLED','FAILED')),
    fare                 NUMERIC(10,2) NOT NULL CHECK (fare > 0 AND fare <= 5000),
    idempotency_key      TEXT NOT NULL UNIQUE,          -- blocks duplicates (rows 540, 541)
    version              INT NOT NULL DEFAULT 0,        -- optimistic locking (driver race, later)
    created_at           TIMESTAMPTZ NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL,
    inserted_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (pickup_location <> destination_location),
    CHECK (updated_at >= created_at)
) WITH (fillfactor = 80);                               -- room for cheap status updates

-- Few indexes on purpose: every index slows inserts.
CREATE INDEX IF NOT EXISTS ix_bookings_status ON bookings (booking_status);
CREATE INDEX IF NOT EXISTS ix_bookings_driver ON bookings (driver_id);

-- Idempotency: one row per Kafka event ever processed ------------------------
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        TEXT PRIMARY KEY,
    topic           TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    outcome         TEXT NOT NULL,                      -- INSERTED / DUPLICATE / REJECTED
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every rejected record, with the reason (feeds the Grafana dashboard) -------
CREATE TABLE IF NOT EXISTS rejected_records (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL UNIQUE,               -- same rejection never stored twice
    booking_id      TEXT,
    idempotency_key TEXT,
    stage           TEXT NOT NULL,                      -- validator / consumer
    error_code      TEXT NOT NULL,
    error_message   TEXT,
    raw_payload     JSONB,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rejected_code ON rejected_records (error_code);
