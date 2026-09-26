-- 001: the team's Supabase schema, exactly as designed (Person 2).
-- Written with IF NOT EXISTS so it is safe on a database where these tables already exist.
-- (The original script's DROP TABLE lines are intentionally NOT here: migrations never drop data.)

CREATE TABLE IF NOT EXISTS users (
    user_id     BIGINT PRIMARY KEY,
    full_name   VARCHAR(255) NOT NULL,
    email       VARCHAR(255) UNIQUE,
    phone       BIGINT,
    status      VARCHAR(50),
    created_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id           BIGINT PRIMARY KEY,
    driver_name         VARCHAR(255) NOT NULL,
    phone               BIGINT,
    availability_status VARCHAR(50),
    version_no          INT DEFAULT 1,              -- optimistic locking: a driver can't win two rides
    created_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id          BIGINT PRIMARY KEY,
    driver_id           BIGINT REFERENCES drivers(driver_id),
    registration_number VARCHAR(50),
    vehicle_type        VARCHAR(50),
    manufacturer        VARCHAR(50),
    model               VARCHAR(50),
    status              VARCHAR(50),
    created_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bookings (
    booking_id           BIGINT PRIMARY KEY,
    user_id              BIGINT REFERENCES users(user_id),
    driver_id            BIGINT NULL REFERENCES drivers(driver_id),   -- NULL while searching
    pickup_location      VARCHAR(255),
    destination_location VARCHAR(255),
    booking_status       VARCHAR(50),
    fare                 DECIMAL(10, 2),
    idempotency_key      VARCHAR(100) UNIQUE,                         -- strict dedup lock
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id            BIGINT PRIMARY KEY,
    booking_id            BIGINT REFERENCES bookings(booking_id),
    user_id               BIGINT REFERENCES users(user_id),
    amount                DECIMAL(10, 2),
    payment_method        VARCHAR(50),
    payment_status        VARCHAR(50),
    idempotency_key       VARCHAR(100) UNIQUE,                        -- strict dedup lock
    transaction_reference VARCHAR(100) UNIQUE,
    created_at            TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bookings_status ON bookings(booking_status);
