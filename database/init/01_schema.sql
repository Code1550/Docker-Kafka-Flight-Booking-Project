-- database/init/01_schema.sql
--
-- Initial database schema and seed data for the Flight Booking System.
--
-- This file is automatically executed by the PostgreSQL Docker container
-- on first startup via the docker-entrypoint-initdb.d/ mount defined in
-- docker-compose.yml. It runs exactly once — when the postgres_data volume
-- is empty (fresh container). Subsequent container restarts skip this file
-- because the volume already contains an initialised database.
--
-- Execution order:
--   Files in docker-entrypoint-initdb.d/ are executed in alphabetical order.
--   This file is named 01_schema.sql so it runs before any future seed files
--   (02_seed_users.sql, 03_seed_promotions.sql, etc.) that depend on these
--   tables existing first.
--
-- Table ownership:
--   flights  → read by Booking Service, written by Flight Inventory Service
--              (seeded here for the resume project)
--   bookings → owned and written exclusively by Booking Service
--   payments → owned and written exclusively by Payment Service
--
--   Each service owns its own table. Cross-service reads (e.g. Booking
--   Service reading the flights table) are permitted. Cross-service writes
--   are not — enforced by convention in this project, and by PostgreSQL
--   role permissions in production.
--
-- Migration strategy:
--   This file creates the initial schema. All subsequent schema changes
--   (adding columns, creating indexes, altering constraints) are handled
--   by Alembic migration files in each service's alembic/ directory.
--   Running `alembic upgrade head` inside any service container applies
--   pending migrations on top of this baseline schema.
--
-- UUID strategy:
--   All primary keys are VARCHAR(36) storing UUID strings in canonical form
--   (e.g. "a1b2c3d4-e5f6-7890-abcd-ef1234567890") rather than PostgreSQL's
--   native UUID type. This keeps the schema portable across databases used
--   in testing (SQLite, which has no native UUID type) and avoids the
--   implicit type casting that PostgreSQL applies when comparing UUID columns
--   with VARCHAR values passed from Python (where all UUIDs are strings).
--
-- Timestamp strategy:
--   All timestamps are TIMESTAMP WITHOUT TIME ZONE (naive UTC).
--   The application always inserts UTC values — no timezone conversion
--   is performed at the database layer. This avoids ambiguity from
--   PostgreSQL's timezone-aware TIMESTAMPTZ type when the application
--   and database are in different timezones (common in cloud deployments).


-- ─────────────────────────────────────────────────────────────────────────────
-- EXTENSIONS
-- ─────────────────────────────────────────────────────────────────────────────

-- pgcrypto — provides gen_random_uuid() for generating UUIDs in SQL.
-- Used in seed data INSERT statements to generate deterministic-looking
-- UUIDs without hardcoding them. In application code, UUIDs are generated
-- by Python's uuid.uuid4() — this extension is only used here in SQL.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- pg_trgm — trigram indexing for ILIKE text search on flight numbers
-- and passenger names. Enables fast "search as you type" queries like:
--   SELECT * FROM flights WHERE flight_number ILIKE '%AA1%';
-- without a full table scan. Not strictly required for the resume project
-- but demonstrates awareness of PostgreSQL's text search capabilities.
CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- ─────────────────────────────────────────────────────────────────────────────
-- SCHEMA SETUP
-- ─────────────────────────────────────────────────────────────────────────────

-- All tables live in the public schema (PostgreSQL default).
-- In production, consider using separate schemas per service:
--   CREATE SCHEMA booking_service;
--   CREATE SCHEMA payment_service;
-- This enforces service boundaries at the database level and allows
-- per-schema access control via GRANT/REVOKE.

SET search_path TO public;

-- Use UTC for all session-level timestamp operations.
-- Ensures that NOW() and CURRENT_TIMESTAMP return UTC values if used
-- in triggers or default expressions, consistent with application timestamps.
SET timezone TO 'UTC';


-- ─────────────────────────────────────────────────────────────────────────────
-- CUSTOM TYPES (ENUMS)
-- Using PostgreSQL enum types for status columns provides:
--   1. Storage efficiency — enums are stored as integers internally
--   2. Constraint enforcement — invalid values are rejected at the DB level
--   3. Self-documentation — pg_type catalog shows valid values
--
-- Trade-off: adding a new enum value requires ALTER TYPE which takes a
-- brief ACCESS EXCLUSIVE lock on the type. For high-write tables like
-- bookings and payments, use VARCHAR with CHECK constraints instead
-- (already defined on the ORM models — the enums here are for reference
-- and are not used by the ORM models which use VARCHAR).
-- ─────────────────────────────────────────────────────────────────────────────

-- Drop types if they exist from a previous partial initialisation.
-- IF NOT EXISTS is not supported for CREATE TYPE in older PostgreSQL versions
-- so we DROP first with IF EXISTS as a safety net.
DROP TYPE IF EXISTS booking_status_enum CASCADE;
CREATE TYPE booking_status_enum AS ENUM (
    'requested',        -- BookingRequestedEvent received by Booking Service
    'seat_held',        -- SeatReservedEvent received, Redis lock acquired
    'payment_pending',  -- PaymentIntent created, awaiting Stripe confirmation
    'confirmed',        -- PaymentProcessedEvent received, booking complete
    'failed',           -- Unrecoverable failure (declined card, seat unavailable)
    'cancelled'         -- Cancelled by passenger after confirmation
);

DROP TYPE IF EXISTS payment_status_enum CASCADE;
CREATE TYPE payment_status_enum AS ENUM (
    'pending',          -- Payment record created, Stripe call in progress
    'success',          -- Stripe PaymentIntent status = succeeded
    'failed',           -- Card declined or Stripe permanent error
    'refunded',         -- Charge reversed after confirmation
    'disputed'          -- Chargeback filed by passenger
);

DROP TYPE IF EXISTS seat_class_enum CASCADE;
CREATE TYPE seat_class_enum AS ENUM (
    'economy',
    'premium_economy',
    'business',
    'first'
);

DROP TYPE IF EXISTS notification_type_enum CASCADE;
CREATE TYPE notification_type_enum AS ENUM (
    'email',
    'sms',
    'both'
);

DROP TYPE IF EXISTS flight_status_enum CASCADE;
CREATE TYPE flight_status_enum AS ENUM (
    'scheduled',        -- Flight is on time and accepting bookings
    'boarding',         -- Boarding in progress, no new bookings
    'departed',         -- Flight has left the gate
    'arrived',          -- Flight has landed at destination
    'delayed',          -- Departure delayed, bookings still accepted
    'cancelled'         -- Flight cancelled, all bookings should be refunded
);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: flights
-- Read-only from the Booking Service's perspective.
-- Seeded below with sample flight data for local development.
-- In production, this table is managed by a Flight Inventory Service.
-- ─────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS flights CASCADE;

CREATE TABLE flights (

    -- ── PRIMARY KEY ──────────────────────────────────────────────────────────
    flight_id           VARCHAR(36)     NOT NULL,

    -- ── FLIGHT IDENTIFICATION ─────────────────────────────────────────────────
    flight_number       VARCHAR(10)     NOT NULL,
    airline_code        VARCHAR(3)      NOT NULL,   -- IATA airline designator e.g. "AA"
    airline_name        VARCHAR(100)    NOT NULL,   -- e.g. "American Airlines"

    -- ── ROUTE ────────────────────────────────────────────────────────────────
    -- IATA 3-letter airport codes stored in uppercase
    origin              CHAR(3)         NOT NULL,
    destination         CHAR(3)         NOT NULL,
    origin_city         VARCHAR(100)    NOT NULL,   -- e.g. "New York"
    destination_city    VARCHAR(100)    NOT NULL,   -- e.g. "Los Angeles"

    -- ── SCHEDULE ─────────────────────────────────────────────────────────────
    departure_time      TIMESTAMP       NOT NULL,   -- UTC
    arrival_time        TIMESTAMP       NOT NULL,   -- UTC
    -- Duration stored as minutes for easy display without datetime arithmetic
    duration_minutes    INTEGER         NOT NULL,

    -- ── CAPACITY ─────────────────────────────────────────────────────────────
    total_seats         INTEGER         NOT NULL,
    available_seats     INTEGER         NOT NULL,
    economy_seats       INTEGER         NOT NULL,
    premium_economy_seats INTEGER       NOT NULL    DEFAULT 0,
    business_seats      INTEGER         NOT NULL    DEFAULT 0,
    first_seats         INTEGER         NOT NULL    DEFAULT 0,

    -- ── PRICING (base fares per class) ───────────────────────────────────────
    -- Stored here for display purposes — actual fare used in payment is
    -- determined by PaymentService._get_price_for_seat_class() which uses
    -- fixed prices per class. In production, a dynamic pricing service
    -- would calculate fares based on demand, advance purchase, and season.
    economy_price           NUMERIC(10, 2)  NOT NULL,
    premium_economy_price   NUMERIC(10, 2),
    business_price          NUMERIC(10, 2),
    first_price             NUMERIC(10, 2),
    currency                CHAR(3)         NOT NULL    DEFAULT 'USD',

    -- ── STATUS ────────────────────────────────────────────────────────────────
    status              VARCHAR(20)     NOT NULL    DEFAULT 'scheduled',

    -- ── AIRCRAFT INFORMATION ──────────────────────────────────────────────────
    aircraft_type       VARCHAR(50),            -- e.g. "Boeing 737-800"
    aircraft_registration VARCHAR(10),          -- e.g. "N123AA"

    -- ── AUDIT ────────────────────────────────────────────────────────────────
    created_at          TIMESTAMP       NOT NULL    DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL    DEFAULT NOW(),

    -- ── CONSTRAINTS ───────────────────────────────────────────────────────────
    CONSTRAINT pk_flights
        PRIMARY KEY (flight_id),

    CONSTRAINT uq_flights_number_departure
        UNIQUE (flight_number, departure_time),

    CONSTRAINT ck_flights_arrival_after_departure
        CHECK (arrival_time > departure_time),

    CONSTRAINT ck_flights_available_seats_non_negative
        CHECK (available_seats >= 0),

    CONSTRAINT ck_flights_total_seats_positive
        CHECK (total_seats > 0),

    CONSTRAINT ck_flights_available_lte_total
        CHECK (available_seats <= total_seats),

    CONSTRAINT ck_flights_origin_differs_destination
        CHECK (origin <> destination),

    CONSTRAINT ck_flights_origin_uppercase
        CHECK (origin = UPPER(origin)),

    CONSTRAINT ck_flights_destination_uppercase
        CHECK (destination = UPPER(destination)),

    CONSTRAINT ck_flights_status_valid
        CHECK (status IN (
            'scheduled', 'boarding', 'departed',
            'arrived', 'delayed', 'cancelled'
        )),

    CONSTRAINT ck_flights_economy_price_positive
        CHECK (economy_price > 0),

    CONSTRAINT ck_flights_duration_positive
        CHECK (duration_minutes > 0)
);

-- ── INDEXES ───────────────────────────────────────────────────────────────────

-- Primary lookup pattern: "find flights from JFK to LAX departing tomorrow"
CREATE INDEX ix_flights_route_departure
    ON flights (origin, destination, departure_time);

-- Status-based filtering: "show all scheduled flights"
CREATE INDEX ix_flights_status
    ON flights (status);

-- Flight number lookup: "find flight AA123"
CREATE INDEX ix_flights_flight_number
    ON flights (flight_number);

-- Trigram index for partial flight number search:
-- "find all flights containing AA1"
CREATE INDEX ix_flights_flight_number_trgm
    ON flights USING gin (flight_number gin_trgm_ops);

-- Availability filtering: "find flights with available seats"
CREATE INDEX ix_flights_available_seats
    ON flights (available_seats)
    WHERE available_seats > 0;

-- ── COMMENTS ──────────────────────────────────────────────────────────────────

COMMENT ON TABLE flights IS
    'Available flight inventory. Read-only for Booking Service. '
    'Managed by Flight Inventory Service in production.';

COMMENT ON COLUMN flights.origin IS 'IATA 3-letter departure airport code (uppercase)';
COMMENT ON COLUMN flights.destination IS 'IATA 3-letter arrival airport code (uppercase)';
COMMENT ON COLUMN flights.available_seats IS
    'Decremented by Seat Service when Redis lock is acquired. '
    'Read-only for Booking Service.';


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: bookings
-- Owned exclusively by Booking Service.
-- One row per BookingRequestedEvent, updated as subsequent events arrive.
-- ─────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS bookings CASCADE;

CREATE TABLE bookings (

    -- ── PRIMARY KEY ───────────────────────────────────────────────────────────
    booking_id          VARCHAR(36)     NOT NULL,

    -- ── TRACING ───────────────────────────────────────────────────────────────
    correlation_id      VARCHAR(36)     NOT NULL,

    -- ── PASSENGER ────────────────────────────────────────────────────────────
    -- Denormalised from BookingRequestedEvent — snapshot at time of booking.
    -- Immune to subsequent passenger profile updates.
    passenger_id            VARCHAR(36)     NOT NULL,
    passenger_first_name    VARCHAR(100)    NOT NULL,
    passenger_last_name     VARCHAR(100)    NOT NULL,
    passenger_email         VARCHAR(254)    NOT NULL,
    passenger_phone         VARCHAR(30),
    passport_number         VARCHAR(20),

    -- ── FLIGHT ───────────────────────────────────────────────────────────────
    -- Denormalised snapshot — preserves what was booked even if the flight
    -- record is later amended (e.g. schedule change after booking).
    flight_id           VARCHAR(36)     NOT NULL,
    flight_number       VARCHAR(10)     NOT NULL,
    origin              CHAR(3)         NOT NULL,
    destination         CHAR(3)         NOT NULL,
    departure_time      TIMESTAMP       NOT NULL,
    arrival_time        TIMESTAMP       NOT NULL,
    seat_number         VARCHAR(4)      NOT NULL,
    seat_class          VARCHAR(20)     NOT NULL,

    -- ── PRICING ───────────────────────────────────────────────────────────────
    total_price         NUMERIC(10, 2)  NOT NULL,
    currency            CHAR(3)         NOT NULL    DEFAULT 'USD',

    -- ── PAYMENT ──────────────────────────────────────────────────────────────
    -- Populated when PaymentProcessedEvent is consumed.
    payment_id          VARCHAR(36),
    payment_amount      NUMERIC(10, 2),
    payment_currency    CHAR(3),
    payment_method      VARCHAR(50),
    transaction_ref     VARCHAR(100),

    -- ── NOTIFICATION PREFERENCE ───────────────────────────────────────────────
    notification_type   VARCHAR(10)     NOT NULL    DEFAULT 'email',

    -- ── STATUS ────────────────────────────────────────────────────────────────
    status              VARCHAR(20)     NOT NULL    DEFAULT 'requested',

    -- ── FAILURE TRACKING ──────────────────────────────────────────────────────
    failure_reason      TEXT,

    -- ── AUDIT TIMESTAMPS ──────────────────────────────────────────────────────
    -- Set by Booking Service in UTC — not by PostgreSQL DEFAULT NOW().
    -- See booking-service/app/db/models.py module docstring for rationale.
    requested_at        TIMESTAMP,
    confirmed_at        TIMESTAMP,
    failed_at           TIMESTAMP,
    cancelled_at        TIMESTAMP,
    created_at          TIMESTAMP       NOT NULL    DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL    DEFAULT NOW(),

    -- ── CONSTRAINTS ───────────────────────────────────────────────────────────
    CONSTRAINT pk_bookings
        PRIMARY KEY (booking_id),

    CONSTRAINT uq_bookings_passenger_flight_seat
        UNIQUE (passenger_id, flight_id, seat_number),

    CONSTRAINT ck_bookings_status_valid
        CHECK (status IN (
            'requested', 'seat_held', 'payment_pending',
            'confirmed', 'failed', 'cancelled'
        )),

    CONSTRAINT ck_bookings_total_price_positive
        CHECK (total_price > 0),

    CONSTRAINT ck_bookings_seat_class_valid
        CHECK (seat_class IN (
            'economy', 'premium_economy', 'business', 'first'
        )),

    CONSTRAINT ck_bookings_notification_type_valid
        CHECK (notification_type IN ('email', 'sms', 'both')),

    CONSTRAINT ck_bookings_arrival_after_departure
        CHECK (arrival_time > departure_time),

    CONSTRAINT ck_bookings_confirmed_at_requires_payment
        CHECK (
            confirmed_at IS NULL OR payment_id IS NOT NULL
        )
);

-- ── INDEXES ───────────────────────────────────────────────────────────────────

-- Primary passenger lookup: "show all bookings for passenger X"
CREATE INDEX ix_bookings_passenger_id
    ON bookings (passenger_id);

-- Status monitoring: "how many bookings are in each status?"
CREATE INDEX ix_bookings_status
    ON bookings (status);

-- Correlation tracing: "find all DB records for correlation_id X"
CREATE INDEX ix_bookings_correlation_id
    ON bookings (correlation_id);

-- Flight + status composite: "show all confirmed bookings on flight X"
CREATE INDEX ix_bookings_flight_status
    ON bookings (flight_id, status);

-- Time-range reporting: "bookings requested in the last 24 hours"
CREATE INDEX ix_bookings_status_requested_at
    ON bookings (status, requested_at);

-- Email lookup: "find booking for passenger@example.com"
CREATE INDEX ix_bookings_passenger_email
    ON bookings (passenger_email);

-- Partial index — confirmed bookings only (most common read pattern)
CREATE INDEX ix_bookings_confirmed
    ON bookings (passenger_id, confirmed_at)
    WHERE status = 'confirmed';

-- ── COMMENTS ──────────────────────────────────────────────────────────────────

COMMENT ON TABLE bookings IS
    'Flight booking lifecycle records. Owned exclusively by Booking Service. '
    'Created on booking.requested, updated by subsequent Kafka events.';

COMMENT ON COLUMN bookings.booking_id IS
    'UUID generated by API Gateway. Used as Kafka message key for partition ordering.';
COMMENT ON COLUMN bookings.correlation_id IS
    'Trace ID shared across all services. Join logs on this column.';
COMMENT ON COLUMN bookings.failure_reason IS
    'Human-readable failure reason when status=failed. '
    'Examples: seat already reserved, card declined, flight not found.';


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: payments
-- Owned exclusively by Payment Service.
-- One row per Stripe charge attempt (success AND failure).
-- ─────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS payments CASCADE;

CREATE TABLE payments (

    -- ── PRIMARY KEY ───────────────────────────────────────────────────────────
    payment_id          VARCHAR(36)     NOT NULL,

    -- ── BOOKING REFERENCE ─────────────────────────────────────────────────────
    -- Not a foreign key — see payment-service/app/db/models.py for rationale.
    booking_id          VARCHAR(36)     NOT NULL,
    correlation_id      VARCHAR(36)     NOT NULL,

    -- ── FLIGHT AND SEAT ───────────────────────────────────────────────────────
    -- Denormalised from SeatReservedEvent for self-contained payment records.
    flight_id           VARCHAR(36)     NOT NULL,
    seat_number         VARCHAR(4)      NOT NULL,
    seat_class          VARCHAR(20)     NOT NULL,

    -- ── PAYMENT AMOUNTS ───────────────────────────────────────────────────────
    amount              NUMERIC(10, 2)  NOT NULL,
    currency            CHAR(3)         NOT NULL    DEFAULT 'USD',
    captured_amount     NUMERIC(10, 2),

    -- ── PAYMENT METHOD ────────────────────────────────────────────────────────
    payment_method      VARCHAR(50)     NOT NULL,
    card_brand          VARCHAR(20),   -- e.g. "visa", "mastercard"
    card_last4          CHAR(4),       -- last 4 digits of card number

    -- ── STRIPE REFERENCE ──────────────────────────────────────────────────────
    transaction_ref         VARCHAR(100),  -- Stripe PaymentIntent ID
    stripe_error_code       VARCHAR(100),
    stripe_decline_code     VARCHAR(100),

    -- ── STATUS ────────────────────────────────────────────────────────────────
    status              VARCHAR(20)     NOT NULL    DEFAULT 'pending',
    failure_reason      TEXT,
    retry_count         INTEGER         NOT NULL    DEFAULT 0,

    -- ── REFUND TRACKING ───────────────────────────────────────────────────────
    refund_amount       NUMERIC(10, 2),
    refund_ref          VARCHAR(100),
    refund_reason       VARCHAR(100),

    -- ── AUDIT TIMESTAMPS ──────────────────────────────────────────────────────
    attempted_at        TIMESTAMP       NOT NULL,
    completed_at        TIMESTAMP,
    refunded_at         TIMESTAMP,
    created_at          TIMESTAMP       NOT NULL    DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL    DEFAULT NOW(),

    -- ── CONSTRAINTS ───────────────────────────────────────────────────────────
    CONSTRAINT pk_payments
        PRIMARY KEY (payment_id),

    CONSTRAINT uq_payments_booking_id
        UNIQUE (booking_id),

    CONSTRAINT uq_payments_transaction_ref
        UNIQUE (transaction_ref),

    CONSTRAINT ck_payments_status_valid
        CHECK (status IN (
            'pending', 'success', 'failed', 'refunded', 'disputed'
        )),

    CONSTRAINT ck_payments_amount_positive
        CHECK (amount > 0),

    CONSTRAINT ck_payments_captured_lte_amount
        CHECK (captured_amount IS NULL OR captured_amount <= amount),

    CONSTRAINT ck_payments_refund_lte_amount
        CHECK (refund_amount IS NULL OR refund_amount <= amount),

    CONSTRAINT ck_payments_refunded_has_ref
        CHECK (status <> 'refunded' OR refund_ref IS NOT NULL),

    CONSTRAINT ck_payments_completed_after_attempted
        CHECK (completed_at IS NULL OR completed_at >= attempted_at),

    CONSTRAINT ck_payments_retry_count_non_negative
        CHECK (retry_count >= 0),

    CONSTRAINT ck_payments_seat_class_valid
        CHECK (seat_class IN (
            'economy', 'premium_economy', 'business', 'first'
        ))
);

-- ── INDEXES ───────────────────────────────────────────────────────────────────

-- Idempotency lookup: booking_id already covered by unique constraint index
-- Additional explicit index for clarity and to support query planner hints
CREATE INDEX ix_payments_booking_id
    ON payments (booking_id);

-- Status + time reporting: "all successful payments this month"
CREATE INDEX ix_payments_status_attempted_at
    ON payments (status, attempted_at);

-- Correlation tracing
CREATE INDEX ix_payments_correlation_id
    ON payments (correlation_id);

-- Flight reconciliation: "all payments for flight X"
CREATE INDEX ix_payments_flight_id
    ON payments (flight_id);

-- Finance reporting: "total revenue by currency"
CREATE INDEX ix_payments_currency_status
    ON payments (currency, status);

-- Fraud analysis: "all payments with error code stolen_card"
CREATE INDEX ix_payments_stripe_error_code
    ON payments (stripe_error_code)
    WHERE stripe_error_code IS NOT NULL;

-- Partial index on successful payments with transaction_ref
CREATE INDEX ix_payments_transaction_ref_success
    ON payments (transaction_ref)
    WHERE status = 'success';

-- ── COMMENTS ──────────────────────────────────────────────────────────────────

COMMENT ON TABLE payments IS
    'Payment transaction records. Owned exclusively by Payment Service. '
    'Written for every Stripe charge attempt — both successes and failures.';

COMMENT ON COLUMN payments.booking_id IS
    'Not a foreign key to bookings table — see payment-service/app/db/models.py. '
    'Unique constraint ensures one payment record per booking (idempotency anchor).';
COMMENT ON COLUMN payments.transaction_ref IS
    'Stripe PaymentIntent ID. Used to look up charges in the Stripe dashboard.';


-- ─────────────────────────────────────────────────────────────────────────────
-- UPDATED_AT TRIGGER
-- Automatically updates the updated_at column on every UPDATE.
-- Without this trigger, Booking Service would need to explicitly set
-- updated_at = NOW() on every UPDATE statement — easy to forget and
-- causes the column to become stale.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    -- NEW refers to the row being inserted or updated.
    -- Setting NEW.updated_at ensures every UPDATE automatically stamps
    -- the current UTC time without any application-level code.
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply the trigger to all three tables that have an updated_at column.
-- BEFORE UPDATE ensures the timestamp is set before the row is written,
-- not after — so the updated_at value is consistent with the same transaction.

CREATE TRIGGER trg_flights_updated_at
    BEFORE UPDATE ON flights
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_bookings_updated_at
    BEFORE UPDATE ON bookings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_payments_updated_at
    BEFORE UPDATE ON payments
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- ─────────────────────────────────────────────────────────────────────────────
-- SEED DATA: FLIGHTS
-- 10 sample flights across popular US and international routes.
-- Covers all four seat classes so every booking scenario can be tested.
-- Departure times are set far in the future (2027) so the
-- must_be_future validator in api-gateway/app/models.py never rejects them.
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO flights (
    flight_id,
    flight_number,
    airline_code,
    airline_name,
    origin,
    destination,
    origin_city,
    destination_city,
    departure_time,
    arrival_time,
    duration_minutes,
    total_seats,
    available_seats,
    economy_seats,
    premium_economy_seats,
    business_seats,
    first_seats,
    economy_price,
    premium_economy_price,
    business_price,
    first_price,
    currency,
    status,
    aircraft_type
) VALUES

-- ── DOMESTIC US ROUTES ────────────────────────────────────────────────────────

-- Flight 1: New York JFK → Los Angeles LAX
(
    'f1000001-0000-4000-a000-000000000001',
    'AA123',
    'AA',
    'American Airlines',
    'JFK',
    'LAX',
    'New York',
    'Los Angeles',
    '2027-06-01 08:00:00',
    '2027-06-01 11:30:00',
    330,   -- 5h 30m
    180,
    180,
    150, 20, 8, 2,
    299.99, 599.99, 1299.99, 2499.99,
    'USD',
    'scheduled',
    'Boeing 737-800'
),

-- Flight 2: Los Angeles LAX → New York JFK (return)
(
    'f1000002-0000-4000-a000-000000000002',
    'AA456',
    'AA',
    'American Airlines',
    'LAX',
    'JFK',
    'Los Angeles',
    'New York',
    '2027-06-15 10:00:00',
    '2027-06-15 18:00:00',
    480,   -- 8h 00m (cross-country, headwinds)
    180,
    180,
    150, 20, 8, 2,
    299.99, 599.99, 1299.99, 2499.99,
    'USD',
    'scheduled',
    'Boeing 737-800'
),

-- Flight 3: Chicago ORD → Miami MIA
(
    'f1000003-0000-4000-a000-000000000003',
    'UA789',
    'UA',
    'United Airlines',
    'ORD',
    'MIA',
    'Chicago',
    'Miami',
    '2027-07-04 09:00:00',
    '2027-07-04 13:30:00',
    270,   -- 4h 30m
    160,
    160,
    130, 18, 10, 2,
    189.99, 399.99, 899.99, 1799.99,
    'USD',
    'scheduled',
    'Airbus A320'
),

-- Flight 4: San Francisco SFO → Seattle SEA
(
    'f1000004-0000-4000-a000-000000000004',
    'DL321',
    'DL',
    'Delta Air Lines',
    'SFO',
    'SEA',
    'San Francisco',
    'Seattle',
    '2027-07-10 07:30:00',
    '2027-07-10 09:45:00',
    135,   -- 2h 15m
    150,
    150,
    130, 12, 8, 0,
    129.99, 249.99, 549.99, NULL,
    'USD',
    'scheduled',
    'Boeing 737-700'
),

-- Flight 5: Dallas DFW → Denver DEN
(
    'f1000005-0000-4000-a000-000000000005',
    'SW567',
    'SW',
    'Southwest Airlines',
    'DFW',
    'DEN',
    'Dallas',
    'Denver',
    '2027-08-20 14:00:00',
    '2027-08-20 15:45:00',
    105,   -- 1h 45m
    175,
    175,
    175, 0, 0, 0,  -- Southwest: all economy, no premium classes
    149.99, NULL, NULL, NULL,
    'USD',
    'scheduled',
    'Boeing 737-MAX 8'
),

-- ── INTERNATIONAL ROUTES ──────────────────────────────────────────────────────

-- Flight 6: New York JFK → London LHR
(
    'f1000006-0000-4000-a000-000000000006',
    'BA112',
    'BA',
    'British Airways',
    'JFK',
    'LHR',
    'New York',
    'London',
    '2027-09-01 18:00:00',
    '2027-09-02 06:00:00',
    420,   -- 7h 00m
    280,
    280,
    200, 40, 30, 10,
    699.99, 1199.99, 2999.99, 5999.99,
    'USD',
    'scheduled',
    'Boeing 777-300ER'
),

-- Flight 7: London LHR → New York JFK (return)
(
    'f1000007-0000-4000-a000-000000000007',
    'BA113',
    'BA',
    'British Airways',
    'LHR',
    'JFK',
    'London',
    'New York',
    '2027-09-14 11:00:00',
    '2027-09-14 14:00:00',
    480,   -- 8h 00m (against jet stream)
    280,
    280,
    200, 40, 30, 10,
    699.99, 1199.99, 2999.99, 5999.99,
    'USD',
    'scheduled',
    'Boeing 777-300ER'
),

-- Flight 8: Los Angeles LAX → Tokyo NRT
(
    'f1000008-0000-4000-a000-000000000008',
    'JL062',
    'JL',
    'Japan Airlines',
    'LAX',
    'NRT',
    'Los Angeles',
    'Tokyo',
    '2027-10-05 12:00:00',
    '2027-10-06 16:00:00',
    660,   -- 11h 00m
    244,
    244,
    180, 32, 26, 6,
    899.99, 1499.99, 3499.99, 6999.99,
    'USD',
    'scheduled',
    'Boeing 787-9 Dreamliner'
),

-- Flight 9: Sydney SYD → Singapore SIN
(
    'f1000009-0000-4000-a000-000000000009',
    'SQ222',
    'SQ',
    'Singapore Airlines',
    'SYD',
    'SIN',
    'Sydney',
    'Singapore',
    '2027-11-11 21:00:00',
    '2027-11-12 04:30:00',
    510,   -- 8h 30m
    255,
    255,
    185, 36, 30, 4,
    799.99, 1399.99, 3199.99, 7499.99,
    'USD',
    'scheduled',
    'Airbus A350-900'
),

-- Flight 10: Miami MIA → Cancun CUN (short international)
(
    'f1000010-0000-4000-a000-000000000010',
    'AA999',
    'AA',
    'American Airlines',
    'MIA',
    'CUN',
    'Miami',
    'Cancun',
    '2027-12-20 09:00:00',
    '2027-12-20 10:30:00',
    90,   -- 1h 30m
    160,
    160,
    140, 14, 6, 0,
    199.99, 349.99, 749.99, NULL,
    'USD',
    'scheduled',
    'Airbus A319'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- SEED DATA VERIFICATION
-- Sanity-check the seed data immediately after insertion.
-- If any assertion fails, the entire initialisation script fails and
-- PostgreSQL rolls back — preventing a partially-seeded database.
-- ─────────────────────────────────────────────────────────────────────────────

DO $$
DECLARE
    flight_count    INTEGER;
    invalid_flights INTEGER;
BEGIN
    -- Verify all 10 flights were inserted
    SELECT COUNT(*) INTO flight_count FROM flights;
    IF flight_count <> 10 THEN
        RAISE EXCEPTION
            'Seed data error: expected 10 flights, found %', flight_count;
    END IF;

    -- Verify all flights have valid routes (origin <> destination)
    SELECT COUNT(*) INTO invalid_flights
    FROM flights
    WHERE origin = destination;

    IF invalid_flights > 0 THEN
        RAISE EXCEPTION
            'Seed data error: % flight(s) have identical origin and destination',
            invalid_flights;
    END IF;

    -- Verify all flights depart in the future
    -- (uses a hardcoded date well before the seed dates)
    SELECT COUNT(*) INTO invalid_flights
    FROM flights
    WHERE departure_time < '2027-01-01 00:00:00';

    IF invalid_flights > 0 THEN
        RAISE EXCEPTION
            'Seed data error: % flight(s) have past departure times',
            invalid_flights;
    END IF;

    RAISE NOTICE
        'Schema initialisation complete: % flights seeded successfully.',
        flight_count;
END $$;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEWS (optional — useful for Grafana and development queries)
-- ─────────────────────────────────────────────────────────────────────────────

-- View: booking summary with payment status joined
-- Used by development queries and Grafana dashboards to show the current
-- state of all bookings without writing a multi-table JOIN every time.
CREATE OR REPLACE VIEW v_booking_summary AS
SELECT
    b.booking_id,
    b.correlation_id,
    b.passenger_first_name  || ' ' || b.passenger_last_name
                            AS passenger_name,
    b.passenger_email,
    b.flight_number,
    b.origin                || ' → ' || b.destination
                            AS route,
    b.departure_time,
    b.seat_number,
    b.seat_class,
    b.total_price,
    b.currency,
    b.status                AS booking_status,
    b.failure_reason,
    b.requested_at,
    b.confirmed_at,
    p.status                AS payment_status,
    p.transaction_ref       AS stripe_payment_intent,
    p.amount                AS payment_amount,
    p.stripe_error_code,
    p.retry_count           AS payment_retries
FROM
    bookings b
LEFT JOIN
    payments p ON p.booking_id = b.booking_id
ORDER BY
    b.requested_at DESC;

COMMENT ON VIEW v_booking_summary IS
    'Joined view of bookings + payments for dashboards and development queries. '
    'Do not use in high-frequency application queries — use direct table access.';


-- View: flight availability summary
-- Shows available seat counts by class for operational monitoring.
CREATE OR REPLACE VIEW v_flight_availability AS
SELECT
    f.flight_id,
    f.flight_number,
    f.origin || ' → ' || f.destination AS route,
    f.departure_time,
    f.status,
    f.available_seats,
    f.total_seats,
    ROUND(
        (f.available_seats::NUMERIC / f.total_seats::NUMERIC) * 100,
        1
    )                       AS availability_pct,
    -- Count confirmed bookings per class for cross-checking
    COUNT(b.booking_id)
        FILTER (WHERE b.seat_class = 'economy'
                  AND b.status = 'confirmed')
                            AS economy_booked,
    COUNT(b.booking_id)
        FILTER (WHERE b.seat_class = 'business'
                  AND b.status = 'confirmed')
                            AS business_booked,
    COUNT(b.booking_id)
        FILTER (WHERE b.seat_class = 'first'
                  AND b.status = 'confirmed')
                            AS first_booked
FROM
    flights f
LEFT JOIN
    bookings b ON b.flight_id = f.flight_id
GROUP BY
    f.flight_id,
    f.flight_number,
    f.origin,
    f.destination,
    f.departure_time,
    f.status,
    f.available_seats,
    f.total_seats
ORDER BY
    f.departure_time;

COMMENT ON VIEW v_flight_availability IS
    'Flight availability summary for operational monitoring and Grafana dashboards.';


-- ─────────────────────────────────────────────────────────────────────────────
-- GRANT PERMISSIONS
-- In production, each service should connect with a dedicated PostgreSQL
-- role that has only the permissions it needs:
--   booking_service_role  → INSERT/UPDATE/SELECT on bookings,
--                           SELECT on flights
--   payment_service_role  → INSERT/UPDATE/SELECT on payments
--   readonly_role         → SELECT on all tables and views
--                           (for Grafana, analytics, support queries)
--
-- For local development, all services connect as the same superuser
-- (POSTGRES_USER in .env) so no explicit GRANTs are needed. The GRANT
-- statements below are commented out as a production reference.
-- ─────────────────────────────────────────────────────────────────────────────

-- -- Create service roles (uncomment for production)
-- CREATE ROLE booking_service_role LOGIN PASSWORD 'booking_secret';
-- CREATE ROLE payment_service_role LOGIN PASSWORD 'payment_secret';
-- CREATE ROLE readonly_role        LOGIN PASSWORD 'readonly_secret';
--
-- -- Booking Service: full access to bookings, read-only on flights
-- GRANT SELECT, INSERT, UPDATE ON bookings TO booking_service_role;
-- GRANT SELECT                  ON flights  TO booking_service_role;
--
-- -- Payment Service: full access to payments only
-- GRANT SELECT, INSERT, UPDATE ON payments TO payment_service_role;
--
-- -- Read-only role: all tables and views (for Grafana, analytics)
-- GRANT SELECT ON flights, bookings, payments       TO readonly_role;
-- GRANT SELECT ON v_booking_summary, v_flight_availability TO readonly_role;


-- ─────────────────────────────────────────────────────────────────────────────
-- INITIALISATION COMPLETE
-- ─────────────────────────────────────────────────────────────────────────────

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Flight Booking System — DB Initialised';
    RAISE NOTICE '  Tables:  flights, bookings, payments';
    RAISE NOTICE '  Views:   v_booking_summary, v_flight_availability';
    RAISE NOTICE '  Seed:    10 flights (2027 departures)';
    RAISE NOTICE '  Trigger: updated_at auto-stamp on all tables';
    RAISE NOTICE '========================================';
END $$;