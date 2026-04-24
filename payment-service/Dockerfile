# payment-service/Dockerfile
#
# Multi-stage build for the Payment Service.
#
# The Payment Service is a Kafka consumer that:
#   - Consumes SeatReservedEvent from `seat.reserved`
#   - Processes payments via a Stripe mock (or real Stripe in production)
#   - Publishes PaymentProcessedEvent or PaymentFailedEvent to downstream topics
#   - Writes payment transaction records to PostgreSQL
#
# Like the Booking Service, this is a headless worker — no HTTP server,
# no EXPOSE instruction. The CMD runs the consumer loop directly.
#
# Differences from booking-service/Dockerfile:
#   - Prometheus metrics exposed on port 8002 (8001 is Booking Service)
#   - entrypoint.sh runs Payment Service specific migrations (payments table)
#   - HEALTHCHECK sentinel file path is the same pattern but process is
#     payment-specific
#   - No alembic/ directory mounted — Payment Service owns only the
#     payments table, migrations live in payment-service/alembic/


# ──────────────────────────────────────────────────────────────────────────────
# STAGE 1 — builder
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS builder

# Prevents Python from writing .pyc bytecode files to disk.
ENV PYTHONDONTWRITEBYTECODE=1

# Ensures log output is sent to the terminal immediately without buffering.
# Essential for a payment service — you want to see every transaction log
# in real time, not batched and delayed.
ENV PYTHONUNBUFFERED=1

WORKDIR /build

# Install build-time system dependencies required to compile confluent-kafka.
# confluent-kafka is a C extension wrapping librdkafka. The slim base image
# ships without a C compiler or the Kafka C headers so we install them here.
#
# gcc          — C compiler for building the confluent-kafka extension
# librdkafka-dev — librdkafka C headers and static library
# python3-dev  — Python C headers required by any C extension build
# libssl-dev   — OpenSSL headers — required by confluent-kafka when Kafka
#                is configured with SSL/TLS (needed even for non-TLS local
#                dev to avoid compile-time errors)
# libffi-dev   — Foreign Function Interface headers required by cryptography
#                package used for Stripe payload signature verification
#
# All build tools are installed in Stage 1 only — they are NOT copied to
# the runtime image, keeping the final image lean and attack-surface small.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        librdkafka-dev \
        python3-dev \
        libssl-dev \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements before source code to maximise Docker layer cache hits.
# The pip install layer is only invalidated when requirements.txt changes,
# not on every application code edit — saves significant build time during
# active development.
COPY requirements.txt .

# Install all Python dependencies into /install so Stage 2 can copy only
# the compiled packages without bringing the build tools along.
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefix=/install -r requirements.txt


# ──────────────────────────────────────────────────────────────────────────────
# STAGE 2 — runtime
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install runtime-only system libraries.
#
# librdkafka1  — the compiled confluent-kafka extension dynamically links
#                against this shared object at runtime. Without it the
#                consumer crashes on startup with:
#                  ImportError: librdkafka.so.1: cannot open shared object file
#
# libssl3      — OpenSSL runtime library. Required by the cryptography package
#                used for Stripe webhook signature verification and by
#                confluent-kafka for SSL/TLS Kafka connections.
#
# ca-certificates — system CA bundle. Required when making HTTPS requests
#                   to the Stripe API (or any external payment gateway).
#                   Without it, httpx raises SSL certificate verification
#                   errors even against legitimate Stripe endpoints.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        librdkafka1 \
        libssl3 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a dedicated non-root user for the Payment Service process.
# The Payment Service handles sensitive financial data and communicates
# with payment gateways — running as non-root limits the blast radius
# of any container escape or remote code execution vulnerability.
RUN addgroup --system appgroup && \
    adduser  --system --ingroup appgroup appuser

WORKDIR /app

# Copy compiled Python packages from builder.
# Only the installed packages are transferred — compilers, headers,
# and build tools stay behind in the builder stage.
COPY --from=builder /install /usr/local

# Copy application source code.
# Copying app/ last means all earlier layers (apt install, pip install)
# are reused from cache on code-only changes — the most common case
# during development.
COPY app/ ./app/

# Copy Alembic migration config and files.
# Payment Service owns the payments table so its migrations live here.
# entrypoint.sh runs `alembic upgrade head` before the consumer starts
# to ensure the schema is always current.
COPY alembic.ini ./alembic.ini
COPY alembic/    ./alembic/

# Copy and permission the entrypoint script.
# entrypoint.sh orchestrates the startup sequence:
#   1. Wait for PostgreSQL to accept connections
#   2. Wait for Kafka to be reachable
#   3. Run Alembic migrations (payments table)
#   4. Start the Kafka consumer loop
#
# Stripe mock validation happens inside the consumer, not at startup,
# because the mock server may not be running in all environments.
COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

# shared/ is intentionally NOT copied here.
# Mounted at runtime via docker-compose volumes:
#   volumes:
#     - ./shared:/app/shared
# Allows shared/schemas.py changes to take effect without an image rebuild.

# Switch to non-root user.
# Must come after all COPY and RUN instructions that need elevated privileges.
USER appuser

# ── HEALTHCHECK ──────────────────────────────────────────────────────────────
# The Payment Service has no HTTP server so we use the same sentinel file
# pattern as the Booking Service — the consumer loop touches this file on
# every successfully processed payment event.
#
# --start-period=90s   → longer than Booking Service (60s) because the
#                         Payment Service waits for both PostgreSQL AND
#                         Kafka to be healthy before starting, and the
#                         Stripe mock container may also need warm-up time.
# --interval=30s       → check every 30 seconds after start-period expires
# --timeout=10s        → kill the check after 10 seconds if it hangs
# --retries=3          → mark unhealthy after 3 consecutive failures
#
# The check verifies two conditions:
#   1. The sentinel file exists (consumer has started)
#   2. The file was modified within the last 90 seconds (consumer is not stalled)
#      90s threshold is higher than Booking Service (60s) because payment
#      processing involves an external API call (Stripe mock) which can be
#      slower than a pure DB write.
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["sh", "-c", \
         "test -f /tmp/payment_consumer_heartbeat && \
          [ $(( $(date +%s) - $(date +%s -r /tmp/payment_consumer_heartbeat) )