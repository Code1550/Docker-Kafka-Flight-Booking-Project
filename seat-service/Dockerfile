# seat-service/Dockerfile
#
# Multi-stage build for the Seat Service.
#
# The Seat Service is a Kafka consumer that:
#   - Consumes BookingRequestedEvent from `booking.requested`
#   - Acquires a distributed Redis lock on the requested seat
#   - Publishes SeatReservedEvent to `seat.reserved` on lock success
#   - Publishes SeatUnavailableEvent to `seat.unavailable` on lock failure
#   - Releases Redis locks when PaymentFailedEvent is consumed from
#     `payment.failed` (seat becomes available again for other passengers)
#
# Key differences from other service Dockerfiles:
#   - No PostgreSQL dependency — the Seat Service uses Redis exclusively
#     for seat lock state. No Alembic, no asyncpg, no DB wait in entrypoint.
#   - No alembic/ directory — Redis is schema-less, no migrations needed.
#   - HEALTHCHECK sentinel file path is /tmp/seat_consumer_heartbeat to
#     avoid colliding with the Booking and Payment Service sentinel files
#     if all services run on the same Docker host.
#   - Prometheus metrics exposed on port 8003 (8001=Booking, 8002=Payment).
#   - entrypoint.sh only waits for Kafka and Redis — no PostgreSQL wait.
#   - start-period is 45s — shorter than Payment Service (90s) because
#     there is no Stripe mock warm-up and no database migration step.


# ──────────────────────────────────────────────────────────────────────────────
# STAGE 1 — builder
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS builder

# Prevent Python from writing .pyc bytecode files.
# Keeps the image clean and avoids stale cache issues on rebuild.
ENV PYTHONDONTWRITEBYTECODE=1

# Send Python stdout and stderr directly to the terminal without buffering.
# Essential for a consumer service — seat lock acquisition and release events
# must appear in logs immediately, not held in a buffer.
ENV PYTHONUNBUFFERED=1

WORKDIR /build

# Install build-time system dependencies.
#
# gcc          — C compiler required to build the confluent-kafka C extension
# librdkafka-dev — librdkafka headers and static library for confluent-kafka
# python3-dev  — Python C headers required by any C extension build
#
# Note: No libssl-dev or libffi-dev needed here — unlike the Payment Service,
# the Seat Service makes no outbound HTTPS calls (Redis uses plain TCP in
# local dev, and TLS Redis connections require libssl only at runtime via
# the redis-py driver which handles it in pure Python).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        librdkafka-dev \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements before source code.
# Docker layer cache: pip install only re-runs when requirements.txt changes,
# not on every application code edit — critical for fast iteration on the
# seat lock logic which is the most frequently adjusted code in this service.
COPY requirements.txt .

# Install all Python dependencies into /install.
# --prefix=/install keeps them separate from the builder's own Python
# so Stage 2 can COPY just the compiled packages.
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
# librdkafka1  — shared object (.so) that the compiled confluent-kafka
#                extension dynamically links against at runtime. Without it
#                the consumer crashes on startup with:
#                  ImportError: librdkafka.so.1: cannot open shared object file
#
# No libssl3 or ca-certificates needed — unlike the Payment Service, the
# Seat Service communicates only with Redis (TCP, no TLS in local dev) and
# Kafka (plain PLAINTEXT in local dev). TLS support can be added later by
# including libssl3 here and configuring SSL in the Kafka and Redis clients.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Create a dedicated non-root user.
# The Seat Service manages distributed locks that control seat availability —
# running as non-root limits the blast radius of any container compromise
# that could otherwise manipulate Redis lock state.
RUN addgroup --system appgroup && \
    adduser  --system --ingroup appgroup appuser

WORKDIR /app

# Copy compiled Python packages from builder.
# Only the installed packages are transferred — compilers and build tools
# stay in the builder image and never appear in the runtime layer.
COPY --from=builder /install /usr/local

# Copy application source code.
# Copying app/ last ensures Docker reuses all earlier layers (system packages,
# pip packages) on code-only changes — the most common case during active
# development of the seat locking logic.
COPY app/ ./app/

# Copy the entrypoint script.
# entrypoint.sh for the Seat Service is simpler than other services:
#   1. Wait for Redis to accept connections (no PostgreSQL wait)
#   2. Wait for Kafka broker to be reachable
#   3. Start the Kafka consumer loop
#
# No Alembic migration step — Redis is schema-less and seat lock keys
# are created on demand by the consumer when it processes the first event.
COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

# shared/ is intentionally NOT copied here.
# Mounted at runtime via docker-compose volumes:
#   volumes:
#     - ./shared:/app/shared
# Changes to shared/schemas.py take effect without rebuilding the image.

# Switch to non-root user.
# Must come after all COPY instructions that write to /app as root.
USER appuser

# ── HEALTHCHECK ───────────────────────────────────────────────────────────────
# The Seat Service has no HTTP server so we use the sentinel file pattern.
# The consumer loop touches /tmp/seat_consumer_heartbeat after every
# processed event (lock acquired or lock failed — both count as liveness).
#
# Unique path distinguishes this from Booking Service (/tmp/consumer_heartbeat)
# and Payment Service (/tmp/payment_consumer_heartbeat) so all three can
# run on the same Docker host without the files interfering.
#
# --start-period=45s:
#   Shorter than Payment Service (90s) because there is no DB migration
#   step and no Stripe mock warm-up. Redis and Kafka typically become
#   ready within 30s — the extra 15s accounts for slow Docker host startup.
#
# --interval=30s:
#   Check every 30 seconds after start-period expires.
#
# --timeout=10s:
#   Kill the health check if it hangs for 10 seconds — prevents a stuck
#   health check from blocking the Docker engine's container management.
#
# --retries=3:
#   Mark unhealthy after 3 consecutive failures (~90s of unresponsiveness)
#   before Docker restarts the container. 3 retries avoids false restarts
#   during a Redis reconnect after a brief network blip.
#
# The stale threshold is 60s — higher than the 30s check interval to
# tolerate periods where no booking.requested or payment.failed events
# arrive (the consumer is healthy but idle, not processing any messages).
# If the heartbeat file is not updated for 60s during active traffic,
# that indicates the consumer loop is genuinely stalled.
HEALTHCHECK --interval=30s --timeout=10s --start-period=45s --retries=3 \
    CMD ["sh", "-c", \
         "test -f /tmp/seat_consumer_heartbeat && \
          [ $(( $(date +%s) - $(date +%s -r /tmp/seat_consumer_heartbeat) )) -lt 60 ] \
          || exit 1"]

# Run entrypoint.sh using exec form.
# exec form (JSON array) makes the Python process PID 1 inside the container
# so it receives SIGTERM directly from Docker on `docker compose stop`.
#
# Why is PID 1 critical for the Seat Service?
#   A SIGTERM during seat lock acquisition (between the Redis SET NX and
#   the Kafka publish) must be handled gracefully — the consumer should
#   finish the current lock operation before exiting. If the shell is PID 1
#   and Python is a child process, SIGTERM hits the shell which may kill
#   Python immediately without allowing the current lock operation to complete,
#   leaving a Redis lock in an acquired state with no downstream event published.
#   exec form eliminates this risk by making Python PID 1 directly.
ENTRYPOINT ["./entrypoint.sh"]