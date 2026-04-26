# notification-service/Dockerfile
#
# Multi-stage build for the Notification Service.
#
# The Notification Service is a Kafka consumer that:
#   - Consumes BookingConfirmedEvent from `booking.confirmed`
#   - Sends confirmation emails via SendGrid (or a mock in local dev)
#   - Optionally sends SMS confirmation via Twilio (or a mock)
#   - Logs every notification attempt for audit purposes
#
# Key differences from other service Dockerfiles:
#
#   No PostgreSQL dependency:
#     The Notification Service is stateless — it fires emails and exits.
#     Notification history is handled by SendGrid's dashboard in production.
#     No SQLAlchemy, no asyncpg, no alembic, no DB wait in entrypoint.sh.
#
#   No Redis dependency:
#     Unlike the Seat Service, the Notification Service holds no distributed
#     state. Each notification is independent and idempotent by nature —
#     sending a duplicate confirmation email is an annoyance, not a data
#     integrity issue, so no locking is required.
#
#   libssl3 and ca-certificates in runtime stage:
#     Like the Payment Service, the Notification Service makes outbound
#     HTTPS calls — to SendGrid's API (api.sendgrid.com) and optionally
#     Twilio's API. Both require TLS certificate verification against
#     current root certificates.
#
#   Prometheus metrics on port 8004:
#     Does not conflict with:
#       Booking Service  → 8001
#       Payment Service  → 8002
#       Seat Service     → 8003
#
#   Shortest start-period (30s):
#     The Notification Service has the simplest startup sequence —
#     no DB migrations, no Redis warmup, no Stripe mock. It only needs
#     Kafka to be reachable. 30s is sufficient for Kafka to be healthy
#     and for the consumer to subscribe and begin polling.
#
#   HEALTHCHECK sentinel path: /tmp/notification_consumer_heartbeat
#     Unique path to avoid collision with other service sentinel files
#     when all containers run on the same Docker host.


# ──────────────────────────────────────────────────────────────────────────────
# STAGE 1 — builder
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS builder

# Prevent Python from writing .pyc bytecode files.
ENV PYTHONDONTWRITEBYTECODE=1

# Send log output directly to the terminal without buffering.
# Every notification send/failure must appear in logs immediately —
# delayed log output during a SendGrid outage makes diagnosis harder.
ENV PYTHONUNBUFFERED=1

WORKDIR /build

# Install build-time system dependencies.
#
# gcc          — C compiler for building the confluent-kafka C extension
# librdkafka-dev — librdkafka headers and static library
# python3-dev  — Python C headers required by C extensions
# libssl-dev   — OpenSSL headers required by the cryptography package
#                used for SendGrid and Twilio HMAC signature verification
# libffi-dev   — FFI headers required by the cryptography package
#
# libssl-dev and libffi-dev are included because the Notification Service
# makes HTTPS calls to external APIs. The cryptography package (pulled in
# by httpx and sendgrid) requires these headers at build time even when
# the actual TLS operations happen at runtime via libssl3.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        librdkafka-dev \
        python3-dev \
        libssl-dev \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements before source code.
# Docker layer caching: pip install only reruns when requirements.txt
# changes — code edits alone do not invalidate this layer, keeping
# incremental build times fast during active development.
COPY requirements.txt .

# Install all Python dependencies into /install for clean Stage 2 copy.
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
# librdkafka1      — shared object that confluent-kafka links against at
#                    runtime. Without it the consumer crashes on startup:
#                      ImportError: librdkafka.so.1: cannot open shared object
#
# libssl3          — OpenSSL runtime library for HTTPS calls to SendGrid
#                    and Twilio. Required by httpx when verifying TLS
#                    certificates on outbound API connections.
#
# ca-certificates  — System CA certificate bundle for TLS verification.
#                    Without this, httpx raises SSL certificate errors when
#                    connecting to api.sendgrid.com or api.twilio.com even
#                    though the certificates are perfectly valid.
#                    The slim base image ships with a minimal CA bundle
#                    that may not include all root CAs used by SendGrid.
#
# Note: Unlike the Seat Service, we DO need libssl3 and ca-certificates
# here because the Notification Service makes outbound HTTPS calls.
# Unlike the Payment Service, we do NOT need curl (no HTTP health probe
# since there is no web server — we use the sentinel file pattern instead).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        librdkafka1 \
        libssl3 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a dedicated non-root user.
# The Notification Service handles PII (passenger names, email addresses,
# phone numbers) and communicates with external email/SMS APIs. Running
# as non-root limits the blast radius if a malicious notification payload
# or a SendGrid SDK vulnerability is exploited.
RUN addgroup --system appgroup && \
    adduser  --system --ingroup appgroup appuser

WORKDIR /app

# Copy compiled Python packages from builder stage.
# Compilers, headers, and build tools stay in the builder image.
COPY --from=builder /install /usr/local

# Copy application source code last — maximises Docker layer cache
# reuse on code-only changes (the most frequent change during development
# of notification templates and SendGrid integration logic).
COPY app/ ./app/

# Copy notification templates directory.
# Email and SMS templates are stored as separate files rather than
# hardcoded strings in service.py so they can be edited without
# modifying Python code — important for marketing teams who need to
# update confirmation email copy without a code review.
#
# Template engine (Jinja2) renders these at runtime with passenger
# and flight details interpolated from the BookingConfirmedEvent.
COPY templates/ ./templates/

# Copy entrypoint script.
# Notification Service entrypoint.sh is the simplest of all services:
#   1. Wait for Kafka broker to be reachable
#   2. Optionally verify SendGrid API key is valid (ping /v3/user/profile)
#   3. Start the Kafka consumer loop
#
# No PostgreSQL wait, no Redis wait, no Alembic migrations.
# SendGrid verification is optional (skipped when SENDGRID_MOCK_ENABLED=true)
# so local development starts instantly without a real SendGrid account.
COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

# shared/ is intentionally NOT copied here.
# Mounted at runtime via docker-compose volumes:
#   volumes:
#     - ./shared:/app/shared
# Changes to shared/schemas.py are picked up without rebuilding the image.

# Switch to non-root user before CMD.
# The appuser has read access to /app (COPY'd files are owned by root
# but world-readable) and write access is not needed — notification
# templates are read-only and there is no local file output.
USER appuser

# ── HEALTHCHECK ───────────────────────────────────────────────────────────────
# Sentinel file pattern — no HTTP server to probe.
# The consumer loop touches /tmp/notification_consumer_heartbeat after
# every processed notification event (success or mock — both count).
#
# --start-period=30s:
#   Shortest start-period in the project — the Notification Service only
#   needs Kafka to be ready (no DB migrations, no Redis warmup). Kafka
#   is typically healthy within 15-20s of `docker compose up`, leaving
#   10s of buffer before health checks begin.
#
# --interval=30s:
#   Check every 30 seconds after start-period. Notification events arrive
#   less frequently than seat lock events (one per completed booking vs
#   one per booking request) so a 60s stale threshold is used rather than
#   the 30s used by the Seat Service.
#
# --timeout=10s:
#   Kill the health check command if it takes longer than 10 seconds.
#   The check is a simple file stat — should complete in milliseconds.
#
# --retries=3:
#   Mark unhealthy after 3 consecutive failures. Allows for up to 90s
#   of low-traffic periods where no booking confirmations are processed
#   before Docker considers the consumer stalled (30s interval * 3 retries
#   after the 60s stale threshold = ~150s total before restart).
#
# 60s stale threshold (vs 30s for Seat Service, 90s for Payment Service):
#   Booking confirmations arrive at lower frequency than seat lock events
#   (only confirmed bookings generate notifications, not all requests).
#   During off-peak hours, the heartbeat file may not be updated for
#   30-60 seconds without the consumer being genuinely stalled — the
#   60s threshold prevents false restarts during quiet periods.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD ["sh", "-c", \
         "test -f /tmp/notification_consumer_heartbeat && \
          [ $(( $(date +%s) - $(date +%s -r /tmp/notification_consumer_heartbeat) )) -lt 60 ] \
          || exit 1"]

# Run entrypoint.sh using exec form (JSON array).
# Makes the Python consumer process PID 1 so it receives SIGTERM directly
# from Docker on `docker compose stop`.
#
# For the Notification Service, clean shutdown on SIGTERM means:
#   - Finish sending the current email/SMS (in-flight API call completes)
#   - Commit the Kafka offset for the current message
#   - Close the Kafka consumer and trigger a broker-side rebalance
#   - Flush any buffered log output
#
# If the shell is PID 1 instead of Python, SIGTERM hits the shell which
# may kill Python immediately mid-SendGrid-API-call — potentially sending
# a notification without committing the Kafka offset. On restart, the same
# booking.confirmed event is re-delivered and the passenger receives a
# duplicate confirmation email. While not catastrophic, duplicate emails
# erode passenger trust — exec form prevents this.
ENTRYPOINT ["./entrypoint.sh"]