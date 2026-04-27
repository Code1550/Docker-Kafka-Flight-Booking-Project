# booking-service/Dockerfile
#
# Multi-stage build for the Booking Service.
#
# The Booking Service has no HTTP server - it is a long-running asyncio
# process that consumes from Kafka and writes to PostgreSQL. The Dockerfile
# reflects this: there is no EXPOSE instruction and the CMD runs the consumer
# loop directly rather than starting a web server.
#
# Stage 1 (builder) - installs Python dependencies in isolation
# Stage 2 (runtime) - copies only what is needed to run, keeping the
#                     final image lean and free of compilers and build tools


# ──────────────────────────────────────────────────────────────────────────────
# STAGE 1 - builder
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS builder

# Prevents Python from writing .pyc bytecode files to disk.
# Keeps the image clean and avoids stale cache issues between rebuilds.
ENV PYTHONDONTWRITEBYTECODE=1

# Forces Python stdout and stderr to be unbuffered.
# Critical for a consumer service - log lines appear in `docker compose logs`
# immediately rather than being held in a buffer until the process flushes.
ENV PYTHONUNBUFFERED=1

WORKDIR /build

# Install system-level build dependencies required by confluent-kafka.
# confluent-kafka is a C extension that wraps librdkafka. The slim base image
# ships without a C compiler or the Kafka C headers, so I install them here
# in the builder stage only - they are NOT copied to the runtime stage,
# keeping the final image small.
#
# gcc        - C compiler required to build the confluent-kafka extension
# librdkafka-dev - librdkafka headers and static library
# python3-dev - Python C headers required by any C extension build
# --no-install-recommends - skips optional packages to minimise layer size
# Clean up apt cache in the same RUN layer to avoid bloating the image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        librdkafka-dev \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements before source code.
# Docker layer caching re-uses the pip install layer on every build unless
# requirements.txt actually changes - code edits alone do not trigger
# a full reinstall, saving significant build time during development.
COPY requirements.txt .

# Install all Python dependencies into /install.
# --prefix=/install  → installs into a custom directory so the runtime
#                      stage can COPY just this directory, not the whole
#                      builder filesystem.
# --no-cache-dir     → skips the pip download cache inside the image layer.
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefix=/install -r requirements.txt


# ──────────────────────────────────────────────────────────────────────────────
# STAGE 2 - runtime
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install only the runtime shared library that confluent-kafka needs at
# runtime (not the -dev headers used for compilation).
# librdkafka1 is the shared object (.so) file that the compiled
# confluent-kafka extension dynamically links against when it runs.
# Without this the consumer crashes on startup with:
#   ImportError: librdkafka.so.1: cannot open shared object file
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user and group.
# The Booking Service runs with elevated trust (database write access,
# Kafka consumer group membership) so minimising OS-level privileges
# limits the blast radius of any container escape vulnerability.
RUN addgroup --system appgroup && \
    adduser  --system --ingroup appgroup appuser

WORKDIR /app

# Copy installed Python packages from the builder stage.
# Only the compiled packages are transferred - the C compiler, headers,
# and build tools stay behind in the builder image.
COPY --from=builder /install /usr/local

# Copy the application source code.
# Copying app/ last means Docker can reuse all earlier layers (apt install,
# pip install) when only application code changes - the most common case
# during active development.
COPY app/ ./app/

# Copy the Alembic migration configuration and migration files.
# The Booking Service owns the bookings and flights tables, so database
# migrations live here. The entrypoint script runs `alembic upgrade head`
# before starting the consumer to ensure the schema is always up to date.
COPY alembic.ini    ./alembic.ini
COPY alembic/       ./alembic/

# Copy the entrypoint script that orchestrates startup:
#   1. Wait for PostgreSQL to accept connections
#   2. Run Alembic migrations
#   3. Start the Kafka consumer loop
# Using an entrypoint script keeps CMD clean and makes each startup step
# visible and independently testable.
COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

# shared/ is intentionally NOT copied here.
# It is mounted at runtime via docker-compose:
#   volumes:
#     - ./shared:/app/shared
# This means changes to shared/schemas.py are picked up without a rebuild.

# Switch to non-root user before the process starts.
# Must come after COPY instructions that need root-level write access.
USER appuser

# Health check for the Booking Service.
# Unlike the API Gateway, there is no HTTP server to curl against.
# Instead I check for a sentinel file that the consumer loop creates
# (and updates its mtime) on every successfully processed message.
# If the file is older than 60 seconds the consumer is considered stalled.
#
# The sentinel file approach is a common pattern for health-checking
# long-running worker processes that have no network interface.
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["sh", "-c", \
         "test -f /tmp/consumer_heartbeat && \
          [ $(( $(date +%s) - $(date +%s -r /tmp/consumer_heartbeat) )) -lt 60 ] \
          || exit 1"]

# Run the entrypoint script which handles migrations then starts the consumer.
# Using the exec form (JSON array) so the Python process receives OS signals
# directly rather than through a shell - this ensures SIGTERM triggers a
# clean shutdown (flush Kafka producer, close DB connections) instead of
# being swallowed by the shell wrapper.
ENTRYPOINT ["./entrypoint.sh"]