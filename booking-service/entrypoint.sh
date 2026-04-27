#!/bin/sh
# booking-service/entrypoint.sh
#
# Runs before the consumer starts on every container boot.
# Handles two pre-flight steps that must succeed before the consumer
# is safe to start:
#   1. Wait for PostgreSQL to be reachable
#   2. Apply any pending Alembic migrations
#
# Using a shell script here rather than putting this logic in consumer.py
# keeps infrastructure concerns (is the DB up?) separate from application
# concerns (process the Kafka message).

set -e  # exit immediately if any command returns a non-zero status

echo "Waiting for PostgreSQL to be ready..."

# Poll PostgreSQL until it accepts connections.
# python3 -c exit() returns 0 on success and 1 on any exception —
# a lightweight alternative to installing pg_isready in the image.
until python3 -c "
import asyncio, asyncpg, os, sys
async def check():
    try:
        conn = await asyncpg.connect(os.environ['POSTGRES_URL'].replace('+asyncpg', ''))
        await conn.close()
    except Exception as e:
        sys.exit(1)
asyncio.run(check())
"; do
  echo "   PostgreSQL not ready - retrying in 2s..."
  sleep 2
done

echo "PostgreSQL is ready"

echo "Running Alembic migrations..."
alembic upgrade head
echo "Migrations applied"

echo "Starting Booking Service consumer..."

# Replace the shell process with the Python consumer using exec.
# This makes Python PID 1 inside the container so it receives
# SIGTERM directly from Docker on `docker compose stop` or `docker compose down`.
exec python3 -m app.consumer