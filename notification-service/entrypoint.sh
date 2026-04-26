#!/bin/sh
# notification-service/entrypoint.sh
#
# Pre-flight startup script for the Notification Service.
#
# Startup sequence (simplest of all services — no DB, no Redis):
#   1. Wait for Kafka broker to be reachable
#   2. Verify SendGrid API key (skipped in mock mode)
#   3. Start the Kafka consumer loop
#
# Why no PostgreSQL wait?
#   The Notification Service is stateless — it reads from Kafka,
#   calls SendGrid, and exits. No DB writes, no schema migrations.
#
# Why no Redis wait?
#   Notifications do not require distributed locking or caching.
#   Each email/SMS is independent and idempotent enough that a
#   duplicate (from at-least-once Kafka delivery) is acceptable.

set -e

echo "⏳ Waiting for Kafka broker..."
until python3 -c "
from confluent_kafka.admin import AdminClient
import os, sys
try:
    a = AdminClient({
        'bootstrap.servers': os.environ.get(
            'KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'
        )
    })
    a.list_topics(timeout=5)
except Exception as e:
    print(f'Kafka check failed: {e}', flush=True)
    sys.exit(1)
"; do
  echo "   Kafka not ready — retrying in 3s..."
  sleep 3
done
echo "✅ Kafka broker is reachable"

# Verify SendGrid API key only in production mode.
# In mock mode (SENDGRID_MOCK_ENABLED=true, the default in .env),
# skip the verification so local development starts without a real
# SendGrid account or network access to api.sendgrid.com.
if [ "${SENDGRID_MOCK_ENABLED:-true}" = "false" ]; then
    echo "⏳ Verifying SendGrid API key..."
    until python3 -c "
import httpx, os, sys
try:
    r = httpx.get(
        'https://api.sendgrid.com/v3/user/profile',
        headers={'Authorization': f'Bearer {os.environ[\"SENDGRID_API_KEY\"]}'},
        timeout=10,
    )
    if r.status_code == 200:
        print('SendGrid API key verified', flush=True)
    else:
        print(f'SendGrid returned {r.status_code}', flush=True)
        sys.exit(1)
except Exception as e:
    print(f'SendGrid check failed: {e}', flush=True)
    sys.exit(1)
"; do
      echo "   SendGrid verification failed — retrying in 5s..."
      sleep 5
    done
    echo "✅ SendGrid API key verified"
else
    echo "ℹ️  SENDGRID_MOCK_ENABLED=true — skipping SendGrid verification"
fi

echo "🚀 Starting Notification Service consumer..."

# exec replaces the shell with Python making it PID 1.
# Ensures SIGTERM from Docker is delivered directly to the Python
# process for graceful shutdown — completing in-flight API calls
# before exiting to prevent duplicate notification re-delivery.
exec python3 -m app.consumer