# booking-service/app/__init__.py
#
# Marks the app/ directory as a Python package so its modules can be
# imported using dot notation from anywhere inside the container:
#
#   from app.consumer import run_consumer
#   from app.producer import publish_seat_reserved
#   from app.service  import BookingService
#   from app.db       import models, connection
#
# This file is intentionally empty beyond this docstring.
#
# Startup logic (database engine creation, Kafka consumer group
# initialisation, Alembic migration checks) lives in entrypoint.sh
# and app/consumer.py so it runs in the correct order and under the
# correct asyncio event loop — not at import time.
#
# Putting startup logic here would cause it to execute whenever any
# module in the package is imported, including during unit tests, which
# would require a live Kafka broker and PostgreSQL instance just to run
# a simple test. Keeping __init__.py empty avoids that coupling entirely.