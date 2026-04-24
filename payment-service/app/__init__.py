# payment-service/app/__init__.py
#
# Marks the app/ directory as a Python package so its modules can be
# imported using dot notation from anywhere inside the container:
#
#   from app.consumer import run_consumer
#   from app.producer import publish_payment_processed, publish_payment_failed
#   from app.service  import PaymentService
#   from app.db       import models, connection
#
# This file is intentionally empty beyond this docstring.
#
# Why keep it empty?
#
# The Payment Service has a strict initialisation order that must be
# respected to avoid runtime errors and resource leaks:
#
#   1. app/config.py     → loaded first by every other module. Reads all
#                          environment variables from .env via pydantic-settings
#                          and validates them at import time. If STRIPE_API_KEY
#                          or POSTGRES_URL is missing the process exits here
#                          with a clear error rather than a cryptic AttributeError
#                          later inside a payment transaction.
#
#   2. app/db/connection.py → creates the SQLAlchemy async engine using the
#                             validated settings from config.py. Must come
#                             after config.py so the database URL is available.
#
#   3. app/db/models.py  → defines the Payment ORM model that references the
#                          engine metadata. Must come after connection.py.
#
#   4. app/service.py    → imports the Stripe SDK and configures it with the
#                          API key from config.py. The Stripe client is
#                          initialised lazily inside PaymentService.__init__()
#                          rather than at module import time so that unit tests
#                          can instantiate PaymentService with mock credentials
#                          without triggering a real Stripe API connection.
#
#   5. app/producer.py   → creates the Kafka producer singleton. Must come
#                          after config.py so KAFKA_BOOTSTRAP_SERVERS is set.
#
#   6. app/consumer.py   → the entry point. Imports and orchestrates all of
#                          the above in the correct order inside run_consumer().
#
# Placing any import statements in this file would collapse that explicit
# order into an implicit chain that is hard to trace when something fails.
# For example, if __init__.py imported app.service at package load time,
# the Stripe SDK would be configured before entrypoint.sh has confirmed
# that the STRIPE_API_KEY environment variable is present — causing a
# confusing KeyError deep inside the stripe library rather than a clear
# "STRIPE_API_KEY is required" message from pydantic-settings.
#
# Rule of thumb: if a module does I/O, opens a connection, or reads an
# environment variable, it should be imported explicitly at the call site
# (in consumer.py) rather than implicitly here.
#
# Testing note:
#   Keeping this file empty means individual modules can be imported in
#   isolation during unit tests:
#
#       from app.service import PaymentService   # works without Kafka or DB
#       from app.db.models import Payment        # works without a live engine
#
#   If __init__.py imported everything eagerly, every test file would
#   require a live PostgreSQL instance and Stripe credentials just to
#   import the module under test — making tests slow, fragile, and
#   impossible to run in a CI environment without full infrastructure.