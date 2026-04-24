# payment-service/app/db/__init__.py
#
# Marks the db/ directory as a Python package so its modules can be
# imported using dot notation from anywhere inside the container:
#
#   from app.db.connection import get_db_session, init_db, close_db
#   from app.db.models     import Payment
#
# This file is intentionally empty beyond this docstring.
#
# Why keep it empty?
#
# The Payment Service database package has the same strict initialisation
# order as the Booking Service, with one additional concern — the Stripe
# SDK must never be initialised before the database engine is ready,
# because a successful Stripe charge that cannot be written to PostgreSQL
# is the worst possible failure mode (passenger charged, no record exists).
#
# The correct initialisation order enforced by consumer.py is:
#
#   1. app/config.py          → validates all environment variables at import
#                               time including POSTGRES_URL, STRIPE_API_KEY,
#                               and STRIPE_MOCK_ENABLED. A missing variable
#                               raises a clear ValidationError here rather than
#                               an AttributeError inside a payment transaction.
#
#   2. app/db/connection.py   → creates the SQLAlchemy async engine and
#                               sessionmaker using the validated POSTGRES_URL
#                               from config.py. Must come before models.py
#                               because the engine's metadata object is
#                               referenced by the ORM mapped classes.
#
#   3. app/db/models.py       → defines the Payment ORM model that maps to
#                               the payments table. Must come after
#                               connection.py so Base.metadata is fully
#                               configured before Alembic reads it for
#                               migration autogeneration.
#
#   4. app/service.py         → imports the stripe SDK and calls
#                               stripe.api_key = settings.STRIPE_API_KEY
#                               inside PaymentService.__init__(). Initialised
#                               after the DB layer so that if the DB engine
#                               fails to configure, the Stripe client is never
#                               set up and no charges can be accidentally
#                               attempted against a broken DB state.
#
#   5. app/producer.py        → creates the Kafka producer singleton using
#                               KAFKA_BOOTSTRAP_SERVERS from config.py.
#                               Must come after the DB layer so that if a
#                               payment is processed but the Kafka publish
#                               fails, the DB record already exists and the
#                               consumer can safely retry without risk of
#                               a double charge (idempotency check in
#                               service.py will find the existing record).
#
#   6. app/consumer.py        → the entry point. Imports and orchestrates
#                               all of the above in the correct order inside
#                               run_consumer(). This is the only module that
#                               should import from all the others.
#
# Why does initialisation order matter more here than in other services?
#
#   In the Booking Service, the worst failure mode is a duplicate DB record —
#   caught and ignored by the idempotency check, no money changes hands.
#
#   In the Payment Service, the worst failure mode is:
#     1. Stripe charges the card  ✓
#     2. DB write fails           ✗  ← no record of the charge
#     3. Kafka publish fails      ✗  ← booking never confirmed
#
#   This "phantom charge" scenario (money taken, booking not confirmed) is
#   the highest-severity failure in the system. The initialisation order
#   above, combined with the idempotency check in service.py and the
#   Stripe idempotency key, provides three independent layers of protection:
#
#     Layer 1 — DB check before Stripe call (service.py)
#               If a Payment record exists → skip Stripe, return cached result
#
#     Layer 2 — Stripe idempotency key = booking_id
#               Even if two threads race past Layer 1 simultaneously,
#               Stripe deduplicates and returns the same PaymentIntent
#
#     Layer 3 — DB write before Kafka publish (service.py)
#               Payment record is persisted before any downstream event
#               is published, so the record always exists if a consumer
#               retries after a Kafka publish failure
#
# Testing note:
#   Keeping this file empty means the DB modules can be imported in
#   isolation during unit tests:
#
#       from app.db.models import Payment        # no engine required
#       from app.db.connection import init_db    # no Stripe required
#
#   A populated __init__.py that eagerly imported service.py would
#   require valid Stripe credentials just to import the DB models —
#   making unit tests impossible to run without full infrastructure.