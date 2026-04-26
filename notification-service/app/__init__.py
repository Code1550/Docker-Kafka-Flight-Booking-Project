# notification-service/app/__init__.py
#
# Marks the app/ directory as a Python package so its modules can be
# imported using dot notation from anywhere inside the container:
#
#   from app.consumer import run_consumer
#   from app.service  import NotificationService
#   from app.config   import settings
#
# This file is intentionally empty beyond this docstring.
#
# Why keep it empty?
#
# The Notification Service has the simplest initialisation order of all
# four services because it has no database, no Redis, and no payment SDK
# to configure. Despite this simplicity, the same discipline of keeping
# __init__.py empty applies — and the reasons are worth documenting
# explicitly so future contributors understand the pattern is intentional
# rather than an oversight.
#
# The correct initialisation order enforced by consumer.py is:
#
#   1. app/config.py
#      → Loaded first by every other module. Reads and validates all
#        environment variables via pydantic-settings including:
#          - KAFKA_BOOTSTRAP_SERVERS    (required for consumer)
#          - SENDGRID_API_KEY           (required for email dispatch)
#          - SENDGRID_MOCK_ENABLED      (controls mock vs real dispatch)
#          - SENDGRID_FROM_EMAIL        (sender address for all emails)
#          - SENDGRID_FROM_NAME         (sender display name)
#          - TWILIO_ACCOUNT_SID         (required for SMS, if enabled)
#          - TWILIO_AUTH_TOKEN          (required for SMS, if enabled)
#          - TWILIO_FROM_NUMBER         (E.164 sender number for SMS)
#          - TWILIO_MOCK_ENABLED        (controls mock vs real SMS)
#          - TOPIC_BOOKING_CONFIRMED    (consumer subscription topic)
#          - TOPIC_BOOKING_FAILED_DLQ   (producer DLQ topic)
#          - NOTIFICATION_FROM_EMAIL    (fallback if SENDGRID_FROM_EMAIL unset)
#        A missing SENDGRID_API_KEY raises a clear ValidationError at
#        startup rather than a 403 Forbidden from SendGrid mid-notification.
#
#   2. app/service.py
#      → Defines NotificationService which configures both the SendGrid
#        and Twilio clients inside __init__() rather than at module level.
#        This is the same lazy-initialisation pattern used in
#        payment-service/app/service.py for the Stripe SDK — it allows
#        NotificationService to be imported in unit tests without triggering
#        real API client initialisation (which would require valid credentials
#        and potentially make network calls during import).
#
#        NotificationService also initialises the Jinja2 environment inside
#        __init__() — loading templates from the templates/ directory and
#        compiling them into Jinja2's bytecode cache. Template compilation
#        is done once at startup rather than on every notification so the
#        rendering hot path (called for every booking.confirmed event) only
#        performs template variable substitution, not full re-compilation.
#
#   3. app/consumer.py
#      → The entry point. Orchestrates the full startup sequence in
#        run_consumer():
#          a. Start Prometheus metrics server on port 8004
#          b. Instantiate NotificationService (configures SendGrid,
#             Twilio, and Jinja2 environment)
#          c. Build the Kafka consumer and subscribe to booking.confirmed
#          d. Create the initial heartbeat sentinel file
#          e. Run the poll loop until SIGTERM
#          f. Flush any pending log output and close the consumer cleanly
#
#        Note: There is no separate producer.py in the Notification Service.
#        The Notification Service is a pure consumer — it only produces
#        DLQ events on error, which are handled by a thin inline producer
#        inside consumer.py rather than a dedicated module. This reflects
#        the service's narrow responsibility: consume, notify, done.
#
# Why is the Notification Service simpler than the others?
#
#   The Notification Service sits at the END of the event chain:
#
#     API Gateway
#       → booking.requested
#         → Booking Service + Seat Service (parallel)
#           → seat.reserved
#             → Payment Service
#               → payment.processed
#                 → Booking Service
#                   → booking.confirmed
#                     → Notification Service  ← HERE
#
#   By the time booking.confirmed arrives, every upstream service has
#   already done its job correctly:
#     - The booking exists in PostgreSQL (Booking Service)
#     - The seat is locked in Redis (Seat Service)
#     - The card has been charged (Payment Service)
#     - The booking status is CONFIRMED in PostgreSQL (Booking Service)
#
#   The Notification Service's only job is to tell the passenger. This
#   narrow responsibility is why it needs no database, no cache, and no
#   payment SDK — and why its __init__.py, like all others, stays empty.
#
# Idempotency note:
#   Unlike the other services where idempotency is critical (duplicate DB
#   writes, double charges), a duplicate notification is relatively benign —
#   the passenger receives two confirmation emails. The Notification Service
#   handles at-least-once Kafka delivery by:
#     1. Logging the SendGrid message_id for every sent email — if the same
#        booking_id generates two log entries with different message_ids,
#        support can identify and apologise for the duplicate.
#     2. Optionally implementing a lightweight idempotency check using
#        SendGrid's message_id stored in a short-lived Redis key (TTL=1h).
#        This is NOT implemented in the current project (no Redis dependency)
#        but is the recommended production pattern.
#
# Testing note:
#   Keeping this file empty means service.py is importable without valid
#   SendGrid or Twilio credentials:
#
#       # Works in tests — no API calls made at import time
#       from app.service import NotificationService
#
#       # Test with mock=True to skip real API calls
#       service = NotificationService(mock=True)
#       result  = await service.send_confirmation(event=mock_event)
#       assert result["email_sent"] is True
#
#   If __init__.py eagerly initialised NotificationService at import time,
#   every test would require valid SendGrid credentials and network access
#   to api.sendgrid.com — making tests impossible to run offline or in
#   CI environments without production API keys.