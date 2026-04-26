# notification-service/app/consumer.py
#
# Main entry point for the Notification Service.
#
# This module runs a long-lived asyncio event loop that:
#   1. Subscribes to the `booking.confirmed` Kafka topic
#   2. Deserializes each message into a BookingConfirmedEvent
#   3. Delegates notification dispatch to NotificationService (service.py)
#   4. Commits the Kafka offset only after the notification is dispatched
#   5. Routes unprocessable messages to the dead-letter queue (DLQ)
#   6. Updates a sentinel file on every processed event for Docker health check
#
# Position in the event chain:
#   booking.confirmed is the LAST topic in the happy path. By the time
#   a message arrives here, the booking is fully confirmed in PostgreSQL,
#   the seat is locked in Redis, and the card has been charged. The
#   Notification Service's only job is to tell the passenger. There are
#   no downstream services to trigger — this consumer produces only DLQ
#   events on error, never normal application events.
#
# Idempotency:
#   Duplicate booking.confirmed delivery (at-least-once Kafka guarantee)
#   results in a duplicate confirmation email — annoying but not a data
#   integrity issue. NotificationService logs the SendGrid message_id for
#   every sent email so duplicates are detectable in logs. A production
#   system would add a Redis-backed idempotency check (booking_id →
#   sendgrid_message_id, TTL=1h) but this is omitted here to keep the
#   Notification Service dependency-free (no Redis client).
#
# Offset management:
#   enable.auto.commit is False. Offsets are committed manually AFTER the
#   notification is dispatched and the SendGrid/Twilio API call confirms
#   acceptance (HTTP 202). If the container crashes after dispatch but
#   before the offset commit, the same booking.confirmed is re-delivered
#   and the passenger receives a duplicate email — the acceptable failure
#   mode for a notification service (vs. missing the notification entirely
#   which would happen with auto-commit before dispatch).

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from pythonjsonlogger import jsonlogger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
    before_sleep_log,
)

from app.config import settings
from app.service import NotificationService
from shared.schemas import (
    BookingConfirmedEvent,
    BookingFailedDLQEvent,
    deserialize_event,
    serialize_event,
)


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# Structured JSON logging — every notification log line includes booking_id,
# correlation_id, passenger_email, notification_type, sendgrid_message_id,
# and dispatch_duration_ms as structured fields for Grafana Loki queries.
#
# GDPR audit note:
#   passenger_email is logged here deliberately — notification audit trails
#   ("when was passenger X notified of booking Y?") require the email address
#   to be queryable. In production, implement log field-level encryption or
#   pseudonymisation for PII fields to comply with GDPR Article 32.
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("notification-service.consumer")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(
    jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
)
logger.addHandler(handler)


# ──────────────────────────────────────────────────────────────────────────────
# PROMETHEUS METRICS
# Exposed on port 8004. Prometheus scrapes every 15 seconds.
# The notifications_sent_total{status="success"} counter is the primary
# SLI — alert when it stops incrementing during active booking periods.
# ──────────────────────────────────────────────────────────────────────────────

# Total notifications dispatched broken down by type and outcome
NOTIFICATIONS_SENT_TOTAL = Counter(
    "notification_service_sent_total",
    "Total notifications dispatched by type and outcome",
    ["notification_type", "status"],
    # notification_type: "email" | "sms" | "both"
    # status:            "success" | "failed" | "mock" | "dlq"
)

# End-to-end dispatch latency including SendGrid/Twilio API call
NOTIFICATION_DISPATCH_DURATION = Histogram(
    "notification_service_dispatch_duration_seconds",
    "Time taken to dispatch one notification end-to-end",
    ["notification_type"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
    # Upper buckets reach 30s because SendGrid can be slow during
    # high-volume periods and Twilio SMS delivery occasionally takes
    # several seconds for international numbers.
)

# Jinja2 template rendering time — should be sub-millisecond after warmup
TEMPLATE_RENDER_DURATION = Histogram(
    "notification_service_template_render_duration_seconds",
    "Time taken to render email and SMS templates via Jinja2",
    buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05],
)

# SendGrid-specific error counter for alerting on specific error codes
SENDGRID_ERRORS_TOTAL = Counter(
    "notification_service_sendgrid_errors_total",
    "Total SendGrid API errors by HTTP status code",
    ["status_code"],    # labels: "400" | "401" | "429" | "500" | "503"
)

# Twilio-specific error counter
TWILIO_ERRORS_TOTAL = Counter(
    "notification_service_twilio_errors_total",
    "Total Twilio API errors by error code",
    ["error_code"],
)

# DLQ counter — alert when this rises
DLQ_MESSAGES_TOTAL = Counter(
    "notification_service_dlq_messages_total",
    "Total messages routed to the dead-letter queue",
)

# Total messages consumed broken down by outcome
MESSAGES_CONSUMED_TOTAL = Counter(
    "notification_service_messages_consumed_total",
    "Total Kafka messages consumed",
    ["status"],     # "success" | "dlq" | "deserialization_error"
)

# SendGrid health gauge — 1=healthy, 0=degraded
SENDGRID_HEALTH = Gauge(
    "notification_service_sendgrid_healthy",
    "1 if SendGrid API is reachable and accepting requests, 0 if not",
)

# Twilio health gauge
TWILIO_HEALTH = Gauge(
    "notification_service_twilio_healthy",
    "1 if Twilio API is reachable and accepting requests, 0 if not",
)


# ──────────────────────────────────────────────────────────────────────────────
# SENTINEL FILE
# Unique path distinguishes this from other service heartbeat files.
# Touched after every processed notification regardless of dispatch outcome.
# Docker HEALTHCHECK reads modification time — stale > 60s triggers restart.
# ──────────────────────────────────────────────────────────────────────────────

HEARTBEAT_FILE = Path("/tmp/notification_consumer_heartbeat")


def _update_heartbeat() -> None:
    """Touch sentinel file to signal the consumer is alive and processing."""
    HEARTBEAT_FILE.touch(exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# GRACEFUL SHUTDOWN
# SIGTERM fires on `docker compose stop`. The handler sets a flag that
# the poll loop checks between messages. In-flight SendGrid/Twilio API
# calls always complete before shutdown — preventing the passenger from
# receiving a partial notification or missing one entirely.
# ──────────────────────────────────────────────────────────────────────────────

_shutdown_requested: bool = False


def _handle_shutdown(signum, frame) -> None:
    """
    Signal handler for SIGTERM and SIGINT.

    Sets the shutdown flag. The poll loop checks this flag only between
    messages — never mid-dispatch — so in-flight SendGrid/Twilio API
    calls always complete before the process exits.

    Why this matters:
      SendGrid's Web API returns 202 Accepted immediately and processes
      the email asynchronously. The 202 response is what we commit the
      Kafka offset against. If SIGTERM fires between the SendGrid call
      and the 202 response, the passenger's email is queued in SendGrid
      but the offset is not committed — on restart, the booking.confirmed
      is re-delivered and a duplicate email is queued. Completing the
      current dispatch before checking the shutdown flag minimises this
      window to near zero.
    """
    global _shutdown_requested
    logger.info(
        "Shutdown signal received — will exit after current notification completes",
        extra={"signal": signal.Signals(signum).name},
    )
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT,  _handle_shutdown)


# ──────────────────────────────────────────────────────────────────────────────
# KAFKA CONSUMER FACTORY
# ──────────────────────────────────────────────────────────────────────────────

def _build_consumer() -> Consumer:
    """
    Construct a configured confluent-kafka Consumer for the Notification Service.

    Configuration differences from other service consumers:

    max.poll.interval.ms = 300_000 (5 minutes):
      Higher than Seat Service (2 minutes) because SendGrid and Twilio
      API calls can occasionally take 20-30 seconds during degraded
      conditions, and the consumer's retry loop (3 attempts with backoff)
      can add up to 60 additional seconds. A 5-minute poll interval gives
      enough headroom for the retry budget without triggering a false
      rebalance during a slow SendGrid API response.

    fetch.min.bytes = 1:
      Low latency — notification confirmations should reach passengers as
      quickly as possible after their booking is confirmed. The Notification
      Service is the last step in the chain and any delay here is
      passenger-visible ("I confirmed my booking 5 minutes ago but haven't
      received an email yet").

    session.timeout.ms = 30_000:
      Standard 30s session timeout — SendGrid calls are async (fire and
      get 202) so heartbeats should not be missed during normal operation.
    """
    config = {
        "bootstrap.servers":      settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id":               settings.KAFKA_CONSUMER_GROUP,

        # Start from earliest — a missed notification means a confirmed
        # passenger never received their email. Better to re-send a
        # duplicate (which we log) than to miss the notification entirely.
        "auto.offset.reset":      "earliest",

        # Manual commit — offset advances only after successful dispatch
        "enable.auto.commit":     False,

        # Generous poll interval to accommodate SendGrid/Twilio latency
        "max.poll.interval.ms":   300_000,   # 5 minutes

        # Standard heartbeat and session settings
        "heartbeat.interval.ms":  3_000,
        "session.timeout.ms":     30_000,

        # Low fetch minimum for fast notification delivery
        "fetch.min.bytes":        1,
        "fetch.wait.max.ms":      250,    # 250ms — lower than other services

        # Standard per-partition cap
        "max.partition.fetch.bytes": 1_048_576,
    }

    return Consumer(config)


# ──────────────────────────────────────────────────────────────────────────────
# DLQ PRODUCER
# Inline producer — the Notification Service produces only DLQ events,
# never normal application events. A dedicated producer.py module is not
# warranted for a single error-path topic.
# ──────────────────────────────────────────────────────────────────────────────

from confluent_kafka import Producer as _RawProducer

_dlq_producer: _RawProducer | None = None


def _get_dlq_producer() -> _RawProducer:
    """Lazy singleton DLQ producer — created on first error."""
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = _RawProducer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "acks":              "all",
                "retries":           3,
                "retry.backoff.ms":  500,
            }
        )
    return _dlq_producer


def _send_to_dlq(
    raw_payload: str,
    error_message: str,
    booking_id: str | None,
    retry_count: int,
) -> None:
    """
    Publish an unprocessable notification event to the dead-letter queue.

    Called when:
      - A message cannot be deserialized (malformed JSON, unknown schema)
      - SendGrid and Twilio both fail after all retries are exhausted
      - A passenger email address is invalid (bounced on first attempt)

    Unlike other services, notification DLQ events do not indicate a
    financial or data integrity issue — they indicate a passenger who
    was not notified of their confirmed booking. The DLQ event should
    trigger a support workflow: "contact passenger X by phone to confirm
    booking Y was successful". This is worth wiring to a PagerDuty
    alert or a support ticketing system in production.

    Args:
        raw_payload:   Original raw Kafka message bytes as a string.
        error_message: Human-readable description of why dispatch failed.
        booking_id:    The booking_id from the event (None if deserialization
                       failed before we could extract it).
        retry_count:   Number of dispatch attempts made before giving up.
    """
    dlq_event = BookingFailedDLQEvent(
        correlation_id="unknown",
        booking_id=booking_id,
        failed_topic=settings.TOPIC_BOOKING_CONFIRMED,
        error_message=error_message,
        raw_payload=raw_payload,
        retry_count=retry_count,
    )

    producer = _get_dlq_producer()
    producer.produce(
        topic=settings.TOPIC_BOOKING_FAILED_DLQ,
        key=(booking_id or "unknown").encode("utf-8"),
        value=serialize_event(dlq_event).encode("utf-8"),
        headers=[
            ("event_type",   b"booking.failed.dlq"),
            ("service",      b"notification-service"),
            ("failed_topic", settings.TOPIC_BOOKING_CONFIRMED.encode("utf-8")),
        ],
    )

    remaining = producer.flush(timeout=10)
    DLQ_MESSAGES_TOTAL.inc()

    if remaining > 0:
        # Cannot write to DLQ — passenger notification permanently lost.
        # Log at CRITICAL so support is paged immediately.
        logger.critical(
            "CRITICAL: Failed to write notification event to DLQ — "
            "passenger will not be notified of confirmed booking",
            extra={
                "booking_id":    booking_id,
                "error_message": error_message,
            },
        )
    else:
        logger.error(
            "Notification event routed to DLQ — passenger may not be notified",
            extra={
                "booking_id":    booking_id,
                "error_message": error_message,
                "retry_count":   retry_count,
            },
        )


# ──────────────────────────────────────────────────────────────────────────────
# MESSAGE PROCESSOR
# Handles a single booking.confirmed Kafka message end-to-end.
# Separated from the poll loop so it can be unit-tested without Kafka.
# ──────────────────────────────────────────────────────────────────────────────

async def _process_message(
    msg: Message,
    notification_service: NotificationService,
) -> bool:
    """
    Deserialize, validate, and dispatch one booking.confirmed message.

    Returns:
        True  — notification dispatched (or mock dispatched), offset safe
                to commit.
        False — message routed to DLQ after permanent failure, offset
                should still be committed to unblock the partition.

    Never raises — all exceptions are caught internally. An unhandled
    exception in _process_message would propagate to the poll loop and
    crash the consumer, stopping ALL notification processing until restart.
    """
    raw   = msg.value().decode("utf-8") if msg.value() else ""
    topic = msg.topic()

    # ── STEP 1: Deserialize ───────────────────────────────────────────────────
    # Deserialization failure is a permanent error — a malformed payload
    # will never become valid. Route to DLQ immediately without retrying.
    try:
        event: BookingConfirmedEvent = deserialize_event(topic, raw)
    except Exception as exc:
        logger.error(
            "Failed to deserialize booking.confirmed message",
            extra={
                "topic":  topic,
                "offset": msg.offset(),
                "error":  str(exc),
                "raw":    raw[:500],
            },
        )
        MESSAGES_CONSUMED_TOTAL.labels(status="deserialization_error").inc()
        _send_to_dlq(
            raw_payload=raw,
            error_message=f"Deserialization failed: {exc}",
            booking_id=None,
            retry_count=0,
        )
        return False

    booking_id         = event.booking_id
    correlation_id     = event.correlation_id
    notification_type  = event.notification_type.value
    passenger_email    = event.passenger.email
    passenger_name     = event.passenger.passenger_full_name \
                         if hasattr(event.passenger, "passenger_full_name") \
                         else f"{event.passenger.first_name} {event.passenger.last_name}"

    logger.info(
        "Processing booking.confirmed — dispatching notification",
        extra={
            "booking_id":        booking_id,
            "correlation_id":    correlation_id,
            "notification_type": notification_type,
            "passenger_email":   passenger_email,
            "flight_number":     event.flight.flight_number,
            "origin":            event.flight.origin,
            "destination":       event.flight.destination,
        },
    )

    # ── STEP 2: Dispatch notification with retry ───────────────────────────────
    # Transient errors (SendGrid rate limit, Twilio 5xx, network timeout)
    # are retried with exponential backoff. Permanent errors (invalid email
    # address, Twilio invalid phone number) go to DLQ immediately — retrying
    # a bounced email wastes SendGrid quota and delays detection of the issue.
    MAX_RETRIES = settings.NOTIFICATION_RETRY_ATTEMPTS   # default: 3

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with NOTIFICATION_DISPATCH_DURATION.labels(
                notification_type=notification_type
            ).time():
                result = await notification_service.send_confirmation(
                    event=event,
                )

            # result dict returned by NotificationService:
            # {
            #   "email_sent":          bool,
            #   "sms_sent":            bool,
            #   "sendgrid_message_id": str | None,
            #   "twilio_sid":          str | None,
            #   "mock":                bool,
            #   "notification_type":   str,
            # }

            # ── SUCCESS ───────────────────────────────────────────────────────
            status = "mock" if result.get("mock") else "success"

            NOTIFICATIONS_SENT_TOTAL.labels(
                notification_type=notification_type,
                status=status,
            ).inc()
            SENDGRID_HEALTH.set(1)
            TWILIO_HEALTH.set(1)
            MESSAGES_CONSUMED_TOTAL.labels(status="success").inc()
            _update_heartbeat()

            logger.info(
                "Notification dispatched successfully",
                extra={
                    "booking_id":          booking_id,
                    "correlation_id":      correlation_id,
                    "notification_type":   notification_type,
                    "passenger_email":     passenger_email,
                    "sendgrid_message_id": result.get("sendgrid_message_id"),
                    "twilio_sid":          result.get("twilio_sid"),
                    "mock":                result.get("mock", False),
                    "attempt":             attempt,
                },
            )
            return True

        except Exception as exc:
            # ── CLASSIFY ERROR ────────────────────────────────────────────────
            # Determine whether the error is retryable or permanent before
            # deciding whether to retry or route to DLQ.
            is_permanent  = _is_permanent_error(exc)
            is_last_attempt = attempt == MAX_RETRIES

            # Update health gauges based on error type
            error_str = str(exc).lower()
            if "sendgrid" in error_str or "email" in error_str:
                SENDGRID_HEALTH.set(0)
                SENDGRID_ERRORS_TOTAL.labels(
                    status_code=_extract_status_code(exc)
                ).inc()
            if "twilio" in error_str or "sms" in error_str:
                TWILIO_HEALTH.set(0)
                TWILIO_ERRORS_TOTAL.labels(
                    error_code=_extract_error_code(exc)
                ).inc()

            logger.warning(
                "Error dispatching notification",
                extra={
                    "booking_id":        booking_id,
                    "correlation_id":    correlation_id,
                    "notification_type": notification_type,
                    "passenger_email":   passenger_email,
                    "attempt":           attempt,
                    "max_retries":       MAX_RETRIES,
                    "error":             str(exc),
                    "is_permanent":      is_permanent,
                    "will_retry":        not is_permanent and not is_last_attempt,
                },
            )

            if is_permanent or is_last_attempt:
                # ── PERMANENT FAILURE OR RETRIES EXHAUSTED → DLQ ─────────────
                NOTIFICATIONS_SENT_TOTAL.labels(
                    notification_type=notification_type,
                    status="dlq",
                ).inc()
                MESSAGES_CONSUMED_TOTAL.labels(status="dlq").inc()

                _send_to_dlq(
                    raw_payload=raw,
                    error_message=(
                        f"Notification dispatch failed "
                        f"({'permanent error' if is_permanent else f'after {MAX_RETRIES} attempts'}): "
                        f"{exc}"
                    ),
                    booking_id=booking_id,
                    retry_count=attempt,
                )
                _update_heartbeat()
                return False

            # ── TRANSIENT ERROR → RETRY WITH BACKOFF ──────────────────────────
            # Jitter prevents thundering herd when SendGrid has an outage
            # and all notification consumers retry simultaneously.
            import random
            backoff = min(2 ** attempt, 30) + random.uniform(0, 2)
            logger.info(
                f"Retrying notification dispatch in {backoff:.1f}s",
                extra={
                    "booking_id": booking_id,
                    "attempt":    attempt,
                    "backoff_s":  round(backoff, 1),
                },
            )
            await asyncio.sleep(backoff)

    # Unreachable — loop always returns inside the for block
    return False


# ──────────────────────────────────────────────────────────────────────────────
# ERROR CLASSIFICATION HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _is_permanent_error(exc: Exception) -> bool:
    """
    Determine whether a notification dispatch error is permanent (no retry)
    or transient (should be retried with backoff).

    Permanent errors — retrying will not fix them:
      - Invalid email address (bounced)            → SendGrid 400
      - Invalid phone number (Twilio 21211)        → Twilio error
      - Unsubscribed recipient (SendGrid suppression) → SendGrid 400
      - Invalid API key (both SendGrid and Twilio) → 401/403

    Transient errors — retrying may fix them:
      - Rate limit exceeded                         → SendGrid 429
      - Internal server error                       → SendGrid/Twilio 5xx
      - Network timeout                             → ConnectionError
      - DNS resolution failure (temporary)          → ConnectionError

    Args:
        exc: The exception raised by NotificationService.send_confirmation().

    Returns:
        True  if the error is permanent — route to DLQ immediately.
        False if the error is transient — retry with backoff.
    """
    error_str    = str(exc).lower()
    error_class  = type(exc).__name__

    # Permanent HTTP status codes from SendGrid / Twilio
    permanent_status_codes = {"400", "401", "403", "404", "422"}
    for code in permanent_status_codes:
        if code in error_str:
            return True

    # Twilio permanent error codes
    # 21211 = invalid 'To' phone number
    # 21614 = 'To' number is not a mobile number
    # 21610 = message cannot be sent to opted-out number
    permanent_twilio_codes = {"21211", "21614", "21610", "21408"}
    for code in permanent_twilio_codes:
        if code in error_str:
            return True

    # SendGrid bounce and suppression indicators
    permanent_sendgrid_indicators = [
        "invalid email",
        "email address",
        "unsubscribed",
        "bounced",
        "spam report",
        "invalid_email_address",
    ]
    for indicator in permanent_sendgrid_indicators:
        if indicator in error_str:
            return True

    # Authentication errors are permanent — wrong API key will not fix itself
    if "authentication" in error_str or "unauthorized" in error_str:
        return True

    # Everything else is treated as transient — retry with backoff
    return False


def _extract_status_code(exc: Exception) -> str:
    """
    Extract the HTTP status code from a SendGrid exception for Prometheus labels.

    Returns "unknown" if no status code can be extracted — prevents
    Prometheus from rejecting the metric observation due to an unexpected
    label value.

    Args:
        exc: The exception raised during SendGrid API call.

    Returns:
        HTTP status code as a string e.g. "429", or "unknown".
    """
    error_str = str(exc)
    for code in ["400", "401", "403", "404", "422", "429", "500", "502", "503"]:
        if code in error_str:
            return code
    return "unknown"


def _extract_error_code(exc: Exception) -> str:
    """
    Extract the Twilio error code from a Twilio exception for Prometheus labels.

    Twilio error codes are 5-digit integers embedded in exception messages
    e.g. "Error 21211: The 'To' number +14155552671 is not a valid..."

    Returns "unknown" if no error code can be extracted.

    Args:
        exc: The exception raised during Twilio API call.

    Returns:
        Twilio error code as a string e.g. "21211", or "unknown".
    """
    import re
    error_str = str(exc)
    match = re.search(r"\b2\d{4}\b", error_str)   # Twilio codes: 20000-29999
    return match.group(0) if match else "unknown"


# ──────────────────────────────────────────────────────────────────────────────
# CONSUMER LOOP
# ──────────────────────────────────────────────────────────────────────────────

@retry(
    # Retry the poll loop on KafkaException (broker restart, network blip).
    # After 10 attempts the infrastructure problem is severe enough to warrant
    # a container restart so Docker can apply its restart policy.
    stop=stop_after_attempt(10),
    wait=wait_exponential_jitter(initial=2, max=60, jitter=2),
    retry=retry_if_exception_type(KafkaException),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def _run_consumer_loop(
    consumer: Consumer,
    notification_service: NotificationService,
) -> None:
    """
    Core poll loop — runs until shutdown signal or fatal KafkaException.

    poll() timeout is 1.0s — higher than the Seat Service (0.5s) because
    booking.confirmed events arrive less frequently than booking.requested
    events (only confirmed bookings, not all requests). A 1s timeout is
    an acceptable delay between message availability and processing for
    confirmation emails — passengers expect emails within seconds of
    booking, not milliseconds.
    """
    loop = asyncio.get_event_loop()

    logger.info(
        "Notification Service consumer loop started",
        extra={
            "topic":          settings.TOPIC_BOOKING_CONFIRMED,
            "consumer_group": settings.KAFKA_CONSUMER_GROUP,
            "mock_mode":      settings.SENDGRID_MOCK_ENABLED,
        },
    )

    while not _shutdown_requested:

        # ── POLL ──────────────────────────────────────────────────────────────
        msg: Message | None = await loop.run_in_executor(
            None,
            lambda: consumer.poll(timeout=1.0),
        )

        # ── NO MESSAGE ────────────────────────────────────────────────────────
        if msg is None:
            continue

        # ── KAFKA ERRORS ──────────────────────────────────────────────────────
        if msg.error():
            error = msg.error()

            if error.code() == KafkaError._PARTITION_EOF:
                # All current booking.confirmed events consumed — caught up.
                # During quiet periods this fires frequently — log at DEBUG
                # to avoid flooding logs with harmless EOF messages.
                logger.debug(
                    "End of partition — no new booking confirmations",
                    extra={
                        "topic":     msg.topic(),
                        "partition": msg.partition(),
                        "offset":    msg.offset(),
                    },
                )
                continue

            raise KafkaException(error)

        # ── LOG RECEIPT ───────────────────────────────────────────────────────
        logger.info(
            "Booking confirmed event received — preparing notification",
            extra={
                "topic":          msg.topic(),
                "partition":      msg.partition(),
                "offset":         msg.offset(),
                "key":            msg.key().decode("utf-8") if msg.key() else None,
                "correlation_id": _extract_header(msg, "correlation_id"),
                "notification_type": _extract_header(msg, "notification_type"),
            },
        )

        # ── PROCESS MESSAGE ───────────────────────────────────────────────────
        await _process_message(
            msg=msg,
            notification_service=notification_service,
        )

        # ── COMMIT OFFSET ─────────────────────────────────────────────────────
        # Commit after dispatch regardless of success or DLQ routing.
        # A committed DLQ event means the DLQ topic has the original payload —
        # the booking.confirmed partition can advance safely.
        #
        # asynchronous=False — wait for broker acknowledgement before
        # polling the next message. Notification events are low-volume
        # so the synchronous commit overhead is negligible.
        await loop.run_in_executor(
            None,
            lambda: consumer.commit(message=msg, asynchronous=False),
        )

        logger.debug(
            "Offset committed",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
            },
        )

    logger.info(
        "Shutdown flag set — exiting Notification Service consumer loop cleanly"
    )


# ──────────────────────────────────────────────────────────────────────────────
# PARTITION CALLBACKS
# ──────────────────────────────────────────────────────────────────────────────

def _on_partition_assign(consumer: Consumer, partitions: list) -> None:
    """
    Called when partitions are assigned during a rebalance.

    For the Notification Service, a rebalance pauses email dispatch
    briefly. Log clearly so engineers can correlate "passenger didn't
    get email immediately" complaints with rebalance events.
    """
    logger.info(
        "Partitions assigned to Notification Service consumer",
        extra={
            "partitions": [
                {
                    "topic":     p.topic,
                    "partition": p.partition,
                    "offset":    p.offset,
                }
                for p in partitions
            ]
        },
    )


def _on_partition_revoke(consumer: Consumer, partitions: list) -> None:
    """
    Called when partitions are revoked during a rebalance.

    Any in-flight SendGrid or Twilio API call in progress when this fires
    will complete because the shutdown flag is checked only between messages.
    After revocation another consumer instance takes over.
    """
    logger.warning(
        "Partitions revoked from Notification Service — rebalance in progress",
        extra={
            "partitions": [
                {"topic": p.topic, "partition": p.partition}
                for p in partitions
            ]
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# HELPER UTILITIES
# ──────────────────────────────────────────────────────────────────────────────

def _extract_header(msg: Message, key: str) -> str | None:
    """
    Extract a single string value from Kafka message headers by key.

    Used to read correlation_id and notification_type for logging without
    deserializing the full JSON payload — avoids a full JSON parse just
    to log the message receipt line.

    Returns None if the header is absent rather than raising KeyError.
    """
    if not msg.headers():
        return None
    for k, v in msg.headers():
        if k == key:
            return v.decode("utf-8") if v else None
    return None


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

async def run_consumer() -> None:
    """
    Bootstrap the Notification Service consumer:
      1. Start Prometheus metrics server on port 8004
      2. Instantiate NotificationService (configures SendGrid, Twilio,
         and Jinja2 template environment)
      3. Build the Kafka consumer and subscribe to booking.confirmed
      4. Create the initial heartbeat sentinel file
      5. Run the poll loop until shutdown signal
      6. Close the consumer cleanly on exit
    """

    # ── METRICS SERVER ────────────────────────────────────────────────────────
    start_http_server(port=8004)
    logger.info("Prometheus metrics server started on port 8004")

    # ── NOTIFICATION SERVICE ──────────────────────────────────────────────────
    # NotificationService.__init__() configures:
    #   - SendGrid client with API key from settings
    #   - Twilio client with account SID and auth token from settings
    #   - Jinja2 environment with templates/ directory and compiled templates
    #   - Mock flags (SENDGRID_MOCK_ENABLED, TWILIO_MOCK_ENABLED)
    notification_service = NotificationService()

    logger.info(
        "NotificationService initialised",
        extra={
            "sendgrid_mock": settings.SENDGRID_MOCK_ENABLED,
            "twilio_mock":   settings.TWILIO_MOCK_ENABLED,
            "from_email":    settings.SENDGRID_FROM_EMAIL,
            "from_name":     settings.SENDGRID_FROM_NAME,
        },
    )

    # Set initial health gauge values — assume healthy until proven otherwise
    SENDGRID_HEALTH.set(1)
    TWILIO_HEALTH.set(1)

    # ── KAFKA CONSUMER ────────────────────────────────────────────────────────
    consumer = _build_consumer()
    consumer.subscribe(
        [settings.TOPIC_BOOKING_CONFIRMED],
        on_assign=_on_partition_assign,
        on_revoke=_on_partition_revoke,
    )

    logger.info(
        "Notification Service subscribed to Kafka topic",
        extra={
            "topic":          settings.TOPIC_BOOKING_CONFIRMED,
            "consumer_group": settings.KAFKA_CONSUMER_GROUP,
            "bootstrap":      settings.KAFKA_BOOTSTRAP_SERVERS,
        },
    )

    # Create initial heartbeat file — prevents Docker from marking the
    # container as unhealthy during the 30-second start-period window.
    _update_heartbeat()

    try:
        await _run_consumer_loop(
            consumer=consumer,
            notification_service=notification_service,
        )

    finally:
        # ── CLEAN SHUTDOWN ────────────────────────────────────────────────────

        # Close the Kafka consumer — commits any pending offsets and notifies
        # the broker that this consumer is leaving the group, triggering an
        # immediate rebalance so another instance takes over the partitions.
        consumer.close()
        logger.info("Kafka consumer closed")

        # Flush the DLQ producer if it was created (only on error paths)
        if _dlq_producer:
            _dlq_producer.flush(timeout=10)
            logger.info("DLQ producer flushed")

        # Flush any buffered log output so the final log lines are visible
        # in `docker compose logs` after the container stops.
        for h in logger.handlers:
            h.flush()

        logger.info("Notification Service shut down cleanly")


# ──────────────────────────────────────────────────────────────────────────────
# MODULE ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("Using uvloop event loop for Notification Service")
    except ImportError:
        logger.warning("uvloop not available — using default asyncio event loop")

    try:
        asyncio.run(run_consumer())
    except Exception as exc:
        logger.critical(
            "Notification Service crashed with unhandled exception",
            extra={"error": str(exc)},
            exc_info=True,
        )
        sys.exit(1)