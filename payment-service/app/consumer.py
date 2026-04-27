# payment-service/app/consumer.py
#
# Main entry point for the Payment Service.
#
# This module runs a long-lived asyncio event loop that:
#   1. Subscribes to the `seat.reserved` Kafka topic
#   2. Deserializes each message into a SeatReservedEvent
#   3. Delegates payment processing to PaymentService (service.py)
#   4. Publishes PaymentProcessedEvent or PaymentFailedEvent downstream
#   5. Commits the Kafka offset only after the DB write AND Kafka publish succeed
#   6. Routes unprocessable messages to the dead-letter queue (DLQ)
#   7. Updates a sentinel file on every processed event for Docker health check
#
# Concurrency model:
#   Same as booking-service/app/consumer.py - confluent-kafka's synchronous
#   poll() runs in asyncio's thread pool executor so the event loop stays
#   free to handle concurrent DB writes and Stripe API calls without blocking.
#
# Offset management:
#   enable.auto.commit is False. Offsets are committed manually AFTER both
#   the PostgreSQL write and the downstream Kafka publish succeed. This is
#   stricter than the Booking Service because a payment event that is consumed
#   but not published downstream leaves the booking permanently stuck in
#   PAYMENT_PENDING - a worse failure mode than a duplicate DB write.
#
# Idempotency:
#   The Payment Service must be idempotent because Kafka guarantees
#   at-least-once delivery. If the container crashes after charging the
#   card but before committing the offset, the same SeatReservedEvent is
#   re-delivered. PaymentService.handle_seat_reserved() checks whether a
#   payment record already exists for the booking_id before calling Stripe
#   — preventing a double charge on re-delivery.

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from pythonjsonlogger import jsonlogger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from app.config import settings
from app.service import PaymentService
from app.db.connection import get_db_session, init_db, close_db
from shared.schemas import (
    SeatReservedEvent,
    BookingFailedDLQEvent,
    deserialize_event,
    serialize_event,
)


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# Structured JSON logging — every payment log line includes booking_id,
# correlation_id, payment_id, amount, and stripe_error_code as structured
# fields so Grafana Loki can filter without regex.
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("payment-service.consumer")
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
# Exposed on port 8002. Prometheus scrapes this every 15 seconds.
# Visualised in the Grafana Kafka + Payment overview dashboard.
# ──────────────────────────────────────────────────────────────────────────────

# Total payment attempts broken down by outcome
PAYMENT_ATTEMPTS_TOTAL = Counter(
    "payment_service_attempts_total",
    "Total payment processing attempts",
    ["status"],     # labels: "success" | "failed" | "retried" | "dlq"
)

# End-to-end processing time including Stripe API call and DB write
PAYMENT_PROCESSING_DURATION = Histogram(
    "payment_service_processing_duration_seconds",
    "Time taken to process one seat.reserved message end-to-end",
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
    # Upper buckets are higher than Booking Service because Stripe API calls
    # can take several seconds under load or during network degradation.
)

# Stripe-specific error counter — lets you alert on card_declined spike
STRIPE_ERRORS_TOTAL = Counter(
    "payment_service_stripe_errors_total",
    "Total Stripe API errors by error code",
    ["error_code"],     # labels: "card_declined" | "rate_limit" | "api_error" | ...
)

# DLQ counter — alert when this rises to catch unprocessable events early
DLQ_MESSAGES_TOTAL = Counter(
    "payment_service_dlq_messages_total",
    "Total messages routed to dead-letter queue by Payment Service",
)

# Amount of money successfully processed — useful for business dashboards
REVENUE_PROCESSED_TOTAL = Counter(
    "payment_service_revenue_processed_total",
    "Total payment amount successfully processed (in base currency units)",
    ["currency"],
)

# Current consumer lag approximation — incremented on retry, reset on success
CONSUMER_RETRIES_TOTAL = Counter(
    "payment_service_consumer_retries_total",
    "Total number of message processing retries",
)

# Tracks whether the Stripe connection is healthy — used for alerting
STRIPE_HEALTH = Gauge(
    "payment_service_stripe_healthy",
    "1 if Stripe API is reachable, 0 if not",
)


# ──────────────────────────────────────────────────────────────────────────────
# SENTINEL FILE
# Touched on every successfully processed payment event.
# Docker HEALTHCHECK reads the file modification time to detect a stalled
# consumer — if not updated within 90 seconds the container is restarted.
# Different path from Booking Service (/tmp/consumer_heartbeat) so both
# services can run on the same host without the files colliding.
# ──────────────────────────────────────────────────────────────────────────────

HEARTBEAT_FILE = Path("/tmp/payment_consumer_heartbeat")


def _update_heartbeat() -> None:
    """Touch sentinel file to signal the consumer is alive and processing."""
    HEARTBEAT_FILE.touch(exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# GRACEFUL SHUTDOWN
# Intercepts SIGTERM (Docker stop) and SIGINT (Ctrl+C) and sets a flag
# the consumer loop checks on every iteration. This ensures the current
# payment transaction is always completed before the process exits —
# a mid-charge shutdown would leave the card charged with no DB record.
# ──────────────────────────────────────────────────────────────────────────────

_shutdown_requested: bool = False


def _handle_shutdown(signum, frame) -> None:
    """Signal handler — sets shutdown flag for clean consumer loop exit."""
    global _shutdown_requested
    logger.info(
        "Shutdown signal received — will exit after current message completes",
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
    Construct a configured confluent-kafka Consumer for the Payment Service.

    Configuration differences from Booking Service consumer:
      - max.poll.interval.ms is higher (600s vs 300s) because payment
        processing includes a Stripe API call that can take up to 30s
        under degraded conditions, plus retries with exponential backoff.
        If processing takes longer than max.poll.interval.ms the broker
        considers the consumer dead and triggers a rebalance — increasing
        the timeout prevents false rebalances during slow Stripe responses.
      - session.timeout.ms is kept at 30s — this tracks liveness via
        heartbeats (separate from poll interval) and 30s is appropriate.
      - fetch.min.bytes is higher (1024 vs 1) because seat.reserved events
        are larger payloads than booking.requested events (they include
        full passenger and flight details embedded from shared/schemas.py).
    """
    config = {
        "bootstrap.servers":      settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id":               settings.KAFKA_CONSUMER_GROUP,

        # Start from earliest unread offset — ensures no payment events
        # are missed if the service is restarted after a crash.
        "auto.offset.reset":      "earliest",

        # Manual offset commit — offset only advances after the payment
        # is fully processed, written to DB, and published downstream.
        "enable.auto.commit":     False,

        # Higher than Booking Service to accommodate Stripe API call latency
        "max.poll.interval.ms":   600_000,   # 10 minutes

        # Standard heartbeat and session timeout
        "heartbeat.interval.ms":  3_000,
        "session.timeout.ms":     30_000,

        # Larger minimum fetch for bigger payloads
        "fetch.min.bytes":        1_024,     # 1 KB

        # Cap per-partition fetch at 1MB — payment events are large but
        # should never approach this limit
        "max.partition.fetch.bytes": 1_048_576,

        # Wait up to 500ms for fetch.min.bytes before returning — balances
        # latency against network efficiency
        "fetch.wait.max.ms":      500,
    }

    return Consumer(config)


# ──────────────────────────────────────────────────────────────────────────────
# DLQ PRODUCER
# Thin inline producer for routing unprocessable payment events to the DLQ.
# Separate from the main Kafka producer in producer.py — the DLQ writer
# must work even if the main producer is in a degraded state.
# ──────────────────────────────────────────────────────────────────────────────

from confluent_kafka import Producer as _RawProducer

_dlq_producer: _RawProducer | None = None


def _get_dlq_producer() -> _RawProducer:
    """Lazy singleton DLQ producer — created on first use."""
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = _RawProducer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                # Synchronous delivery required for DLQ - I need to know
                # immediately if the DLQ write succeeded or failed.
                "acks": "all",
                "retries": 3,
            }
        )
    return _dlq_producer


def _send_to_dlq(
    raw_payload: str,
    error_message: str,
    booking_id: str | None,
    retry_count: int,
    failed_topic: str,
) -> None:
    """
    Publish an unprocessable payment event to the dead-letter queue.

    Called when:
      - The message cannot be deserialized (malformed JSON or unknown schema)
      - Payment processing fails after all retries are exhausted
      - A booking_id collision is detected (same event delivered twice
        with conflicting data — should never happen but handled defensively)

    The original raw payload is preserved so engineers can inspect the
    exact bytes that caused the failure and replay them after a fix.
    """
    dlq_event = BookingFailedDLQEvent(
        # correlation_id may not be parseable if deserialization failed —
        # use "unknown" as a safe fallback
        correlation_id="unknown",
        booking_id=booking_id,
        failed_topic=failed_topic,
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
            ("event_type",    b"booking.failed.dlq"),
            ("service",       b"payment-service"),
            ("failed_topic",  failed_topic.encode("utf-8")),
        ],
    )

    # Flush synchronously - DLQ writes must not be buffered. I need to
    # know immediately if this write failed so the caller can log CRITICAL.
    remaining = producer.flush(timeout=10)

    DLQ_MESSAGES_TOTAL.inc()

    if remaining > 0:
        # Cannot write to DLQ — message will be lost. Log at CRITICAL
        # so PagerDuty / alerting pipeline fires immediately.
        logger.critical(
            "CRITICAL: DLQ write failed — payment event will be lost",
            extra={
                "booking_id":    booking_id,
                "failed_topic":  failed_topic,
                "error_message": error_message,
            },
        )
    else:
        logger.error(
            "Payment event routed to DLQ",
            extra={
                "booking_id":    booking_id,
                "failed_topic":  failed_topic,
                "error_message": error_message,
                "retry_count":   retry_count,
            },
        )


# ──────────────────────────────────────────────────────────────────────────────
# MESSAGE PROCESSOR
# Handles a single seat.reserved Kafka message end-to-end.
# Separated from the poll loop so it can be unit-tested without Kafka.
# ──────────────────────────────────────────────────────────────────────────────

async def _process_message(
    msg: Message,
    payment_service: PaymentService,
) -> bool:
    """
    Deserialize, validate, and process one seat.reserved Kafka message.

    Returns:
        True  — message processed successfully, safe to commit offset.
        False — message sent to DLQ after permanent failure,
                offset should still be committed to unblock the partition.

    Never raises — all exceptions are caught internally. Unhandled
    exceptions would crash the consumer loop and stop all payment
    processing until the container restarts.
    """
    raw   = msg.value().decode("utf-8") if msg.value() else ""
    topic = msg.topic()

    # ── STEP 1: Deserialize ───────────────────────────────────────────────────
    # A deserialization failure is always a permanent error — retrying
    # will not fix a malformed payload. Route to DLQ immediately.
    try:
        event: SeatReservedEvent = deserialize_event(topic, raw)
    except Exception as exc:
        logger.error(
            "Failed to deserialize seat.reserved message — routing to DLQ",
            extra={
                "topic":  topic,
                "offset": msg.offset(),
                "error":  str(exc),
                "raw":    raw[:500],   # truncate to prevent log flooding
            },
        )
        PAYMENT_ATTEMPTS_TOTAL.labels(status="dlq").inc()
        _send_to_dlq(
            raw_payload=raw,
            error_message=f"Deserialization failed: {exc}",
            booking_id=None,
            retry_count=0,
            failed_topic=topic,
        )
        return False

    booking_id     = event.booking_id
    correlation_id = event.correlation_id

    logger.info(
        "Processing seat.reserved event",
        extra={
            "booking_id":     booking_id,
            "correlation_id": correlation_id,
            "seat_number":    event.seat_number,
            "flight_id":      event.flight_id,
            "seat_class":     event.seat_class.value,
            "lock_ttl":       event.lock_ttl,
        },
    )

    # ── STEP 2: Process payment with retry ────────────────────────────────────
    # Transient errors (Stripe rate limit, DB connection blip, network
    # timeout) are retried with exponential backoff. Permanent errors
    # (card declined, invalid card number, fraud block) are sent to DLQ
    # immediately without retrying — retrying a declined card wastes time
    # and risks triggering Stripe's abuse detection.
    MAX_RETRIES = settings.PAYMENT_RETRY_ATTEMPTS   # default: 3 from .env

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with PAYMENT_PROCESSING_DURATION.time():
                async with get_db_session() as session:
                    result = await payment_service.handle_seat_reserved(
                        event=event,
                        session=session,
                    )

            # result is a dict returned by PaymentService:
            # {
            #   "status": "success" | "failed",
            #   "payment_id": str,
            #   "amount": Decimal,
            #   "currency": str,
            #   "error_code": str | None,   # populated on failure
            #   "is_retryable": bool,       # True for transient errors
            # }

            if result["status"] == "success":
                # ── PAYMENT SUCCESS ───────────────────────────────────────────
                PAYMENT_ATTEMPTS_TOTAL.labels(status="success").inc()
                REVENUE_PROCESSED_TOTAL.labels(
                    currency=result["currency"]
                ).inc(float(result["amount"]))
                STRIPE_HEALTH.set(1)
                _update_heartbeat()

                logger.info(
                    "Payment processed successfully",
                    extra={
                        "booking_id":     booking_id,
                        "correlation_id": correlation_id,
                        "payment_id":     result["payment_id"],
                        "amount":         str(result["amount"]),
                        "currency":       result["currency"],
                        "attempt":        attempt,
                    },
                )
                return True

            else:
                # ── PAYMENT DECLINED (permanent failure) ──────────────────────
                # Card declines, fraud blocks, and invalid card errors are
                # not retryable — the same card will be declined again.
                # Route to DLQ and commit the offset to unblock the partition.
                error_code = result.get("error_code", "unknown")
                PAYMENT_ATTEMPTS_TOTAL.labels(status="failed").inc()
                STRIPE_ERRORS_TOTAL.labels(error_code=error_code).inc()

                logger.warning(
                    "Payment permanently declined — routing to DLQ",
                    extra={
                        "booking_id":     booking_id,
                        "correlation_id": correlation_id,
                        "error_code":     error_code,
                        "attempt":        attempt,
                    },
                )
                _send_to_dlq(
                    raw_payload=raw,
                    error_message=f"Payment declined: {error_code}",
                    booking_id=booking_id,
                    retry_count=attempt,
                    failed_topic=topic,
                )
                _update_heartbeat()
                return False

        except Exception as exc:
            # ── TRANSIENT ERROR (retryable) ───────────────────────────────────
            # Network timeouts, DB connection errors, Stripe 500s, and
            # rate limit errors land here. Retry with exponential backoff.
            is_last_attempt = attempt == MAX_RETRIES

            CONSUMER_RETRIES_TOTAL.inc()
            STRIPE_HEALTH.set(0)

            logger.warning(
                "Transient error processing payment event",
                extra={
                    "booking_id":     booking_id,
                    "correlation_id": correlation_id,
                    "attempt":        attempt,
                    "max_retries":    MAX_RETRIES,
                    "error":          str(exc),
                    "will_retry":     not is_last_attempt,
                },
            )

            if is_last_attempt:
                # All retries exhausted — treat as permanent failure
                PAYMENT_ATTEMPTS_TOTAL.labels(status="dlq").inc()
                _send_to_dlq(
                    raw_payload=raw,
                    error_message=(
                        f"Payment processing failed after {MAX_RETRIES} "
                        f"attempts: {exc}"
                    ),
                    booking_id=booking_id,
                    retry_count=attempt,
                    failed_topic=topic,
                )
                return False

            # Exponential backoff with jitter — prevents thundering herd
            # when Stripe has an outage and all consumers retry simultaneously.
            # Backoff: 2s → 4s → 8s → 16s (capped at 30s)
            import random
            backoff = min(2 ** attempt, 30) + random.uniform(0, 1)
            logger.info(
                f"Retrying payment in {backoff:.1f}s",
                extra={
                    "booking_id": booking_id,
                    "attempt":    attempt,
                    "backoff_s":  round(backoff, 1),
                },
            )
            PAYMENT_ATTEMPTS_TOTAL.labels(status="retried").inc()
            await asyncio.sleep(backoff)

    # Unreachable — loop always returns inside the for block
    return False


# ──────────────────────────────────────────────────────────────────────────────
# CONSUMER LOOP
# ──────────────────────────────────────────────────────────────────────────────

@retry(
    # Retry the entire consumer loop on KafkaException (broker restart,
    # network partition). Give up after 10 attempts — at that point the
    # infrastructure problem is severe enough to warrant a human response.
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    retry=retry_if_exception_type(KafkaException),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def _run_consumer_loop(
    consumer: Consumer,
    payment_service: PaymentService,
) -> None:
    """
    Core poll loop — runs until shutdown signal or fatal KafkaException.

    Each iteration:
      1. poll() for a message (in thread executor — non-blocking)
      2. Skip control messages (EOF, empty poll)
      3. Process the payment event via _process_message()
      4. Commit the offset after processing (success or DLQ)
    """
    loop = asyncio.get_event_loop()

    logger.info(
        "Payment consumer loop started",
        extra={
            "topic":          settings.TOPIC_SEAT_RESERVED,
            "consumer_group": settings.KAFKA_CONSUMER_GROUP,
        },
    )

    while not _shutdown_requested:

        # ── POLL ──────────────────────────────────────────────────────────────
        # Run poll() in thread executor so the event loop is not blocked
        # while waiting for the Kafka broker to return messages.
        # timeout=1.0s keeps the shutdown flag check responsive.
        msg: Message | None = await loop.run_in_executor(
            None,
            lambda: consumer.poll(timeout=1.0),
        )

        # ── NO MESSAGE ────────────────────────────────────────────────────────
        if msg is None:
            # No messages available right now — loop and poll again.
            # This is normal when the consumer is caught up to the end
            # of the seat.reserved partition.
            continue

        # ── KAFKA ERRORS ──────────────────────────────────────────────────────
        if msg.error():
            error = msg.error()

            if error.code() == KafkaError._PARTITION_EOF:
                # Reached end of partition — not an error, just means we
                # have consumed all currently available seat.reserved events.
                logger.debug(
                    "End of partition reached",
                    extra={
                        "topic":     msg.topic(),
                        "partition": msg.partition(),
                        "offset":    msg.offset(),
                    },
                )
                continue

            # Any other Kafka error — raise so tenacity can catch and retry.
            # Examples: broker not available, leader election in progress.
            raise KafkaException(error)

        # ── PROCESS MESSAGE ───────────────────────────────────────────────────
        logger.info(
            "Message received from Kafka",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
                "key":       msg.key().decode("utf-8") if msg.key() else None,
                # Extract correlation_id from Kafka headers if present —
                # avoids deserializing the full payload just for logging
                "correlation_id": _extract_header(msg, "correlation_id"),
            },
        )

        await _process_message(
            msg=msg,
            payment_service=payment_service,
        )

        # ── COMMIT OFFSET ─────────────────────────────────────────────────────
        # Commit after processing regardless of outcome (success or DLQ).
        # DLQ messages are preserved in booking.failed.dlq — committing here
        # just advances the seat.reserved consumer group offset so the
        # partition is not blocked by one bad message.
        # asynchronous=False means I wait for the broker to acknowledge
        # the commit before polling the next message — safer but slightly
        # slower than async commits. For payment events, correctness > speed.
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

    logger.info("Shutdown flag set — exiting payment consumer loop cleanly")


# ──────────────────────────────────────────────────────────────────────────────
# PARTITION CALLBACKS
# ──────────────────────────────────────────────────────────────────────────────

def _on_partition_assign(consumer: Consumer, partitions: list) -> None:
    """
    Called when the broker assigns partitions to this consumer instance.
    Logs the assignment so rebalance events are visible in the audit trail.
    During a rebalance, payment processing pauses until the new assignment
    is complete — important to know when investigating processing gaps.
    """
    logger.info(
        "Partitions assigned to Payment Service consumer",
        extra={
            "partitions": [
                {"topic": p.topic, "partition": p.partition, "offset": p.offset}
                for p in partitions
            ]
        },
    )


def _on_partition_revoke(consumer: Consumer, partitions: list) -> None:
    """
    Called when partitions are revoked during a rebalance.
    Logs the revocation — any in-flight payment being processed when
    this fires will complete before the partition is handed off because
    the consumer loop checks _shutdown_requested only between messages,
    not mid-processing.
    """
    logger.warning(
        "Partitions revoked from Payment Service — rebalance in progress",
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

    Kafka headers are a list of (key, value) tuples where values are bytes.
    Used to read correlation_id for logging without deserializing the full
    JSON payload — saves CPU on every poll iteration.

    Returns None if the header is absent rather than raising KeyError,
    so missing headers are handled gracefully in log statements.
    """
    if not msg.headers():
        return None
    for k, v in msg.headers():
        if k == key:
            return v.decode("utf-8") if v else None
    return None


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# Called by entrypoint.sh via `python3 -m app.consumer`
# ──────────────────────────────────────────────────────────────────────────────

async def run_consumer() -> None:
    """
    Bootstrap the Payment Service consumer:
      1. Start the Prometheus metrics server on port 8002
      2. Verify database connectivity (init_db)
      3. Build the Kafka consumer and subscribe to seat.reserved
      4. Instantiate PaymentService (configures Stripe client)
      5. Create the initial heartbeat sentinel file
      6. Run the poll loop until shutdown signal
      7. Flush the Kafka producer and close DB pool on exit
    """
    # ── METRICS SERVER ────────────────────────────────────────────────────────
    # Port 8002 — does not conflict with API Gateway (8000), Booking Service
    # Prometheus (8001), or any other service in the compose network.
    start_http_server(port=8002)
    logger.info("Prometheus metrics server started on port 8002")

    # ── DATABASE ──────────────────────────────────────────────────────────────
    # Verify PostgreSQL is reachable and the payments table exists (Alembic
    # migrations ran in entrypoint.sh before this function was called).
    await init_db()

    # ── KAFKA CONSUMER ────────────────────────────────────────────────────────
    consumer = _build_consumer()
    consumer.subscribe(
        [settings.TOPIC_SEAT_RESERVED],
        on_assign=_on_partition_assign,
        on_revoke=_on_partition_revoke,
    )

    logger.info(
        "Payment Service subscribed to Kafka topic",
        extra={
            "topic":          settings.TOPIC_SEAT_RESERVED,
            "consumer_group": settings.KAFKA_CONSUMER_GROUP,
            "bootstrap":      settings.KAFKA_BOOTSTRAP_SERVERS,
        },
    )

    # ── PAYMENT SERVICE ───────────────────────────────────────────────────────
    # PaymentService configures the Stripe SDK on __init__ using settings
    # from app/config.py. In mock mode (STRIPE_MOCK_ENABLED=true) it uses
    # a local mock server instead of hitting the real Stripe API.
    payment_service = PaymentService()

    logger.info(
        "PaymentService initialised",
        extra={
            "stripe_mock_enabled": settings.STRIPE_MOCK_ENABLED,
            "retry_attempts":      settings.PAYMENT_RETRY_ATTEMPTS,
        },
    )

    # Set initial Stripe health gauge — assumes healthy until proven otherwise
    STRIPE_HEALTH.set(1)

    # Create initial heartbeat file so Docker HEALTHCHECK does not flag the
    # container as unhealthy during the 90-second start-period window.
    _update_heartbeat()

    try:
        await _run_consumer_loop(
            consumer=consumer,
            payment_service=payment_service,
        )

    finally:
        # ── CLEAN SHUTDOWN ────────────────────────────────────────────────────
        # Always runs — even if the loop exits via exception or signal.

        # Close the Kafka consumer — commits pending offsets and notifies
        # the broker this consumer is leaving the group. Triggers an
        # immediate rebalance so another instance takes the partitions.
        consumer.close()
        logger.info("Kafka consumer closed")

        # Flush and close the DLQ producer if it was used
        if _dlq_producer:
            _dlq_producer.flush(timeout=10)
            logger.info("DLQ producer flushed")

        # Dispose the SQLAlchemy connection pool — closes all idle DB
        # connections cleanly rather than letting the OS close TCP sockets
        # abruptly on process exit.
        await close_db()
        logger.info("Database connection pool disposed")

        logger.info("Payment Service shut down cleanly")


# ──────────────────────────────────────────────────────────────────────────────
# MODULE ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        import uvloop
        # uvloop is 2-4x faster than the default asyncio event loop for
        # I/O-bound workloads. Payment processing is heavily I/O-bound:
        # Stripe API call + PostgreSQL write + two Kafka produces per message.
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("Using uvloop event loop for Payment Service")
    except ImportError:
        logger.warning("uvloop not available — using default asyncio event loop")

    try:
        asyncio.run(run_consumer())
    except Exception as exc:
        logger.critical(
            "Payment Service crashed with unhandled exception",
            extra={"error": str(exc)},
            exc_info=True,
        )
        # Exit with non-zero code so Docker's restart policy kicks in
        # and the container is restarted automatically.
        sys.exit(1)