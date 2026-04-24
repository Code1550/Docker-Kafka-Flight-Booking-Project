# payment-service/app/producer.py
#
# Kafka producer for the Payment Service.
#
# Responsibilities:
#   - Publish PaymentProcessedEvent to `payment.processed` after a
#     successful Stripe charge and PostgreSQL write
#   - Publish PaymentFailedEvent to `payment.failed` after a card decline
#     or exhausted retries so Booking Service and Seat Service can react
#   - Publish BookingFailedDLQEvent to `booking.failed.dlq` for events
#     that cannot be processed under any circumstances
#
# Design notes:
#   Mirrors the structure of booking-service/app/producer.py with two
#   important differences:
#
#   1. PaymentFailedEvent carries a `retry_attempt` counter so downstream
#      consumers (Booking Service, Seat Service) know whether the failure
#      was a single transient error or the result of exhausted retries.
#      This lets them decide whether to immediately release the seat lock
#      or wait briefly in case the payment is retried externally.
#
#   2. flush() is called with a shorter timeout (10s vs 15s) because
#      payment events are time-sensitive — a delayed payment.processed
#      event leaves the booking in PAYMENT_PENDING and can cause the
#      Redis seat lock to expire before Booking Service confirms the
#      booking. Fast failure is preferable to a long wait here.
#
#   Like the Booking Service producer, a module-level singleton Producer
#   is used. Creating a new Producer per message is expensive and
#   unnecessary for a long-running consumer process.

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from confluent_kafka import Producer, KafkaException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    wait_exponential_jitter,
    retry_if_exception_type,
    before_sleep_log,
)

from app.config import settings
from shared.schemas import (
    PaymentProcessedEvent,
    PaymentFailedEvent,
    BookingFailedDLQEvent,
    serialize_event,
)


logger = logging.getLogger("payment-service.producer")


# ──────────────────────────────────────────────────────────────────────────────
# DELIVERY CALLBACK
# Invoked by librdkafka after each message is delivered or permanently fails.
# Payment delivery failures are logged at ERROR level — a failed
# payment.processed publish leaves the booking in PAYMENT_PENDING
# indefinitely, which is a serious data integrity issue.
# ──────────────────────────────────────────────────────────────────────────────

def _delivery_callback(err, msg) -> None:
    """
    Called by librdkafka once delivery succeeds or permanently fails.

    Unlike the Booking Service delivery callback, payment delivery failures
    are logged at ERROR (not WARNING) because the consequences are more
    severe — a dropped payment.processed event means the passenger is
    charged but their booking is never confirmed.

    In production this callback should also:
      - Increment a PAYMENT_DELIVERY_FAILURES_TOTAL Prometheus counter
      - Write the failed event to a local fallback store (e.g. a file or
        Redis) so it can be replayed after the broker recovers
    """
    if err:
        logger.error(
            "PAYMENT EVENT DELIVERY FAILED — booking may be stuck in PAYMENT_PENDING",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "error":     str(err),
                # key() is the booking_id — log it so engineers can
                # manually fix the affected booking
                "booking_id": msg.key().decode("utf-8") if msg.key() else None,
            },
        )
    else:
        logger.debug(
            "Payment event delivered successfully",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
            },
        )


# ──────────────────────────────────────────────────────────────────────────────
# SINGLETON PRODUCER
# Created lazily on first use via _get_producer().
# One instance is reused for the lifetime of the Payment Service process.
# ──────────────────────────────────────────────────────────────────────────────

_producer: Optional[Producer] = None


def _get_producer() -> Producer:
    """
    Return the module-level Producer singleton, creating it on first call.

    Producer configuration differences from Booking Service producer:
      - linger.ms is lower (5ms vs 25ms) — payment events are time-critical.
        A delayed payment.processed publish can cause the Redis seat lock
        (TTL=300s) to expire before Booking Service processes the confirmation.
        Lower linger means messages are sent sooner at the cost of slightly
        less batching efficiency — a worthwhile trade for payment events.
      - message.timeout.ms is lower (15s vs 30s) — faster failure detection.
        If a payment event cannot be delivered in 15 seconds the broker has
        a serious problem. We want to know quickly so we can retry or alert.
      - queue.buffering.max.messages is set explicitly to cap memory usage.
        Payment events are larger than booking events (they carry full
        passenger, flight, and payment details) so we set a lower queue cap.
    """
    global _producer

    if _producer is not None:
        return _producer

    config = {
        # Broker connection
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,

        # Require all in-sync replicas to acknowledge — maximum durability.
        # A payment event that is lost after a broker crash is catastrophic
        # because it means the passenger was charged but not confirmed.
        "acks": "all",

        # Idempotent producer — prevents duplicate payment.processed events
        # caused by producer retries. A duplicate payment.processed would
        # trigger a duplicate booking confirmation email (annoying) or,
        # worse, a duplicate DB update that overwrites a refunded status.
        "enable.idempotence": True,

        # Strict ordering within each partition.
        # payment.processed must always arrive before any subsequent event
        # referencing the same booking_id (e.g. a refund event in future).
        "max.in.flight.requests.per.connection": 1,

        # Retry failed produce requests up to 5 times before the delivery
        # callback is called with an error.
        "retries": 5,

        # Backoff between produce retries
        "retry.backoff.ms": 300,

        # Lower linger than Booking Service — payment events are time-critical
        # and should be sent to the broker as quickly as possible.
        "linger.ms": 5,

        # Snappy compression — reduces broker network load while being fast
        # enough that it does not add meaningful latency to payment events.
        "compression.type": "snappy",

        # Fail delivery after 15 seconds — faster than Booking Service (30s)
        # because payment events are time-sensitive (seat lock TTL = 300s).
        "message.timeout.ms": 15_000,

        # Cap the in-memory message queue at 1000 messages.
        # Payment events are large (full passenger + flight + payment details).
        # Without this cap, a Kafka outage during a traffic spike could cause
        # the queue to grow until the container runs out of memory.
        "queue.buffering.max.messages": 1_000,

        # Maximum size of the in-memory message queue in kilobytes.
        # 32MB cap provides additional memory protection alongside the
        # message count cap above.
        "queue.buffering.max.kbytes": 32_768,   # 32 MB
    }

    _producer = Producer(config)
    logger.info(
        "Payment Service Kafka producer initialised",
        extra={"bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS},
    )
    return _producer


# ──────────────────────────────────────────────────────────────────────────────
# CORE PUBLISH HELPER
# All public publish_* functions delegate to this. Keeps encode / produce /
# poll logic in one place and applies consistent retry behaviour.
# ──────────────────────────────────────────────────────────────────────────────

@retry(
    # Use jitter on payment producer retries — same reason as consumer.py.
    # If Kafka has a brief outage, all payment produce() calls fail at the
    # same time. Jitter prevents them all retrying simultaneously.
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=10, jitter=1),
    retry=retry_if_exception_type(KafkaException),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def _publish(
    topic: str,
    key: str,
    event,
    headers: Optional[dict[str, str]] = None,
) -> None:
    """
    Serialize and publish one payment event to a Kafka topic.

    Args:
        topic:   Target Kafka topic (e.g. "payment.processed").
        key:     Message key — always booking_id for partition ordering.
        event:   A Pydantic BaseEvent subclass from shared/schemas.py.
        headers: Optional metadata headers — used to carry correlation_id,
                 payment_id, and event_type for tracing tools that read
                 headers without deserializing the full payload.

    Raises:
        KafkaException: If all tenacity retries are exhausted.
    """
    producer = _get_producer()
    loop     = asyncio.get_event_loop()

    # Serialize event to UTF-8 JSON bytes
    value_bytes = serialize_event(event).encode("utf-8")
    key_bytes   = key.encode("utf-8")

    # Convert headers dict to confluent-kafka's list-of-tuples format
    kafka_headers = (
        [(k, v.encode("utf-8")) for k, v in headers.items()]
        if headers else []
    )

    # produce() is synchronous but non-blocking — places the message in
    # librdkafka's internal send buffer and returns immediately.
    # run_in_executor prevents blocking the asyncio event loop.
    await loop.run_in_executor(
        None,
        lambda: producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
            headers=kafka_headers,
            on_delivery=_delivery_callback,
        ),
    )

    # poll(0) drains the delivery callback queue without blocking.
    # Called immediately after every produce() so delivery failures are
    # surfaced and logged in real time rather than discovered at shutdown.
    await loop.run_in_executor(
        None,
        lambda: producer.poll(0),
    )

    logger.debug(
        "Payment event queued for delivery",
        extra={
            "topic": topic,
            "key":   key,
        },
    )


async def _flush(timeout: float = 10.0) -> None:
    """
    Block until all buffered payment events are delivered or timeout expires.

    Timeout is 10s (lower than Booking Service's 15s) because payment events
    are time-critical — a stalled flush during a busy period could delay
    payment confirmations long enough for the Redis seat lock to expire.

    Args:
        timeout: Maximum seconds to wait for delivery.
    """
    producer = _get_producer()
    loop     = asyncio.get_event_loop()

    remaining: int = await loop.run_in_executor(
        None,
        lambda: producer.flush(timeout),
    )

    if remaining > 0:
        logger.error(
            "Kafka flush timed out — payment events may not have been delivered",
            extra={
                "undelivered_count": remaining,
                "timeout_seconds":   timeout,
            },
        )
    else:
        logger.debug("Kafka payment producer flush completed successfully")


# ──────────────────────────────────────────────────────────────────────────────
# PUBLIC PUBLISH FUNCTIONS
# One function per Kafka topic the Payment Service produces to.
# Keeping them separate makes call sites in service.py readable and allows
# topic-specific headers, logging, and business metrics without conditionals.
# ──────────────────────────────────────────────────────────────────────────────

async def publish_payment_processed(event: PaymentProcessedEvent) -> None:
    """
    Publish a PaymentProcessedEvent to the `payment.processed` topic.

    Called by PaymentService after:
      1. Stripe confirms the charge was successful
      2. The payment transaction record is written to PostgreSQL

    Consumers:
      - Booking Service  → updates booking status to CONFIRMED in PostgreSQL
      - Notification Service → sends confirmation email/SMS to passenger

    Headers carry the payment_id and amount as plain bytes so monitoring
    tools and stream processors can read them without deserializing the
    full JSON payload — useful for real-time revenue dashboards.

    Args:
        event: The PaymentProcessedEvent to publish. Must have already been
               validated by Pydantic and have a non-null payment_id.
    """
    await _publish(
        topic=settings.TOPIC_PAYMENT_PROCESSED,
        key=event.booking_id,
        event=event,
        headers={
            # Standard tracing headers read by all consumers
            "correlation_id": event.correlation_id,
            "event_type":     "payment.processed",
            "service":        "payment-service",

            # Payment-specific headers for monitoring tools
            # that process headers without deserializing the payload
            "payment_id":     event.payment.payment_id,
            "amount":         str(event.payment.amount),
            "currency":       event.payment.currency,
            "payment_method": event.payment.payment_method,
        },
    )
    await _flush()

    logger.info(
        "PaymentProcessedEvent published successfully",
        extra={
            "booking_id":     event.booking_id,
            "correlation_id": event.correlation_id,
            "payment_id":     event.payment.payment_id,
            "amount":         str(event.payment.amount),
            "currency":       event.payment.currency,
            "payment_method": event.payment.payment_method,
        },
    )


async def publish_payment_failed(event: PaymentFailedEvent) -> None:
    """
    Publish a PaymentFailedEvent to the `payment.failed` topic.

    Called by PaymentService when:
      - The card is declined (permanent failure — no retry)
      - A Stripe API error persists after all retries are exhausted
      - The passenger's card has insufficient funds
      - Stripe's fraud detection blocks the charge

    Consumers:
      - Booking Service  → marks booking status as FAILED in PostgreSQL
      - Seat Service     → releases the Redis seat lock so the seat
                           becomes available for other passengers

    The `retry_attempt` field on the event tells consumers how many
    times the payment was attempted before giving up. Booking Service
    uses this to decide whether to immediately notify the passenger
    (on first failure) or wait briefly in case of a transient error.

    The `failure_reason` header carries the Stripe error code as a plain
    string so alerting rules in Grafana can fire on specific error codes
    (e.g. "too many card_declined events in the last 5 minutes") without
    deserializing the full payload on every event.

    Args:
        event: The PaymentFailedEvent. Must have payment.failure_reason set.
    """
    await _publish(
        topic=settings.TOPIC_PAYMENT_FAILED,
        key=event.booking_id,
        event=event,
        headers={
            # Standard tracing headers
            "correlation_id":  event.correlation_id,
            "event_type":      "payment.failed",
            "service":         "payment-service",

            # Failure-specific headers for alerting and monitoring
            "failure_reason":  event.payment.failure_reason or "unknown",
            "retry_attempt":   str(event.retry_attempt),
            "payment_method":  event.payment.payment_method,
            "currency":        event.payment.currency,
            "amount":          str(event.payment.amount),
        },
    )
    await _flush()

    logger.warning(
        "PaymentFailedEvent published",
        extra={
            "booking_id":     event.booking_id,
            "correlation_id": event.correlation_id,
            "failure_reason": event.payment.failure_reason,
            "retry_attempt":  event.retry_attempt,
            "payment_method": event.payment.payment_method,
        },
    )


async def publish_to_dlq(event: BookingFailedDLQEvent) -> None:
    """
    Publish a BookingFailedDLQEvent to the `booking.failed.dlq` topic.

    Called when a seat.reserved event cannot be processed under any
    circumstances — deserialization failure, missing booking record,
    or payment failure after all retries.

    Unlike publish_payment_processed() and publish_payment_failed(),
    this function does NOT use tenacity retry. If we cannot write to
    the DLQ the error must surface immediately as CRITICAL so engineers
    are alerted that messages are being permanently lost.

    The raw_payload field preserves the original Kafka message bytes so
    the event can be replayed after the underlying issue is fixed —
    without raw_payload the failed event is unrecoverable.

    Args:
        event: The BookingFailedDLQEvent with the original payload preserved
               in raw_payload and a clear error_message explaining the failure.
    """
    producer = _get_producer()
    loop     = asyncio.get_event_loop()

    value_bytes = serialize_event(event).encode("utf-8")
    key_bytes   = (event.booking_id or "unknown").encode("utf-8")

    # No tenacity retry — DLQ writes must surface failures immediately.
    # Retrying a failed DLQ write using the same broken connection is
    # unlikely to succeed and delays the CRITICAL log that triggers alerting.
    await loop.run_in_executor(
        None,
        lambda: producer.produce(
            topic=settings.TOPIC_BOOKING_FAILED_DLQ,
            key=key_bytes,
            value=value_bytes,
            headers=[
                ("event_type",    b"booking.failed.dlq"),
                ("service",       b"payment-service"),
                ("failed_topic",
                 event.failed_topic.encode("utf-8")),
                ("retry_count",
                 str(event.retry_count).encode("utf-8")),
            ],
            on_delivery=_delivery_callback,
        ),
    )

    # Flush synchronously and immediately — we must know right now whether
    # the DLQ write succeeded. A buffered DLQ write that silently fails
    # means the event is permanently lost with no record anywhere.
    remaining: int = await loop.run_in_executor(
        None,
        lambda: producer.flush(timeout=10),
    )

    if remaining > 0:
        # Cannot write to DLQ — this is the worst possible outcome.
        # The payment event is permanently lost. Log at CRITICAL and
        # ensure the metric fires so PagerDuty alerts on-call engineers.
        logger.critical(
            "CRITICAL: Failed to write payment event to DLQ — event will be lost permanently",
            extra={
                "booking_id":    event.booking_id,
                "failed_topic":  event.failed_topic,
                "error_message": event.error_message,
                "retry_count":   event.retry_count,
            },
        )
    else:
        logger.error(
            "Payment event written to DLQ",
            extra={
                "booking_id":    event.booking_id,
                "failed_topic":  event.failed_topic,
                "error_message": event.error_message,
                "retry_count":   event.retry_count,
            },
        )


# ──────────────────────────────────────────────────────────────────────────────
# SHUTDOWN HOOK
# Called by consumer.py in the finally block of run_consumer() to ensure
# all buffered payment events are delivered before the process exits.
# ──────────────────────────────────────────────────────────────────────────────

async def flush_and_close(timeout: float = 15.0) -> None:
    """
    Flush all pending payment events and mark the producer as closed.

    Called once from consumer.py during graceful shutdown after the poll
    loop exits. Gives in-flight produce() calls up to `timeout` seconds
    to be delivered before the process terminates.

    Payment events are more important than most to flush cleanly —
    an undelivered payment.processed event means the passenger's
    booking is never confirmed despite being charged.

    Args:
        timeout: Maximum seconds to wait. 15s is a longer window than the
                 per-message flush (10s) because shutdown only happens once
                 and we want to maximise the chance of clean delivery.
    """
    if _producer is None:
        # Producer was never created — nothing to flush.
        return

    logger.info(
        "Flushing Payment Service Kafka producer on shutdown",
        extra={"timeout_seconds": timeout},
    )

    loop = asyncio.get_event_loop()
    remaining: int = await loop.run_in_executor(
        None,
        lambda: _producer.flush(timeout),
    )

    if remaining > 0:
        logger.error(
            "Payment producer shutdown flush timed out",
            extra={
                "undelivered_count": remaining,
                "timeout_seconds":   timeout,
            },
        )
    else:
        logger.info(
            "Payment Service Kafka producer flushed cleanly — all events delivered"
        )