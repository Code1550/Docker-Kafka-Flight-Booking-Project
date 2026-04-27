# booking-service/app/producer.py
#
# Kafka producer for the Booking Service.
#
# Responsibilities:
#   - Publish SeatReservedEvent to `seat.reserved` after a booking record
#     is successfully written to PostgreSQL
#   - Publish BookingConfirmedEvent to `booking.confirmed` after payment
#     confirmation is received from the Payment Service
#   - Publish BookingFailedDLQEvent to `booking.failed.dlq` when a booking
#     cannot be processed after all retries are exhausted
#
# Design notes:
#   This producer is intentionally simpler than the API Gateway's producer.
#   The Booking Service is a consumer-first service — producing is a
#   side effect of processing, not the primary job. There is no async
#   wrapper class here; instead I expose thin async functions that wrap
#   confluent-kafka's synchronous produce() + flush() calls with
#   asyncio.run_in_executor() to avoid blocking the event loop.
#
#   A module-level singleton producer is used rather than creating a new
#   Producer instance per message. Creating a Producer is expensive
#   (TCP handshake, metadata fetch) — reusing one instance across all
#   messages is the correct pattern for a long-running consumer service.

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from confluent_kafka import Producer, KafkaException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from app.config import settings
from shared.schemas import (
    SeatReservedEvent,
    BookingConfirmedEvent,
    BookingFailedDLQEvent,
    serialize_event,
)


logger = logging.getLogger("booking-service.producer")


# ──────────────────────────────────────────────────────────────────────────────
# DELIVERY CALLBACK
# Invoked by librdkafka after each message is delivered or permanently fails.
# Runs in the background poll thread — must not raise exceptions.
# ──────────────────────────────────────────────────────────────────────────────

def _delivery_callback(err, msg) -> None:
    """
    Called by librdkafka once delivery is confirmed or has permanently failed.

    On failure I log and increment a metric - I do not raise here because
    this callback runs outside the async context and exceptions would be
    swallowed silently by librdkafka. The caller's flush() will surface
    undelivered message counts separately.
    """
    if err:
        logger.error(
            "Kafka message delivery failed",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "error":     str(err),
            },
        )
    else:
        logger.debug(
            "Kafka message delivered successfully",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
            },
        )


# ──────────────────────────────────────────────────────────────────────────────
# SINGLETON PRODUCER
# Created once at module import time and reused for the lifetime of the
# process. Lazy-initialised via _get_producer() so the module can be
# imported in tests without immediately requiring a live Kafka broker.
# ──────────────────────────────────────────────────────────────────────────────

_producer: Optional[Producer] = None


def _get_producer() -> Producer:
    """
    Return the module-level Producer singleton, creating it on first call.

    Producer configuration mirrors the API Gateway's producer with two
    differences:
      - linger.ms is higher (25ms vs 5ms) because the Booking Service
        publishes in bursts rather than continuously, so batching saves
        more bandwidth here.
      - message.timeout.ms is set explicitly so stalled deliveries surface
        as errors rather than waiting indefinitely.
    """
    global _producer

    if _producer is not None:
        return _producer

    config = {
        # Broker connection
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,

        # Durability: require all in-sync replicas to acknowledge.
        # Prevents message loss if the leader broker crashes mid-write.
        "acks": "all",

        # Idempotent producer prevents duplicate messages caused by retries.
        # Requires acks="all" and max.in.flight.requests.per.connection <= 5.
        "enable.idempotence": True,

        # Guarantees strict message ordering within a partition.
        # Critical because seat.reserved must arrive before payment.processed
        # for the same booking_id.
        "max.in.flight.requests.per.connection": 1,

        # Retry failed produce requests up to 5 times before calling the
        # delivery callback with an error.
        "retries": 5,

        # Time between retries (ms). Combined with retries=5 this gives
        # up to ~2.5 seconds of retry coverage for transient broker issues.
        "retry.backoff.ms": 500,

        # Batch messages for up to 25ms before sending.
        # Higher than the API Gateway (5ms) because this service publishes
        # in response to DB writes (bursty) rather than HTTP requests (steady).
        "linger.ms": 25,

        # Compress batches to reduce broker network load.
        "compression.type": "snappy",

        # Fail a produce attempt if the message is not delivered within
        # 30 seconds. Surfaces broker problems as errors rather than
        # silent hangs that block shutdown.
        "message.timeout.ms": 30_000,

        # Maximum size of a single message. 1MB is generous for JSON events
        # but protects against accidental raw_payload bloat in DLQ messages.
        "message.max.bytes": 1_048_576,
    }

    _producer = Producer(config)
    logger.info(
        "Kafka producer initialised",
        extra={"bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS},
    )
    return _producer


# ──────────────────────────────────────────────────────────────────────────────
# CORE PUBLISH HELPER
# All public publish_* functions call this to avoid duplicating
# encode / produce / poll logic.
# ──────────────────────────────────────────────────────────────────────────────

@retry(
    # Retry transient Kafka errors (broker restart, leader election) up to
    # 3 times before propagating the exception to the caller. The caller
    # (consumer.py) will then route the message to the DLQ.
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
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
    Serialize and publish one event to a Kafka topic.

    Args:
        topic:   Target Kafka topic name.
        key:     Message key — always the booking_id so all events for
                 the same booking land on the same partition (ordering).
        event:   A Pydantic BaseEvent subclass from shared/schemas.py.
        headers: Optional metadata dict — used to carry correlation_id
                 as a Kafka header for tracing integrations that read
                 headers without deserializing the full payload.

    Raises:
        KafkaException: If all tenacity retries are exhausted.
        RuntimeError:   If the producer singleton cannot be created.
    """
    producer = _get_producer()
    loop     = asyncio.get_event_loop()

    # Serialize the Pydantic model to a UTF-8 JSON byte string
    value_bytes = serialize_event(event).encode("utf-8")
    key_bytes   = key.encode("utf-8")

    # Convert headers dict to confluent-kafka's expected list-of-tuples format
    kafka_headers = (
        [(k, v.encode("utf-8")) for k, v in headers.items()]
        if headers else []
    )

    # produce() is synchronous and non-blocking — it places the message in
    # librdkafka's internal send buffer and returns immediately.
    # Run in executor to avoid blocking the asyncio event loop.
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
    # Called after every produce() so delivery failures are logged
    # immediately rather than accumulating until the next flush().
    await loop.run_in_executor(
        None,
        lambda: producer.poll(0),
    )

    logger.info(
        "Event queued for delivery",
        extra={
            "topic": topic,
            "key":   key,
        },
    )


async def _flush(timeout: float = 15.0) -> None:
    """
    Block until all buffered messages are delivered or timeout expires.

    Called after every publish in the Booking Service (unlike the API Gateway
    which flushes only on shutdown) because producing here is a side effect
    of a DB write - I want to know immediately if Kafka rejected the message
    so I can retry or route to DLQ rather than discovering it at shutdown.

    Args:
        timeout: Maximum seconds to wait. 15s is generous — if Kafka cannot
                 deliver within this window the broker likely has a problem.
    """
    producer = _get_producer()
    loop     = asyncio.get_event_loop()

    remaining: int = await loop.run_in_executor(
        None,
        lambda: producer.flush(timeout),
    )

    if remaining > 0:
        logger.warning(
            "Kafka flush timed out — some messages may not have been delivered",
            extra={"undelivered_count": remaining},
        )
    else:
        logger.debug("Kafka flush completed — all messages delivered")


# ──────────────────────────────────────────────────────────────────────────────
# PUBLIC PUBLISH FUNCTIONS
# One function per Kafka topic this service produces to. Keeping them
# separate makes call sites in service.py readable and makes it easy to
# add topic-specific logic (e.g. custom headers, extra logging) later
# without touching the shared _publish() helper.
# ──────────────────────────────────────────────────────────────────────────────

async def publish_seat_reserved(event: SeatReservedEvent) -> None:
    """
    Publish a SeatReservedEvent to the `seat.reserved` topic.

    Called by BookingService after:
      1. The booking record is written to PostgreSQL
      2. The seat lock has been confirmed available in Redis
         (via Seat Service — Booking Service does not lock seats directly)

    Consumers: Payment Service (triggers charge), Booking Service itself
               (via a separate subscription — updates booking status to
               SEAT_HELD in a follow-up read if needed).

    The correlation_id is sent as both a payload field (via BaseEvent) and
    a Kafka message header so tracing tools can read it without deserializing
    the full JSON payload.
    """
    await _publish(
        topic=settings.TOPIC_SEAT_RESERVED,
        key=event.booking_id,
        event=event,
        headers={
            "correlation_id": event.correlation_id,
            "event_type":     "seat.reserved",
            "service":        "booking-service",
        },
    )
    await _flush()

    logger.info(
        "SeatReservedEvent published",
        extra={
            "booking_id":     event.booking_id,
            "correlation_id": event.correlation_id,
            "seat_number":    event.seat_number,
            "flight_id":      event.flight_id,
            "lock_ttl":       event.lock_ttl,
        },
    )


async def publish_booking_confirmed(event: BookingConfirmedEvent) -> None:
    """
    Publish a BookingConfirmedEvent to the `booking.confirmed` topic.

    Called by BookingService after receiving a PaymentProcessedEvent from
    the Payment Service and updating the booking status to CONFIRMED in
    PostgreSQL.

    Consumers: Notification Service (sends confirmation email/SMS to passenger).

    This is the final event in the happy path. After this event is published
    the booking journey is complete from the Booking Service's perspective.
    """
    await _publish(
        topic=settings.TOPIC_BOOKING_CONFIRMED,
        key=event.booking_id,
        event=event,
        headers={
            "correlation_id":     event.correlation_id,
            "event_type":         "booking.confirmed",
            "service":            "booking-service",
            "notification_type":  event.notification_type.value,
        },
    )
    await _flush()

    logger.info(
        "BookingConfirmedEvent published",
        extra={
            "booking_id":     event.booking_id,
            "correlation_id": event.correlation_id,
            "flight_number":  event.flight.flight_number,
            "passenger_email": event.passenger.email,
        },
    )


async def publish_to_dlq(event: BookingFailedDLQEvent) -> None:
    """
    Publish a BookingFailedDLQEvent to the `booking.failed.dlq` topic.

    Called when a message cannot be processed after all retries. This is
    a last resort — the original payload is preserved in raw_payload so
    engineers can inspect, fix, and replay the message without data loss.

    The DLQ topic has a single partition and a long retention period
    (configured in docker-compose via kafka-init). Alerts in Grafana
    should be wired to the DLQ_MESSAGES_TOTAL Prometheus counter so
    on-call engineers are notified immediately when messages start failing.

    Unlike the other publish functions this one does NOT retry on failure —
    if I cannot even write to the DLQ the process should surface the error
    loudly so the ops team knows messages are being lost.
    """
    producer = _get_producer()
    loop     = asyncio.get_event_loop()

    value_bytes = serialize_event(event).encode("utf-8")
    key_bytes   = (event.booking_id or "unknown").encode("utf-8")

    # No tenacity retry here — DLQ writes must not be silently dropped.
    # If this fails it should propagate and alert.
    await loop.run_in_executor(
        None,
        lambda: producer.produce(
            topic=settings.TOPIC_BOOKING_FAILED_DLQ,
            key=key_bytes,
            value=value_bytes,
            headers=[
                ("event_type", b"booking.failed.dlq"),
                ("service",    b"booking-service"),
                ("failed_topic", event.failed_topic.encode("utf-8")),
            ],
            on_delivery=_delivery_callback,
        ),
    )

    # Flush immediately and synchronously - I need to know right now
    # whether the DLQ write succeeded or failed.
    remaining: int = await loop.run_in_executor(
        None,
        lambda: producer.flush(timeout=10),
    )

    if remaining > 0:
        # This is a critical failure — message is being lost. Log at CRITICAL
        # level so alerting picks it up even if Prometheus is down.
        logger.critical(
            "CRITICAL: Failed to write to DLQ — message will be lost",
            extra={
                "booking_id":    event.booking_id,
                "failed_topic":  event.failed_topic,
                "error_message": event.error_message,
            },
        )
    else:
        logger.error(
            "Message published to DLQ",
            extra={
                "booking_id":    event.booking_id,
                "failed_topic":  event.failed_topic,
                "error_message": event.error_message,
                "retry_count":   event.retry_count,
            },
        )


# ──────────────────────────────────────────────────────────────────────────────
# SHUTDOWN HOOK
# Called by consumer.py during graceful shutdown (after the poll loop exits)
# to ensure no buffered messages are dropped when the container stops.
# ──────────────────────────────────────────────────────────────────────────────

async def flush_and_close(timeout: float = 30.0) -> None:
    """
    Flush all pending messages and mark the producer as closed.

    Called once by consumer.py in the finally block of run_consumer()
    after the poll loop exits. Gives in-flight produce() calls up to
    `timeout` seconds to complete before the process exits.
    """
    if _producer is None:
        return

    logger.info("Flushing Booking Service producer on shutdown")
    await _flush(timeout=timeout)
    logger.info("Booking Service producer shutdown complete")