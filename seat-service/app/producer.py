# seat-service/app/producer.py
#
# Kafka producer for the Seat Service.
#
# Responsibilities:
#   - Publish SeatReservedEvent to `seat.reserved` when a Redis seat lock
#     is successfully acquired for a booking
#   - Publish SeatUnavailableEvent to `seat.unavailable` when a seat is
#     already locked by another booking (concurrent booking race condition)
#   - Publish BookingFailedDLQEvent to `booking.failed.dlq` for events
#     that cannot be processed after all retries are exhausted
#
# Design notes:
#   The Seat Service producer is the leanest of the four service producers
#   because seat events are small (no full passenger or payment details
#   embedded) and the Seat Service has no financial consequences — a missed
#   seat.reserved event does not charge a card, it only delays a booking.
#
#   Despite this, correctness is still critical:
#     - A missed seat.reserved event leaves the booking in REQUESTED state
#       indefinitely because Payment Service never receives the trigger to
#       charge the passenger.
#     - A missed seat.unavailable event leaves the booking in REQUESTED
#       state indefinitely because Booking Service never marks it FAILED.
#   Both cases require the passenger to contact support — so publish
#   correctness matters even without financial stakes.
#
#   Like all other service producers, a module-level singleton Producer is
#   used and reused across all messages. Creating a Producer per message
#   is expensive (TCP handshake, metadata fetch) and unnecessary.
#
#   linger.ms is set to 0 — lower than all other services. Seat events
#   are on the critical path between "passenger clicks Book" and "Payment
#   Service starts charging". Any batching delay here directly adds to the
#   passenger-perceived booking latency. Speed over throughput efficiency.

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from confluent_kafka import Producer, KafkaException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
    before_sleep_log,
)

from app.config import settings
from shared.schemas import (
    SeatReservedEvent,
    SeatUnavailableEvent,
    BookingFailedDLQEvent,
    serialize_event,
)


logger = logging.getLogger("seat-service.producer")


# ──────────────────────────────────────────────────────────────────────────────
# DELIVERY CALLBACK
# Invoked by librdkafka after each message is delivered or permanently fails.
# Seat delivery failures are logged at ERROR — a missed seat.reserved event
# stalls the booking indefinitely without charging the passenger.
# ──────────────────────────────────────────────────────────────────────────────

def _delivery_callback(err, msg) -> None:
    """
    Called by librdkafka once delivery succeeds or permanently fails.

    On failure: logs at ERROR with the booking_id (from the message key)
    so engineers can manually trigger the booking to be retried or failed.

    In production this callback should also:
      - Increment a SEAT_DELIVERY_FAILURES_TOTAL Prometheus counter
      - Write the failed event to a Redis fallback key so the consumer
        loop can detect and republish it on the next heartbeat cycle
    """
    if err:
        logger.error(
            "SEAT EVENT DELIVERY FAILED — booking may be stuck in REQUESTED state",
            extra={
                "topic":      msg.topic(),
                "partition":  msg.partition(),
                "error":      str(err),
                # Message key is always the booking_id — log it so the
                # affected booking can be found and manually resolved
                "booking_id": msg.key().decode("utf-8") if msg.key() else None,
            },
        )
    else:
        logger.debug(
            "Seat event delivered successfully",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
            },
        )


# ──────────────────────────────────────────────────────────────────────────────
# SINGLETON PRODUCER
# ──────────────────────────────────────────────────────────────────────────────

_producer: Optional[Producer] = None


def _get_producer() -> Producer:
    """
    Return the module-level Producer singleton, creating it on first call.

    Configuration priorities for the Seat Service:
      - linger.ms = 0  → send immediately, no batching delay.
                         Seat events are on the critical booking path.
      - message.timeout.ms = 10_000  → fail fast (10s vs 15s in Payment
                                        Service). If seat.reserved cannot
                                        be delivered in 10 seconds, the
                                        booking is already severely delayed.
      - queue.buffering.max.messages = 5000  → higher than Payment Service
                                               because seat events are small
                                               (no embedded passenger details)
                                               and the Seat Service handles
                                               more concurrent bookings per
                                               second than any other service.
    """
    global _producer

    if _producer is not None:
        return _producer

    config = {
        # Broker connection
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,

        # Maximum durability — all in-sync replicas must acknowledge.
        # A missed seat.reserved event stalls a booking indefinitely.
        "acks": "all",

        # Idempotent producer — prevents duplicate seat.reserved events
        # caused by producer retries. A duplicate seat.reserved would
        # trigger a duplicate Stripe charge attempt in Payment Service
        # (mitigated by Stripe's idempotency key, but still undesirable).
        "enable.idempotence": True,

        # Strict ordering within each partition.
        # seat.reserved must always precede any subsequent seat event for
        # the same booking_id — guaranteed by using booking_id as the key.
        "max.in.flight.requests.per.connection": 1,

        # Retry failed produce requests
        "retries": 5,
        "retry.backoff.ms": 200,

        # Zero linger — send immediately without batching.
        # Seat events are time-critical. Any linger delay adds directly
        # to the passenger-perceived booking confirmation time.
        # Trade-off: slightly less network efficiency vs lower latency.
        "linger.ms": 0,

        # Snappy compression — fast enough to not add latency while still
        # reducing broker network load during peak booking windows.
        "compression.type": "snappy",

        # Fail fast — if seat.reserved cannot be delivered in 10 seconds
        # the broker has a serious problem. Surface it quickly.
        "message.timeout.ms": 10_000,

        # Higher queue cap than Payment Service — seat events are small
        # (just seat_number, flight_id, booking_id, lock_ttl) and the
        # Seat Service processes more concurrent events per second.
        "queue.buffering.max.messages": 5_000,

        # Memory cap — 16MB is generous for small seat events
        "queue.buffering.max.kbytes": 16_384,   # 16 MB
    }

    _producer = Producer(config)
    logger.info(
        "Seat Service Kafka producer initialised",
        extra={"bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS},
    )
    return _producer


# ──────────────────────────────────────────────────────────────────────────────
# CORE PUBLISH HELPER
# All public publish_* functions delegate here.
# ──────────────────────────────────────────────────────────────────────────────

@retry(
    # Retry transient Kafka errors (broker restart, leader election).
    # Use jitter to prevent thundering herd if Kafka has a brief outage
    # and all seat events retry simultaneously.
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=0.5, max=8, jitter=0.5),
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
    Serialize and publish one seat event to a Kafka topic.

    Args:
        topic:   Target Kafka topic (e.g. "seat.reserved").
        key:     Message key — always booking_id so all seat events for
                 the same booking land on the same partition (ordering).
        event:   A Pydantic BaseEvent subclass from shared/schemas.py.
        headers: Optional metadata headers — used to carry correlation_id,
                 seat_number, and flight_id for monitoring tools that read
                 headers without deserializing the full payload.

    Raises:
        KafkaException: If all tenacity retries are exhausted.
    """
    producer = _get_producer()
    loop     = asyncio.get_event_loop()

    # Serialize Pydantic event to UTF-8 JSON bytes
    value_bytes = serialize_event(event).encode("utf-8")
    key_bytes   = key.encode("utf-8")

    # Convert headers dict to confluent-kafka list-of-tuples format
    kafka_headers = (
        [(k, v.encode("utf-8")) for k, v in headers.items()]
        if headers else []
    )

    # produce() is synchronous but non-blocking — places message in
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
    # Called after every produce() so delivery failures are logged
    # immediately rather than accumulating until the next flush().
    await loop.run_in_executor(
        None,
        lambda: producer.poll(0),
    )

    logger.debug(
        "Seat event queued for delivery",
        extra={
            "topic": topic,
            "key":   key,
        },
    )


async def _flush(timeout: float = 10.0) -> None:
    """
    Block until all buffered seat events are delivered or timeout expires.

    Timeout is 10s — matches message.timeout.ms in the producer config.
    Called after every seat event publish (not just on shutdown) because
    seat.reserved triggers Payment Service to charge the passenger. If the
    publish is only buffered and never delivered, the passenger waits forever.

    Args:
        timeout: Maximum seconds to wait for delivery confirmation.
    """
    producer = _get_producer()
    loop     = asyncio.get_event_loop()

    remaining: int = await loop.run_in_executor(
        None,
        lambda: producer.flush(timeout),
    )

    if remaining > 0:
        logger.error(
            "Seat Service Kafka flush timed out — seat events may not be delivered",
            extra={
                "undelivered_count": remaining,
                "timeout_seconds":   timeout,
            },
        )
    else:
        logger.debug("Seat Service Kafka flush completed — all events delivered")


# ──────────────────────────────────────────────────────────────────────────────
# PUBLIC PUBLISH FUNCTIONS
# One function per Kafka topic the Seat Service produces to.
# ──────────────────────────────────────────────────────────────────────────────

async def publish_seat_reserved(event: SeatReservedEvent) -> None:
    """
    Publish a SeatReservedEvent to the `seat.reserved` topic.

    Called by SeatService after successfully acquiring the Redis seat lock
    via SET NX EX. This event triggers:
      - Payment Service → charges the passenger via Stripe
      - Booking Service → updates booking status to SEAT_HELD

    The seat_number and flight_id are included as Kafka headers so
    monitoring dashboards can display real-time seat lock activity without
    deserializing the full payload on every event — useful during peak
    booking windows when thousands of events flow per second.

    The lock_ttl header tells consumers how long the Redis lock is valid.
    Payment Service uses this to prioritise bookings whose locks are close
    to expiring — ensuring the seat is charged before the lock expires and
    the seat becomes available for another booking.

    Args:
        event: The SeatReservedEvent — must have a valid booking_id,
               flight_id, seat_number, seat_class, and lock_ttl.
    """
    await _publish(
        topic=settings.TOPIC_SEAT_RESERVED,
        key=event.booking_id,
        event=event,
        headers={
            # Standard tracing headers read by all consumers
            "correlation_id": event.correlation_id,
            "event_type":     "seat.reserved",
            "service":        "seat-service",

            # Seat-specific headers for monitoring and prioritisation
            "seat_number":    event.seat_number,
            "flight_id":      event.flight_id,
            "seat_class":     event.seat_class.value,

            # Lock TTL as a header so Payment Service can read it without
            # deserializing the payload — enables TTL-aware prioritisation
            "lock_ttl":       str(event.lock_ttl),
        },
    )
    await _flush()

    logger.info(
        "SeatReservedEvent published — awaiting payment",
        extra={
            "booking_id":     event.booking_id,
            "correlation_id": event.correlation_id,
            "flight_id":      event.flight_id,
            "seat_number":    event.seat_number,
            "seat_class":     event.seat_class.value,
            "lock_ttl":       event.lock_ttl,
        },
    )


async def publish_seat_unavailable(event: SeatUnavailableEvent) -> None:
    """
    Publish a SeatUnavailableEvent to the `seat.unavailable` topic.

    Called by SeatService when the Redis SET NX EX command returns False —
    meaning another booking already holds the lock on this seat.

    Consumers:
      - Booking Service → marks booking status as FAILED in PostgreSQL
                          and routes to DLQ for notification
      - (Notification Service if wired to DLQ) → notifies passenger that
        their chosen seat is unavailable and prompts seat re-selection

    The reason header carries a short human-readable explanation that
    Booking Service can store in the failure_reason column without
    deserializing the full payload — useful for customer support queries
    like "why did this booking fail?" without joining multiple tables.

    Args:
        event: The SeatUnavailableEvent — must have booking_id, flight_id,
               seat_number, and a clear reason string.
    """
    await _publish(
        topic=settings.TOPIC_SEAT_UNAVAILABLE,
        key=event.booking_id,
        event=event,
        headers={
            # Standard tracing headers
            "correlation_id": event.correlation_id,
            "event_type":     "seat.unavailable",
            "service":        "seat-service",

            # Failure context headers — readable without payload deserialization
            "seat_number":    event.seat_number,
            "flight_id":      event.flight_id,
            "reason":         event.reason,
        },
    )
    await _flush()

    logger.warning(
        "SeatUnavailableEvent published — seat already locked by another booking",
        extra={
            "booking_id":     event.booking_id,
            "correlation_id": event.correlation_id,
            "flight_id":      event.flight_id,
            "seat_number":    event.seat_number,
            "reason":         event.reason,
        },
    )


async def publish_to_dlq(event: BookingFailedDLQEvent) -> None:
    """
    Publish a BookingFailedDLQEvent to the `booking.failed.dlq` topic.

    Called when a seat event cannot be processed after all retries —
    typically when Redis is unreachable for the entire retry window
    (which would be an extended infrastructure outage).

    Unlike the Payment Service DLQ, seat DLQ events do not indicate
    financial loss — no money has been charged. They do indicate a
    booking that will never be confirmed and requires manual intervention
    or a retry mechanism.

    No tenacity retry — DLQ writes must surface failures immediately as
    CRITICAL so on-call engineers are paged. Retrying a failed DLQ write
    with the same broken Kafka connection is unlikely to succeed.

    Args:
        event: The BookingFailedDLQEvent with original payload preserved
               in raw_payload and a clear error_message.
    """
    producer = _get_producer()
    loop     = asyncio.get_event_loop()

    value_bytes = serialize_event(event).encode("utf-8")
    key_bytes   = (event.booking_id or "unknown").encode("utf-8")

    # No tenacity retry on DLQ writes — fail fast and alert
    await loop.run_in_executor(
        None,
        lambda: producer.produce(
            topic=settings.TOPIC_BOOKING_FAILED_DLQ,
            key=key_bytes,
            value=value_bytes,
            headers=[
                ("event_type",    b"booking.failed.dlq"),
                ("service",       b"seat-service"),
                ("failed_topic",
                 event.failed_topic.encode("utf-8")),
                ("retry_count",
                 str(event.retry_count).encode("utf-8")),
            ],
            on_delivery=_delivery_callback,
        ),
    )

    # Flush synchronously and immediately — no buffering for DLQ events
    remaining: int = await loop.run_in_executor(
        None,
        lambda: producer.flush(timeout=10),
    )

    if remaining > 0:
        # Cannot write to DLQ — booking event permanently lost.
        # Log at CRITICAL so alerting fires immediately.
        logger.critical(
            "CRITICAL: Failed to write seat event to DLQ — event permanently lost",
            extra={
                "booking_id":    event.booking_id,
                "failed_topic":  event.failed_topic,
                "error_message": event.error_message,
                "retry_count":   event.retry_count,
            },
        )
    else:
        logger.error(
            "Seat event written to DLQ",
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
# all buffered seat events are delivered before the process exits.
# ──────────────────────────────────────────────────────────────────────────────

async def flush_and_close(timeout: float = 15.0) -> None:
    """
    Flush all pending seat events and mark the producer as closed.

    Called once from consumer.py during graceful shutdown after the poll
    loop exits. Seat events in the buffer at shutdown time are usually
    the result of the last message processed before SIGTERM fired.

    A seat.reserved event that is buffered but not yet delivered when the
    process exits means Payment Service never charges the passenger —
    the booking stalls in REQUESTED state until the Redis lock expires
    (300s) and the seat becomes available again. The 15s flush window
    maximises the chance of delivery before exit.

    Args:
        timeout: Maximum seconds to wait. 15s is longer than the per-message
                 flush (10s) because shutdown only happens once and the extra
                 5 seconds meaningfully increases delivery probability.
    """
    if _producer is None:
        # Producer was never created — nothing to flush
        return

    logger.info(
        "Flushing Seat Service Kafka producer on shutdown",
        extra={"timeout_seconds": timeout},
    )

    loop      = asyncio.get_event_loop()
    remaining = await loop.run_in_executor(
        None,
        lambda: _producer.flush(timeout),
    )

    if remaining > 0:
        logger.error(
            "Seat Service producer shutdown flush timed out",
            extra={
                "undelivered_count": remaining,
                "timeout_seconds":   timeout,
            },
        )
    else:
        logger.info(
            "Seat Service Kafka producer flushed cleanly — all seat events delivered"
        )