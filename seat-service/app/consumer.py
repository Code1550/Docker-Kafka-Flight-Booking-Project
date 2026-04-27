# seat-service/app/consumer.py
#
# Main entry point for the Seat Service.
#
# This module runs a long-lived asyncio event loop that subscribes to
# TWO Kafka topics simultaneously - unlike other services that each
# consume from a single topic:
#
#   booking.requested  → acquire a Redis seat lock, publish seat.reserved
#                        or seat.unavailable based on lock outcome
#
#   payment.failed     → release the Redis seat lock so the seat becomes
#                        available again for other passengers to book
#
# Subscribing to multiple topics on a single consumer is more efficient
# than running two separate consumers because:
#   - One Kafka connection instead of two (lower broker overhead)
#   - One consumer group registration (simpler rebalance logic)
#   - Shared sentinel file and Prometheus metrics server
#   - Lock acquisition and release stay in the same process - easier
#     to reason about the lifecycle of any individual seat lock
#
# Concurrency model:
#   Same pattern as other services - confluent-kafka's synchronous poll()
#   runs in asyncio's thread pool executor. Redis lock operations (SET NX EX,
#   DEL) are async-native via redis-py's async client and run directly in
#   the event loop without executor wrapping.
#
# Offset management:
#   enable.auto.commit is False. Offsets are committed manually after each
#   message is fully processed (lock acquired/released AND Kafka event
#   published). This is especially important here because a committed offset
#   with no Redis lock operation would leave the seat in an inconsistent
#   state - either locked with no event published (ghost lock) or unlocked
#   with a seat.reserved event already consumed by Payment Service
#   (phantom booking).
#
# Idempotency:
#   - For booking.requested: SeatService checks whether a lock for the
#     booking_id already exists in Redis before attempting SET NX EX.
#     If it does (re-delivery after crash), the existing lock result is
#     returned and the corresponding event is re-published.
#   - For payment.failed: Redis DEL is idempotent — deleting a key that
#     does not exist is a no-op. Safe to re-deliver without special handling.

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
from app.service import SeatService
from app.cache.redis_client import create_redis_client, close_redis_client
from app.cache.seat_lock_manager import SeatLockManager
from shared.schemas import (
    BookingRequestedEvent,
    PaymentFailedEvent,
    BookingFailedDLQEvent,
    deserialize_event,
    serialize_event,
)


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# Structured JSON logging — every seat lock log line includes booking_id,
# correlation_id, seat_number, flight_id, and lock_result as structured
# fields. Critical for debugging race conditions where two bookings compete
# for the same seat simultaneously and both appear in logs at the same time.
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("seat-service.consumer")
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
# Exposed on port 8003. Does not conflict with:
#   API Gateway      → no metrics server (uses /metrics route instead)
#   Booking Service  → port 8001
#   Payment Service  → port 8002
# ──────────────────────────────────────────────────────────────────────────────

# Total lock acquisition attempts broken down by outcome
SEAT_LOCK_ACQUISITIONS_TOTAL = Counter(
    "seat_service_lock_acquisitions_total",
    "Total seat lock acquisition attempts",
    ["status"],     # labels: "acquired" | "already_locked" | "redis_error"
)

# Total lock release operations broken down by trigger
SEAT_LOCK_RELEASES_TOTAL = Counter(
    "seat_service_lock_releases_total",
    "Total seat lock release operations",
    ["reason"],     # labels: "payment_failed" | "not_found" | "redis_error"
)

# How long each lock acquisition attempt takes (Redis round-trip)
LOCK_ACQUISITION_DURATION = Histogram(
    "seat_service_lock_acquisition_duration_seconds",
    "Time taken to acquire or reject a seat lock in Redis",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
    # Upper buckets are very short — Redis operations should complete
    # in milliseconds. Anything over 100ms indicates a Redis performance
    # issue worth investigating.
)

# End-to-end message processing time including Redis + Kafka operations
MESSAGE_PROCESSING_DURATION = Histogram(
    "seat_service_message_processing_duration_seconds",
    "Time taken to fully process one Kafka message end-to-end",
    ["topic"],      # label: topic name distinguishes lock vs release latency
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# Total messages consumed broken down by topic and outcome
MESSAGES_CONSUMED_TOTAL = Counter(
    "seat_service_messages_consumed_total",
    "Total Kafka messages consumed by topic and outcome",
    ["topic", "status"],    # status: "success" | "dlq" | "deserialization_error"
)

# DLQ counter — alert when this rises
DLQ_MESSAGES_TOTAL = Counter(
    "seat_service_dlq_messages_total",
    "Total messages routed to the dead-letter queue by Seat Service",
)

# Tracks concurrent seat conflicts — spikes indicate inventory display issues
CONCURRENT_SEAT_CONFLICTS_TOTAL = Counter(
    "seat_service_concurrent_seat_conflicts_total",
    "Total seat lock failures due to the seat already being locked by another booking",
)

# Tracks currently held locks — useful for capacity monitoring
ACTIVE_SEAT_LOCKS = Gauge(
    "seat_service_active_seat_locks",
    "Number of Redis seat locks currently held by the Seat Service",
)

# Redis connection health — 1 = healthy, 0 = unhealthy
REDIS_HEALTH = Gauge(
    "seat_service_redis_healthy",
    "1 if the Redis connection is healthy, 0 if not",
)


# ──────────────────────────────────────────────────────────────────────────────
# SENTINEL FILE
# Touched after every successfully processed message (lock acquired, lock
# rejected, or lock released — all count as liveness signals).
# Docker HEALTHCHECK reads this file's modification time to detect stalls.
# Path is unique to Seat Service to avoid collision with other services.
# ──────────────────────────────────────────────────────────────────────────────

HEARTBEAT_FILE = Path("/tmp/seat_consumer_heartbeat")


def _update_heartbeat() -> None:
    """Touch sentinel file to signal the consumer is alive and processing."""
    HEARTBEAT_FILE.touch(exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# GRACEFUL SHUTDOWN
# SIGTERM fires when `docker compose stop` is called. The handler sets a flag
# that the poll loop checks between messages. This ensures the current seat
# lock operation (acquire or release) always completes before the process
# exits — preventing ghost locks or missed lock releases on shutdown.
# ──────────────────────────────────────────────────────────────────────────────

_shutdown_requested: bool = False


def _handle_shutdown(signum, frame) -> None:
    """
    Signal handler for SIGTERM and SIGINT.

    Sets the shutdown flag so the consumer loop exits cleanly after
    completing the current message. Never interrupts mid-lock-operation
    because the flag is only checked between messages in the poll loop.
    """
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
    Construct a configured confluent-kafka Consumer for the Seat Service.

    Key configuration differences from other service consumers:

    max.poll.interval.ms = 120_000 (2 minutes):
      Lower than Payment Service (10 minutes) because seat lock operations
      are fast — Redis SET NX EX completes in milliseconds. If processing
      takes longer than 2 minutes, the consumer is stuck (not processing).
      A shorter timeout detects stuck consumers faster and triggers a
      rebalance so another instance can take over.

    fetch.min.bytes = 1:
      Low latency over high throughput — seat lock acquisitions are
      time-sensitive. A passenger waiting for their booking to be confirmed
      experiences the seat.reserved → payment.processed → booking.confirmed
      chain. Any delay in seat locking directly delays the confirmation.

    session.timeout.ms = 20_000:
      Shorter than other services (30s) because seat lock operations are
      fast. If the Seat Service stops sending heartbeats for 20 seconds,
      something is genuinely wrong (Redis connection lost, process hung)
      and a rebalance should trigger quickly.
    """
    config = {
        "bootstrap.servers":      settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id":               settings.KAFKA_CONSUMER_GROUP,

        # Start from earliest — ensures no booking.requested events are
        # missed on restart. A missed event means the seat is never locked,
        # Payment Service charges the card, but the seat could be double-booked
        # because no Redis lock was ever acquired.
        "auto.offset.reset":      "earliest",

        # Manual offset commit — commit only after lock operation AND
        # Kafka event publish both succeed.
        "enable.auto.commit":     False,

        # Shorter than Payment Service — seat operations are fast
        "max.poll.interval.ms":   120_000,   # 2 minutes

        # Aggressive heartbeat — detect consumer failures quickly
        "heartbeat.interval.ms":  2_000,     # 2 seconds

        # Shorter session timeout than other services
        "session.timeout.ms":     20_000,    # 20 seconds

        # Low minimum fetch for low-latency processing
        "fetch.min.bytes":        1,

        # Short fetch wait — respond immediately when any message is available
        "fetch.wait.max.ms":      100,

        # Standard per-partition fetch cap
        "max.partition.fetch.bytes": 1_048_576,   # 1 MB
    }

    return Consumer(config)


# ──────────────────────────────────────────────────────────────────────────────
# DLQ PRODUCER
# Thin inline producer for routing unprocessable events to the DLQ.
# Same pattern as other services — separate from the main producer in
# producer.py so DLQ writes work even if the main producer is degraded.
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
    failed_topic: str,
) -> None:
    """
    Publish an unprocessable event to the dead-letter queue.

    Called when:
      - A message cannot be deserialized (malformed JSON, unknown topic)
      - A seat lock operation fails after all retries are exhausted
      - A booking_id cannot be matched to a seat number for lock release

    Unlike Payment Service DLQ writes, seat DLQ events are less critical
    because no money has changed hands — the passenger has not been charged.
    However they must still be monitored because a missed seat.reserved event
    means Payment Service never charges the passenger and the booking stalls.
    """
    dlq_event = BookingFailedDLQEvent(
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
            ("event_type",   b"booking.failed.dlq"),
            ("service",      b"seat-service"),
            ("failed_topic", failed_topic.encode("utf-8")),
        ],
    )

    remaining = producer.flush(timeout=10)
    DLQ_MESSAGES_TOTAL.inc()

    if remaining > 0:
        logger.critical(
            "CRITICAL: Failed to write seat event to DLQ — event will be lost",
            extra={
                "booking_id":    booking_id,
                "failed_topic":  failed_topic,
                "error_message": error_message,
            },
        )
    else:
        logger.error(
            "Seat event routed to DLQ",
            extra={
                "booking_id":    booking_id,
                "failed_topic":  failed_topic,
                "error_message": error_message,
                "retry_count":   retry_count,
            },
        )


# ──────────────────────────────────────────────────────────────────────────────
# MESSAGE ROUTER
# Dispatches messages to the correct handler based on topic name.
# The Seat Service subscribes to two topics — routing must be explicit
# and fast because every millisecond of seat lock latency delays the
# passenger's booking confirmation.
# ──────────────────────────────────────────────────────────────────────────────

async def _route_message(
    msg: Message,
    seat_service: SeatService,
) -> bool:
    """
    Route a Kafka message to the appropriate handler based on its topic.

    Returns:
        True  — message processed successfully, safe to commit offset.
        False — message routed to DLQ, offset should still be committed.

    Never raises — all exceptions are caught internally so the poll loop
    is never interrupted by a single bad message.
    """
    topic = msg.topic()
    raw   = msg.value().decode("utf-8") if msg.value() else ""

    # ── ROUTE: booking.requested → acquire seat lock ──────────────────────────
    if topic == settings.TOPIC_BOOKING_REQUESTED:
        return await _handle_booking_requested(
            msg=msg,
            raw=raw,
            seat_service=seat_service,
        )

    # ── ROUTE: payment.failed → release seat lock ─────────────────────────────
    elif topic == settings.TOPIC_PAYMENT_FAILED:
        return await _handle_payment_failed(
            msg=msg,
            raw=raw,
            seat_service=seat_service,
        )

    # ── UNKNOWN TOPIC ─────────────────────────────────────────────────────────
    # Should never happen — the consumer only subscribes to the two topics
    # above. If it does, route to DLQ rather than silently dropping.
    else:
        logger.error(
            "Received message from unexpected topic — routing to DLQ",
            extra={
                "topic":  topic,
                "offset": msg.offset(),
            },
        )
        _send_to_dlq(
            raw_payload=raw,
            error_message=f"Unexpected topic: {topic}",
            booking_id=None,
            retry_count=0,
            failed_topic=topic,
        )
        return False


# ──────────────────────────────────────────────────────────────────────────────
# BOOKING.REQUESTED HANDLER
# Attempts to acquire a Redis seat lock for the requested seat.
# ──────────────────────────────────────────────────────────────────────────────

async def _handle_booking_requested(
    msg: Message,
    raw: str,
    seat_service: SeatService,
) -> bool:
    """
    Process a booking.requested message by acquiring a Redis seat lock.

    Steps:
      1. Deserialize BookingRequestedEvent from raw JSON
      2. Attempt Redis SET NX EX on the seat lock key
      3. If acquired  → publish SeatReservedEvent to seat.reserved
      4. If rejected  → publish SeatUnavailableEvent to seat.unavailable
      5. Route to DLQ on deserialization failure or exhausted retries

    The seat lock key format is:
      seat_lock:{flight_id}:{seat_number}
    e.g. seat_lock:f9e8d7c6-...:12A

    This key format ensures two bookings for the same seat on the same
    flight conflict (same key) while two bookings for different seats
    on the same flight do not (different keys).

    Args:
        msg:          The raw Kafka Message object.
        raw:          The decoded string payload.
        seat_service: The SeatService instance with lock logic.

    Returns:
        True on success or seat unavailable (both commit the offset).
        False on deserialization failure or DLQ routing.
    """
    topic = msg.topic()

    # ── STEP 1: Deserialize ───────────────────────────────────────────────────
    try:
        event: BookingRequestedEvent = deserialize_event(topic, raw)
    except Exception as exc:
        logger.error(
            "Failed to deserialize booking.requested message",
            extra={
                "topic":  topic,
                "offset": msg.offset(),
                "error":  str(exc),
                "raw":    raw[:300],
            },
        )
        MESSAGES_CONSUMED_TOTAL.labels(
            topic=topic,
            status="deserialization_error",
        ).inc()
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
    seat_number    = event.flight.seat_number
    flight_id      = event.flight.flight_id

    logger.info(
        "Processing booking.requested — attempting seat lock",
        extra={
            "booking_id":     booking_id,
            "correlation_id": correlation_id,
            "flight_id":      flight_id,
            "seat_number":    seat_number,
            "seat_class":     event.flight.seat_class.value,
            "lock_ttl":       settings.SEAT_LOCK_TTL_SECONDS,
        },
    )

    # ── STEP 2 & 3: Acquire lock and publish result ───────────────────────────
    MAX_RETRIES = settings.PROCESSING_RETRY_ATTEMPTS   # default: 3

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with MESSAGE_PROCESSING_DURATION.labels(topic=topic).time():
                with LOCK_ACQUISITION_DURATION.time():
                    result = await seat_service.handle_booking_requested(
                        event=event,
                    )

            # result dict from SeatService:
            # {
            #   "lock_acquired": bool,
            #   "lock_key":      str,    # seat_lock:{flight_id}:{seat_number}
            #   "reason":        str | None,  # populated when lock_acquired=False
            # }

            if result["lock_acquired"]:
                SEAT_LOCK_ACQUISITIONS_TOTAL.labels(status="acquired").inc()
                ACTIVE_SEAT_LOCKS.inc()
            else:
                SEAT_LOCK_ACQUISITIONS_TOTAL.labels(status="already_locked").inc()
                CONCURRENT_SEAT_CONFLICTS_TOTAL.inc()

            MESSAGES_CONSUMED_TOTAL.labels(
                topic=topic,
                status="success",
            ).inc()
            _update_heartbeat()

            logger.info(
                "Seat lock decision made",
                extra={
                    "booking_id":   booking_id,
                    "seat_number":  seat_number,
                    "lock_acquired": result["lock_acquired"],
                    "reason":       result.get("reason"),
                    "attempt":      attempt,
                },
            )
            return True

        except Exception as exc:
            # Transient errors — Redis connection blip, Kafka publish failure
            is_last_attempt = attempt == MAX_RETRIES

            SEAT_LOCK_ACQUISITIONS_TOTAL.labels(status="redis_error").inc()
            REDIS_HEALTH.set(0)

            logger.warning(
                "Error processing booking.requested",
                extra={
                    "booking_id":  booking_id,
                    "seat_number": seat_number,
                    "attempt":     attempt,
                    "max_retries": MAX_RETRIES,
                    "error":       str(exc),
                    "will_retry":  not is_last_attempt,
                },
            )

            if is_last_attempt:
                MESSAGES_CONSUMED_TOTAL.labels(
                    topic=topic,
                    status="dlq",
                ).inc()
                _send_to_dlq(
                    raw_payload=raw,
                    error_message=(
                        f"Seat lock acquisition failed after {MAX_RETRIES} "
                        f"attempts: {exc}"
                    ),
                    booking_id=booking_id,
                    retry_count=attempt,
                    failed_topic=topic,
                )
                return False

            # Exponential backoff with jitter — prevents thundering herd
            # when Redis recovers from a brief outage
            import random
            backoff = min(2 ** attempt, 10) + random.uniform(0, 0.5)
            logger.info(
                f"Retrying seat lock in {backoff:.1f}s",
                extra={
                    "booking_id": booking_id,
                    "attempt":    attempt,
                },
            )
            await asyncio.sleep(backoff)

    return False


# ──────────────────────────────────────────────────────────────────────────────
# PAYMENT.FAILED HANDLER
# Releases the Redis seat lock so the seat becomes available again.
# ──────────────────────────────────────────────────────────────────────────────

async def _handle_payment_failed(
    msg: Message,
    raw: str,
    seat_service: SeatService,
) -> bool:
    """
    Process a payment.failed message by releasing the Redis seat lock.

    Called when Payment Service could not charge the passenger — either
    a card decline or exhausted retries. The seat must be released so
    other passengers can book it.

    Steps:
      1. Deserialize PaymentFailedEvent from raw JSON
      2. Call SeatService.handle_payment_failed() which runs Redis DEL
         on the seat lock key
      3. Update metrics and heartbeat

    Redis DEL is idempotent — deleting a key that does not exist returns
    0 (not an error). This makes payment.failed handling naturally
    idempotent — re-delivery after a crash is always safe.

    Why no DLQ for lock release failures?
      If Redis DEL fails (Redis is down), the seat lock will expire
      automatically after SEAT_LOCK_TTL_SECONDS (default 300s). The seat
      becomes available again without any action from the Seat Service.
      Routing to DLQ would be misleading — the situation self-heals.
      I log a warning and retry instead.

    Args:
        msg:          The raw Kafka Message object.
        raw:          The decoded string payload.
        seat_service: The SeatService instance with lock logic.

    Returns:
        True on success or benign lock-not-found (both commit the offset).
        False only on deserialization failure (permanent error).
    """
    topic = msg.topic()

    # ── STEP 1: Deserialize ───────────────────────────────────────────────────
    try:
        event: PaymentFailedEvent = deserialize_event(topic, raw)
    except Exception as exc:
        logger.error(
            "Failed to deserialize payment.failed message",
            extra={
                "topic":  topic,
                "offset": msg.offset(),
                "error":  str(exc),
                "raw":    raw[:300],
            },
        )
        MESSAGES_CONSUMED_TOTAL.labels(
            topic=topic,
            status="deserialization_error",
        ).inc()
        # Unlike booking.requested deserialization failures, I do not
        # route payment.failed deserialization errors to the DLQ because:
        #   - No seat lock action was taken (nothing to roll back)
        #   - The lock TTL will expire automatically (self-healing)
        #   - DLQ routing would trigger a false alarm in monitoring
        # Log and return False (commit offset) to unblock the partition.
        return False

    booking_id     = event.booking_id
    correlation_id = event.correlation_id

    logger.info(
        "Processing payment.failed — releasing seat lock",
        extra={
            "booking_id":     booking_id,
            "correlation_id": correlation_id,
            "failure_reason": event.payment.failure_reason,
            "retry_attempt":  event.retry_attempt,
        },
    )

    # ── STEP 2: Release lock with retry ───────────────────────────────────────
    MAX_RETRIES = settings.PROCESSING_RETRY_ATTEMPTS

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with MESSAGE_PROCESSING_DURATION.labels(topic=topic).time():
                result = await seat_service.handle_payment_failed(
                    event=event,
                )

            # result dict from SeatService:
            # {
            #   "lock_released": bool,   # True if DEL deleted a key
            #   "lock_key":      str,    # the key that was deleted
            #   "was_present":   bool,   # False if lock had already expired
            # }

            if result["lock_released"]:
                SEAT_LOCK_RELEASES_TOTAL.labels(reason="payment_failed").inc()
                ACTIVE_SEAT_LOCKS.dec()
                REDIS_HEALTH.set(1)
                logger.info(
                    "Seat lock released successfully",
                    extra={
                        "booking_id": booking_id,
                        "lock_key":   result["lock_key"],
                    },
                )
            else:
                # Lock was not present — either already expired via TTL or
                # never acquired (booking.requested went to DLQ).
                # This is not an error — log at INFO and move on.
                SEAT_LOCK_RELEASES_TOTAL.labels(reason="not_found").inc()
                logger.info(
                    "Seat lock not found on release — may have expired via TTL",
                    extra={
                        "booking_id": booking_id,
                        "lock_key":   result.get("lock_key"),
                    },
                )

            MESSAGES_CONSUMED_TOTAL.labels(
                topic=topic,
                status="success",
            ).inc()
            _update_heartbeat()
            return True

        except Exception as exc:
            is_last_attempt = attempt == MAX_RETRIES

            SEAT_LOCK_RELEASES_TOTAL.labels(reason="redis_error").inc()
            REDIS_HEALTH.set(0)

            logger.warning(
                "Error releasing seat lock",
                extra={
                    "booking_id": booking_id,
                    "attempt":    attempt,
                    "max_retries": MAX_RETRIES,
                    "error":      str(exc),
                    "will_retry": not is_last_attempt,
                },
            )

            if is_last_attempt:
                # All retries exhausted — lock will expire via TTL.
                # Log at WARNING (not ERROR) because the situation is
                # self-healing — no DLQ routing for lock release failures.
                logger.warning(
                    "Failed to release seat lock after all retries — "
                    "lock will expire automatically via TTL",
                    extra={
                        "booking_id": booking_id,
                        "ttl_seconds": settings.SEAT_LOCK_TTL_SECONDS,
                    },
                )
                # Return True (not False) — commit the offset so the
                # partition is not blocked. The TTL self-heals this case.
                MESSAGES_CONSUMED_TOTAL.labels(
                    topic=topic,
                    status="success",
                ).inc()
                _update_heartbeat()
                return True

            import random
            backoff = min(2 ** attempt, 8) + random.uniform(0, 0.5)
            await asyncio.sleep(backoff)

    return True


# ──────────────────────────────────────────────────────────────────────────────
# CONSUMER LOOP
# ──────────────────────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential_jitter(initial=2, max=60, jitter=2),
    retry=retry_if_exception_type(KafkaException),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def _run_consumer_loop(
    consumer: Consumer,
    seat_service: SeatService,
) -> None:
    """
    Core poll loop — runs until shutdown signal or fatal KafkaException.

    Subscribes to BOTH booking.requested AND payment.failed.
    Each message is routed to the appropriate handler by _route_message()
    based on the msg.topic() value.

    The poll timeout is 0.5s (shorter than other services at 1.0s) because
    seat lock latency directly affects passenger-perceived booking speed.
    A 1.0s poll timeout means in the worst case a booking.requested event
    waits 1 second before being processed — during peak booking windows
    this delay compounds across thousands of concurrent bookings.
    """
    loop = asyncio.get_event_loop()

    logger.info(
        "Seat Service consumer loop started",
        extra={
            "topics": [
                settings.TOPIC_BOOKING_REQUESTED,
                settings.TOPIC_PAYMENT_FAILED,
            ],
            "consumer_group": settings.KAFKA_CONSUMER_GROUP,
        },
    )

    while not _shutdown_requested:

        # ── POLL ──────────────────────────────────────────────────────────────
        # 0.5s timeout for lower seat lock latency
        msg: Message | None = await loop.run_in_executor(
            None,
            lambda: consumer.poll(timeout=0.5),
        )

        # ── NO MESSAGE ────────────────────────────────────────────────────────
        if msg is None:
            continue

        # ── KAFKA ERRORS ──────────────────────────────────────────────────────
        if msg.error():
            error = msg.error()

            if error.code() == KafkaError._PARTITION_EOF:
                # End of partition — normal when caught up to latest offset
                logger.debug(
                    "End of partition",
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
            "Message received",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
                "key":       msg.key().decode("utf-8") if msg.key() else None,
            },
        )

        # ── ROUTE AND PROCESS ─────────────────────────────────────────────────
        await _route_message(
            msg=msg,
            seat_service=seat_service,
        )

        # ── COMMIT OFFSET ─────────────────────────────────────────────────────
        # Commit after every message regardless of outcome.
        # DLQ-routed messages and TTL self-healing cases both return True
        # so the partition advances and is not blocked by one bad message.
        # asynchronous=False ensures the broker acknowledges the commit
        # before I poll the next message - correctness over speed.
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

    logger.info("Shutdown flag set — exiting Seat Service consumer loop cleanly")


# ──────────────────────────────────────────────────────────────────────────────
# PARTITION CALLBACKS
# ──────────────────────────────────────────────────────────────────────────────

def _on_partition_assign(consumer: Consumer, partitions: list) -> None:
    """
    Called when partitions are assigned during a rebalance.

    For the Seat Service, a rebalance means some seat lock operations
    may be paused temporarily — log clearly so engineers can correlate
    booking confirmation delays with rebalance events in Grafana.
    """
    logger.info(
        "Partitions assigned to Seat Service consumer",
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

    Any in-flight seat lock operation in progress when this fires will
    complete because the shutdown flag is only checked between messages.
    After revocation, another consumer instance will take the partitions
    and process subsequent messages.
    """
    logger.warning(
        "Partitions revoked from Seat Service — rebalance in progress. "
        "Seat lock operations may be delayed.",
        extra={
            "partitions": [
                {"topic": p.topic, "partition": p.partition}
                for p in partitions
            ]
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

async def run_consumer() -> None:
    """
    Bootstrap the Seat Service consumer:
      1. Start Prometheus metrics server on port 8003
      2. Create async Redis client and verify connectivity
      3. Instantiate SeatLockManager with the Redis client
      4. Instantiate SeatService with the SeatLockManager
      5. Build the Kafka consumer and subscribe to both topics
      6. Create the initial heartbeat sentinel file
      7. Run the poll loop until shutdown signal
      8. Release all held locks and close Redis pool on shutdown
    """

    # ── METRICS ───────────────────────────────────────────────────────────────
    start_http_server(port=8003)
    logger.info("Prometheus metrics server started on port 8003")

    # ── REDIS ─────────────────────────────────────────────────────────────────
    # Create the async Redis client. create_redis_client() also verifies
    # connectivity by sending a PING command — raises ConnectionError if
    # Redis is not reachable (entrypoint.sh should have caught this first
    # but the Python-level check provides defence in depth).
    redis_client = await create_redis_client(
        url=settings.REDIS_URL,
        password=settings.REDIS_PASSWORD,
    )
    REDIS_HEALTH.set(1)
    logger.info("Redis client connected and verified")

    # ── LOCK MANAGER ──────────────────────────────────────────────────────────
    # SeatLockManager wraps the Redis client with seat-specific lock logic.
    # Injecting it into SeatService (rather than having SeatService import
    # the Redis client directly) keeps SeatService testable with fakeredis.
    lock_manager = SeatLockManager(
        redis_client=redis_client,
        lock_ttl=settings.SEAT_LOCK_TTL_SECONDS,
    )

    # ── SEAT SERVICE ──────────────────────────────────────────────────────────
    seat_service = SeatService(lock_manager=lock_manager)
    logger.info(
        "SeatService initialised",
        extra={"lock_ttl_seconds": settings.SEAT_LOCK_TTL_SECONDS},
    )

    # ── KAFKA CONSUMER ────────────────────────────────────────────────────────
    consumer = _build_consumer()

    # Subscribe to BOTH topics in a single subscribe() call.
    # confluent-kafka handles multi-topic subscriptions natively —
    # the broker assigns partitions from both topics to this consumer
    # according to the consumer group rebalance protocol.
    consumer.subscribe(
        [
            settings.TOPIC_BOOKING_REQUESTED,
            settings.TOPIC_PAYMENT_FAILED,
        ],
        on_assign=_on_partition_assign,
        on_revoke=_on_partition_revoke,
    )

    logger.info(
        "Seat Service subscribed to Kafka topics",
        extra={
            "topics": [
                settings.TOPIC_BOOKING_REQUESTED,
                settings.TOPIC_PAYMENT_FAILED,
            ],
            "consumer_group": settings.KAFKA_CONSUMER_GROUP,
            "bootstrap":      settings.KAFKA_BOOTSTRAP_SERVERS,
        },
    )

    # Create initial heartbeat file
    _update_heartbeat()

    try:
        await _run_consumer_loop(
            consumer=consumer,
            seat_service=seat_service,
        )

    finally:
        # ── CLEAN SHUTDOWN ────────────────────────────────────────────────────

        # Close the Kafka consumer — commits pending offsets and notifies
        # the broker this consumer is leaving, triggering an immediate
        # rebalance so another instance takes over the partitions.
        consumer.close()
        logger.info("Kafka consumer closed")

        # Flush and close the DLQ producer if it was used
        if _dlq_producer:
            _dlq_producer.flush(timeout=10)
            logger.info("DLQ producer flushed")

        # Close the Redis connection pool.
        # This also releases any locks that were acquired but not yet
        # published downstream — in practice this should not happen
        # because the SIGTERM handler waits for the current message to
        # complete before setting _shutdown_requested, but closing the
        # pool cleanly is a safety net.
        await close_redis_client(redis_client)
        logger.info("Redis client closed cleanly")

        logger.info("Seat Service shut down cleanly")


# ──────────────────────────────────────────────────────────────────────────────
# MODULE ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("Using uvloop event loop for Seat Service")
    except ImportError:
        logger.warning("uvloop not available — using default asyncio event loop")

    try:
        asyncio.run(run_consumer())
    except Exception as exc:
        logger.critical(
            "Seat Service crashed with unhandled exception",
            extra={"error": str(exc)},
            exc_info=True,
        )
        sys.exit(1)