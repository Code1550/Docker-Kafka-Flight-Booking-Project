# booking-service/app/consumer.py
#
# Main entry point for the Booking Service.
#
# This module runs a long-lived asyncio event loop that:
#   1. Subscribes to the `booking.requested` Kafka topic
#   2. Deserializes each message into a BookingRequestedEvent
#   3. Delegates processing to BookingService (service.py)
#   4. Commits the Kafka offset only after successful processing
#   5. Routes unprocessable messages to the dead-letter queue (DLQ)
#   6. Updates a sentinel file on every message for the Docker health check
#
# Concurrency model:
#   confluent-kafka's Consumer is synchronous. We run its poll() call in
#   asyncio's thread pool executor so the event loop stays free to handle
#   other coroutines (DB writes, Kafka produces) concurrently. This is the
#   same pattern used in the API Gateway's producer.py.
#
# Offset management:
#   enable.auto.commit is set to False. Offsets are committed manually
#   AFTER a message is fully processed and written to PostgreSQL. This
#   guarantees at-least-once delivery — if the container crashes mid-
#   processing, the message is re-delivered on restart rather than silently
#   lost. Your service.py logic must be idempotent to handle re-delivery.

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from prometheus_client import Counter, Histogram, start_http_server
from pythonjsonlogger import jsonlogger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from app.config import settings
from app.service import BookingService
from app.db.connection import get_db_session
from shared.schemas import (
    BookingRequestedEvent,
    BookingFailedDLQEvent,
    deserialize_event,
    serialize_event,
)


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# Structured JSON so every log line is parseable by Grafana Loki or any
# log aggregator. Always include booking_id and correlation_id in extras
# so a single booking journey can be traced across all services.
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("booking-service.consumer")
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
# Scraped by Prometheus every 15 seconds. Visualised in the Grafana dashboard.
# The metrics HTTP server is started on port 8001 inside run_consumer() below.
# ──────────────────────────────────────────────────────────────────────────────

# Total messages consumed, labelled by outcome so you can alert on error rate
MESSAGES_CONSUMED = Counter(
    "booking_service_messages_consumed_total",
    "Total Kafka messages consumed by the Booking Service",
    ["status"],   # labels: "success" | "dlq" | "deserialization_error"
)

# End-to-end processing time per message (DB write + Kafka produce)
PROCESSING_DURATION = Histogram(
    "booking_service_processing_duration_seconds",
    "Time taken to fully process one booking.requested message",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# Counts how many messages have been routed to the DLQ
DLQ_MESSAGES_TOTAL = Counter(
    "booking_service_dlq_messages_total",
    "Total messages sent to the dead-letter queue",
)

# Tracks the current consumer group lag (approximated by unprocessed messages)
CONSUMER_LAG = Counter(
    "booking_service_consumer_lag_total",
    "Cumulative count of messages where processing was retried",
)


# ──────────────────────────────────────────────────────────────────────────────
# SENTINEL FILE
# The Docker HEALTHCHECK in the Dockerfile watches this file's modification
# time. The consumer touches it on every successfully processed message.
# If the file goes stale (not modified in > 60s), Docker restarts the container.
# ──────────────────────────────────────────────────────────────────────────────

HEARTBEAT_FILE = Path("/tmp/consumer_heartbeat")


def _update_heartbeat() -> None:
    """Touch the sentinel file to signal the consumer is alive and processing."""
    HEARTBEAT_FILE.touch(exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# GRACEFUL SHUTDOWN
# Handles SIGTERM (sent by Docker on `docker compose stop`) and SIGINT
# (Ctrl+C during local development). Sets a flag that the consumer loop
# checks on every iteration so it can finish the current message before exiting.
# ──────────────────────────────────────────────────────────────────────────────

# Checked on every poll iteration — set to True by signal handlers
_shutdown_requested = False


def _handle_shutdown(signum, frame) -> None:
    """Signal handler for SIGTERM and SIGINT. Sets the shutdown flag."""
    global _shutdown_requested
    logger.info(
        "Shutdown signal received",
        extra={"signal": signal.Signals(signum).name},
    )
    _shutdown_requested = True


# Register signal handlers before the consumer loop starts
signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT,  _handle_shutdown)


# ──────────────────────────────────────────────────────────────────────────────
# KAFKA CONSUMER FACTORY
# ──────────────────────────────────────────────────────────────────────────────

def _build_consumer() -> Consumer:
    """
    Construct and return a configured confluent-kafka Consumer instance.

    Kept as a standalone function (rather than inline in the loop) so the
    consumer can be re-created cleanly after a fatal error without restarting
    the entire process.
    """
    config = {
        # Broker address — matches KAFKA_BOOTSTRAP_SERVERS in docker-compose
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,

        # Consumer group ID — all instances of this service share the same
        # group so Kafka distributes partitions between them automatically.
        # Each partition is consumed by exactly one member of the group.
        "group.id": settings.KAFKA_CONSUMER_GROUP,

        # Where to start reading when no committed offset exists for this
        # group (e.g. first time the service starts, or after a group reset).
        # "earliest" reads from the beginning of the topic so no messages
        # are missed. Use "latest" if you only care about new messages.
        "auto.offset.reset": "earliest",

        # Disable automatic offset commits. We commit manually after each
        # message is fully processed so that a crash mid-processing causes
        # re-delivery rather than silent message loss.
        "enable.auto.commit": False,

        # Maximum time (ms) the broker waits before sending a fetch response
        # even if min.bytes isn't reached yet. Lower = lower latency.
        "fetch.wait.max.ms": 500,

        # Minimum bytes to fetch in a single request. Higher values improve
        # throughput at the cost of latency. 1 byte = respond immediately
        # when any data is available (low latency, suitable for bookings).
        "fetch.min.bytes": 1,

        # Maximum bytes per partition per fetch request. Keeps individual
        # messages from consuming excessive memory if payloads are large.
        "max.partition.fetch.bytes": 1_048_576,   # 1 MB

        # How often the consumer sends a heartbeat to the broker to signal
        # it is alive. Must be lower than session.timeout.ms.
        "heartbeat.interval.ms": 3_000,

        # If the broker does not receive a heartbeat within this window it
        # considers the consumer dead and triggers a rebalance.
        # Higher values tolerate slow processing; lower values detect
        # failures faster. 30s is a safe default for a DB-writing consumer.
        "session.timeout.ms": 30_000,

        # Maximum time between poll() calls before the broker considers the
        # consumer dead. Increase this if your processing logic is slow
        # (e.g. external API calls with long timeouts).
        "max.poll.interval.ms": 300_000,   # 5 minutes
    }

    consumer = Consumer(config)
    return consumer


# ──────────────────────────────────────────────────────────────────────────────
# DLQ PRODUCER
# A lightweight inline producer used only for routing failed messages to the
# dead-letter queue. Not the full KafkaProducer wrapper from the API Gateway —
# the Booking Service does not need the async abstractions since DLQ writes
# happen synchronously on error paths.
# ──────────────────────────────────────────────────────────────────────────────

from confluent_kafka import Producer as _KafkaProducer

_dlq_producer: _KafkaProducer | None = None


def _get_dlq_producer() -> _KafkaProducer:
    """Lazy singleton — creates the DLQ producer on first use."""
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = _KafkaProducer(
            {"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS}
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
    Publish an unprocessable message to the dead-letter queue topic.

    The original raw payload is preserved as a string so engineers can
    inspect or replay it without needing to reconstruct the original event.

    This function is synchronous — DLQ writes happen on error paths where
    we have already left the async context.
    """
    dlq_event = BookingFailedDLQEvent(
        correlation_id="unknown",   # may not be parseable if deserialization failed
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
    )
    producer.flush(timeout=10)

    DLQ_MESSAGES_TOTAL.inc()

    logger.error(
        "Message routed to DLQ",
        extra={
            "booking_id":    booking_id,
            "failed_topic":  failed_topic,
            "error_message": error_message,
            "retry_count":   retry_count,
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# MESSAGE PROCESSOR
# Handles a single Kafka message end-to-end. Called inside the consumer loop
# for every message returned by poll(). Separated from the loop itself so it
# can be unit-tested without a running Kafka broker.
# ──────────────────────────────────────────────────────────────────────────────

async def _process_message(
    msg: Message,
    booking_service: BookingService,
    retry_count: int = 0,
) -> bool:
    """
    Deserialize, validate, and process one Kafka message.

    Returns:
        True  — message was processed successfully, safe to commit offset
        False — message was unprocessable and has been routed to DLQ,
                offset should still be committed to avoid infinite retry

    The function never raises — all exceptions are caught, logged, and
    either retried (transient errors) or sent to the DLQ (permanent errors).
    """
    raw = msg.value().decode("utf-8") if msg.value() else ""
    topic = msg.topic()

    # ── STEP 1: Deserialize ───────────────────────────────────────────────────
    try:
        event: BookingRequestedEvent = deserialize_event(topic, raw)
    except Exception as exc:
        # Deserialization failure is a permanent error — retrying will not fix
        # a malformed payload. Send straight to DLQ without retrying.
        logger.error(
            "Failed to deserialize Kafka message — routing to DLQ",
            extra={
                "topic":  topic,
                "offset": msg.offset(),
                "error":  str(exc),
                "raw":    raw[:200],   # truncate to avoid flooding logs
            },
        )
        MESSAGES_CONSUMED.labels(status="deserialization_error").inc()
        _send_to_dlq(
            raw_payload=raw,
            error_message=f"Deserialization failed: {exc}",
            booking_id=None,
            retry_count=retry_count,
            failed_topic=topic,
        )
        return False

    booking_id     = event.booking_id
    correlation_id = event.correlation_id

    logger.info(
        "Processing booking.requested event",
        extra={
            "booking_id":     booking_id,
            "correlation_id": correlation_id,
            "passenger_id":   event.passenger.passenger_id,
            "flight_id":      event.flight.flight_id,
            "seat_number":    event.flight.seat_number,
            "retry_count":    retry_count,
        },
    )

    # ── STEP 2: Business logic with retry ─────────────────────────────────────
    # Transient errors (DB connection blip, brief Kafka hiccup) are retried
    # up to MAX_RETRIES times with exponential backoff before giving up.
    # Permanent errors (invalid data, constraint violation) go straight to DLQ.
    MAX_RETRIES = settings.PROCESSING_RETRY_ATTEMPTS   # default: 3

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with PROCESSING_DURATION.time():
                async with get_db_session() as session:
                    await booking_service.handle_booking_requested(
                        event=event,
                        session=session,
                    )

            # ── SUCCESS ───────────────────────────────────────────────────────
            MESSAGES_CONSUMED.labels(status="success").inc()
            _update_heartbeat()

            logger.info(
                "Booking processed successfully",
                extra={
                    "booking_id":     booking_id,
                    "correlation_id": correlation_id,
                    "attempt":        attempt,
                },
            )
            return True

        except Exception as exc:
            is_last_attempt = attempt == MAX_RETRIES

            logger.warning(
                "Error processing booking event",
                extra={
                    "booking_id":     booking_id,
                    "correlation_id": correlation_id,
                    "attempt":        attempt,
                    "max_retries":    MAX_RETRIES,
                    "error":          str(exc),
                    "will_retry":     not is_last_attempt,
                },
            )
            CONSUMER_LAG.inc()

            if is_last_attempt:
                # All retries exhausted — route to DLQ so the message is
                # not lost and can be inspected or replayed by an engineer.
                MESSAGES_CONSUMED.labels(status="dlq").inc()
                _send_to_dlq(
                    raw_payload=raw,
                    error_message=f"Processing failed after {MAX_RETRIES} attempts: {exc}",
                    booking_id=booking_id,
                    retry_count=attempt,
                    failed_topic=topic,
                )
                return False

            # Wait before retrying — exponential backoff capped at 16s
            backoff = min(2 ** attempt, 16)
            logger.info(
                f"Retrying in {backoff}s",
                extra={
                    "booking_id": booking_id,
                    "attempt":    attempt,
                },
            )
            await asyncio.sleep(backoff)

    # Should be unreachable — the loop always returns inside the for block
    return False


# ──────────────────────────────────────────────────────────────────────────────
# CONSUMER LOOP
# ──────────────────────────────────────────────────────────────────────────────

@retry(
    # Retry the entire consumer loop if it crashes due to a Kafka error
    # (e.g. broker restart, network blip). Give up after 10 attempts.
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type(KafkaException),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def _run_consumer_loop(
    consumer: Consumer,
    booking_service: BookingService,
) -> None:
    """
    Core poll loop. Runs until a shutdown signal is received or a fatal
    error exhausts all tenacity retries.

    Each iteration:
      1. poll() for a message (non-blocking, runs in thread executor)
      2. Skip control messages (partition EOF, no message)
      3. Process the message via _process_message()
      4. Commit the offset regardless of success/failure
         (failures go to DLQ so the offset is safe to advance)
    """
    loop = asyncio.get_event_loop()

    logger.info(
        "Consumer loop started",
        extra={"topic": settings.TOPIC_BOOKING_REQUESTED},
    )

    while not _shutdown_requested:
        # ── POLL ──────────────────────────────────────────────────────────────
        # Run poll() in the thread executor so the async event loop is not
        # blocked while waiting for the broker to return messages.
        # timeout=1.0 means poll returns after 1 second even if no message
        # is available — this keeps the shutdown check responsive.
        msg: Message | None = await loop.run_in_executor(
            None,
            lambda: consumer.poll(timeout=1.0),
        )

        # ── NO MESSAGE ────────────────────────────────────────────────────────
        if msg is None:
            # poll() returned without a message — topic is empty or we are
            # caught up. Loop back and poll again.
            continue

        # ── KAFKA ERRORS ──────────────────────────────────────────────────────
        if msg.error():
            error = msg.error()

            if error.code() == KafkaError._PARTITION_EOF:
                # Reached the end of a partition — not an error, just means
                # we have consumed all currently available messages.
                logger.debug(
                    "Reached end of partition",
                    extra={
                        "topic":     msg.topic(),
                        "partition": msg.partition(),
                        "offset":    msg.offset(),
                    },
                )
                continue

            # Any other Kafka error is raised so tenacity can catch and
            # retry it. Examples: broker unavailable, network timeout.
            raise KafkaException(error)

        # ── PROCESS MESSAGE ───────────────────────────────────────────────────
        logger.info(
            "Message received from Kafka",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
                "key":       msg.key().decode("utf-8") if msg.key() else None,
            },
        )

        await _process_message(
            msg=msg,
            booking_service=booking_service,
        )

        # ── COMMIT OFFSET ─────────────────────────────────────────────────────
        # Commit after processing regardless of success or DLQ routing.
        # This advances the consumer group offset so the message is not
        # re-delivered on the next poll. DLQ messages are preserved in
        # the DLQ topic for inspection — they are not lost by committing here.
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

    logger.info("Shutdown flag detected — exiting consumer loop cleanly")


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# Called by entrypoint.sh via `python3 -m app.consumer`
# ──────────────────────────────────────────────────────────────────────────────

async def run_consumer() -> None:
    """
    Bootstrap the Booking Service consumer:
      1. Start the Prometheus metrics HTTP server
      2. Build the Kafka consumer and subscribe to the topic
      3. Instantiate BookingService (which holds no state — just methods)
      4. Run the poll loop until shutdown
      5. Close the consumer cleanly on exit
    """

    # Start Prometheus metrics server on a separate port (8001) so it does
    # not conflict with any other service. Prometheus scrapes this endpoint
    # as configured in monitoring/prometheus.yml.
    start_http_server(port=8001)
    logger.info("Prometheus metrics server started on port 8001")

    # Build the consumer and subscribe to the booking.requested topic.
    # subscribe() accepts a list — you can add more topics later without
    # changing the poll loop.
    consumer = _build_consumer()
    consumer.subscribe(
        [settings.TOPIC_BOOKING_REQUESTED],
        on_assign=_on_partition_assign,
        on_revoke=_on_partition_revoke,
    )

    logger.info(
        "Subscribed to Kafka topic",
        extra={
            "topic":          settings.TOPIC_BOOKING_REQUESTED,
            "consumer_group": settings.KAFKA_CONSUMER_GROUP,
            "bootstrap":      settings.KAFKA_BOOTSTRAP_SERVERS,
        },
    )

    # BookingService is stateless — safe to instantiate once and reuse
    # across all messages. DB sessions are created per-message inside
    # _process_message() using the async context manager.
    booking_service = BookingService()

    # Create the initial heartbeat file so the Docker health check does not
    # immediately flag the container as unhealthy during the startup window.
    _update_heartbeat()

    try:
        await _run_consumer_loop(
            consumer=consumer,
            booking_service=booking_service,
        )
    finally:
        # Always close the consumer cleanly, even if the loop exits due to
        # an exception. close() commits any pending offsets and notifies the
        # broker that this consumer is leaving the group, triggering an
        # immediate rebalance so another instance can take over the partitions.
        consumer.close()
        logger.info("Kafka consumer closed cleanly")


# ──────────────────────────────────────────────────────────────────────────────
# PARTITION CALLBACKS
# Called by confluent-kafka when partitions are assigned or revoked during
# a consumer group rebalance (e.g. another instance joins or leaves).
# ──────────────────────────────────────────────────────────────────────────────

def _on_partition_assign(consumer: Consumer, partitions: list) -> None:
    """Logged when the broker assigns partitions to this consumer instance."""
    logger.info(
        "Partitions assigned",
        extra={
            "partitions": [
                {"topic": p.topic, "partition": p.partition}
                for p in partitions
            ]
        },
    )


def _on_partition_revoke(consumer: Consumer, partitions: list) -> None:
    """Logged when the broker revokes partitions (rebalance triggered)."""
    logger.info(
        "Partitions revoked — rebalance in progress",
        extra={
            "partitions": [
                {"topic": p.topic, "partition": p.partition}
                for p in partitions
            ]
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# MODULE ENTRY POINT
# Allows running via `python3 -m app.consumer` from entrypoint.sh.
# Sets up uvloop as the event loop for better async performance, then
# hands off to run_consumer().
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        import uvloop
        # Replace the default asyncio event loop with uvloop.
        # uvloop is 2-4x faster than the default loop for I/O-bound workloads
        # like a Kafka consumer that is constantly reading from the broker
        # and writing to PostgreSQL.
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("Using uvloop event loop")
    except ImportError:
        # uvloop is not available on Windows — fall back to the default loop
        logger.warning("uvloop not available — using default asyncio event loop")

    try:
        asyncio.run(run_consumer())
    except Exception as exc:
        logger.critical(
            "Consumer crashed with unhandled exception",
            extra={"error": str(exc)},
            exc_info=True,
        )
        sys.exit(1)