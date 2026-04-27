# api-gateway/app/producer.py
#
# Kafka producer wrapper for the API Gateway service.
#
# Responsibilities:
#   - Manage the lifecycle of the confluent-kafka Producer instance
#   - Connect to the Kafka broker with automatic retry on startup
#   - Publish serialized event messages to Kafka topics
#   - Handle delivery confirmations and surface errors to the caller
#   - Expose an is_connected flag used by the /health endpoint
#
# Why a wrapper class instead of using confluent-kafka directly?
#   confluent-kafka's Producer is synchronous by design — its produce()
#   method queues messages locally and poll()/flush() actually sends them.
#   This wrapper bridges that synchronous interface into async/await so
#   it fits naturally into FastAPI's async route handlers without blocking
#   the event loop.

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger("api-gateway.producer")


# ──────────────────────────────────────────────────────────────────────────────
# DELIVERY CALLBACK
# Called by librdkafka (the underlying C library) once it knows whether a
# message was successfully written to a Kafka partition or permanently failed.
# This runs in the background poll thread, not in the async event loop.
# ──────────────────────────────────────────────────────────────────────────────

def _delivery_callback(err, msg):
    """
    Invoked by the Kafka producer for every message after delivery is
    attempted. Logs success or failure — does not raise, because this
    callback runs outside of the async request context and exceptions
    here would be silently swallowed by librdkafka.

    For production systems you would increment a Prometheus counter here
    and optionally write failed messages to the DLQ topic.
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
            "Kafka message delivered",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
            },
        )


# ──────────────────────────────────────────────────────────────────────────────
# KAFKA PRODUCER
# ──────────────────────────────────────────────────────────────────────────────

class KafkaProducer:
    """
    Async-friendly wrapper around confluent-kafka's synchronous Producer.

    Usage (managed automatically by the FastAPI lifespan in main.py):

        producer = KafkaProducer(bootstrap_servers="kafka:29092")
        await producer.connect()

        await producer.publish(
            topic="booking.requested",
            key="booking-uuid",
            value='{"event_id": "..."}',
        )

        await producer.flush()   # call on shutdown
    """

    def __init__(self, bootstrap_servers: str):
        # The raw confluent-kafka Producer instance.
        # Set to None until connect() succeeds.
        self._producer: Optional[Producer] = None

        # Kafka broker address(es). Multiple brokers can be comma-separated:
        # "kafka1:9092,kafka2:9092" for a production multi-broker cluster.
        self._bootstrap_servers = bootstrap_servers

        # Tracks whether the producer has successfully connected.
        # Exposed as a property and read by the /health endpoint.
        self._connected: bool = False

        # confluent-kafka's Producer is thread-safe but not async-native.
        # I run poll() in a background thread via asyncio's thread pool
        # executor. This lock prevents concurrent flush() and publish()
        # calls from interfering with each other.
        self._lock = asyncio.Lock()


    # ── PROPERTIES ────────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        """True once connect() has succeeded. Read by the /health endpoint."""
        return self._connected


    # ── CONNECT ───────────────────────────────────────────────────────────────

    @retry(
        # Retry up to 10 times before giving up and crashing the container.
        # Docker's restart policy will then restart the container, which is
        # the correct behaviour - if Kafka never becomes available, I should
        # not silently stay up in a broken state.
        stop=stop_after_attempt(10),

        # Exponential backoff: 1s → 2s → 4s → 8s → 16s → 30s (capped).
        # This prevents thundering-herd when all services start simultaneously
        # and Kafka needs ~15s to become healthy.
        wait=wait_exponential(multiplier=1, min=1, max=30),

        # Only retry on KafkaException — let other exceptions (e.g.
        # misconfiguration) surface immediately without burning retries.
        retry=retry_if_exception_type(KafkaException),

        # Log each retry attempt so it is visible in `docker compose logs`.
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def connect(self) -> None:
        """
        Initialise the confluent-kafka Producer and verify broker reachability.

        Called once during FastAPI startup (lifespan). Retries automatically
        with exponential backoff via tenacity if Kafka is not yet ready.
        """
        logger.info(
            "Connecting to Kafka",
            extra={"bootstrap_servers": self._bootstrap_servers},
        )

        # Producer configuration.
        # See the full list of options at:
        # https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
        producer_config = {
            # Broker address
            "bootstrap.servers": self._bootstrap_servers,

            # How long to wait for broker metadata before raising an error.
            # Kept low so startup failures are detected quickly.
            "socket.timeout.ms": 10_000,

            # Maximum time to wait for a connection to be established.
            "socket.connection.setup.timeout.ms": 10_000,

            # Controls message durability:
            #   0 = fire-and-forget (fastest, messages can be lost)
            #   1 = leader acknowledges (good balance)
            #  -1 = all in-sync replicas acknowledge (slowest, most durable)
            # "all" is equivalent to -1 and is the safest choice.
            "acks": "all",

            # Number of times librdkafka retries a failed produce request
            # before calling the delivery callback with an error.
            "retries": 5,

            # Time to wait before retrying a failed produce request (ms).
            "retry.backoff.ms": 500,

            # Maximum time to wait for messages to be batched before sending.
            # Higher values improve throughput; lower values reduce latency.
            # 5ms is a reasonable default for a booking system.
            "linger.ms": 5,

            # Compress messages before sending to reduce network bandwidth.
            # snappy is fast and widely supported across Kafka versions.
            "compression.type": "snappy",

            # Maximum number of in-flight requests per connection.
            # Setting this to 1 guarantees ordering within a partition,
            # which is critical when multiple events belong to the same booking.
            "max.in.flight.requests.per.connection": 1,

            # Enable idempotent producer to prevent duplicate messages
            # caused by retries. Requires acks="all".
            "enable.idempotence": True,
        }

        # Verify broker reachability using AdminClient before creating the
        # Producer. AdminClient.list_topics() raises KafkaException if the
        # broker is unreachable, which tenacity will catch and retry.
        admin = AdminClient({"bootstrap.servers": self._bootstrap_servers})
        admin.list_topics(timeout=5)

        # Create the Producer instance. This does not open a connection —
        # the actual TCP connection is established on the first produce() call.
        loop = asyncio.get_event_loop()
        self._producer = await loop.run_in_executor(
            None,
            lambda: Producer(producer_config),
        )

        self._connected = True
        logger.info("Kafka producer connected successfully")


    # ── PUBLISH ───────────────────────────────────────────────────────────────

    async def publish(
        self,
        topic: str,
        value: str,
        key: Optional[str] = None,
        headers: Optional[dict] = None,
    ) -> None:
        """
        Publish a single message to a Kafka topic.

        Args:
            topic:   The Kafka topic to publish to (e.g. "booking.requested").
            value:   The message payload as a JSON string (from serialize_event).
            key:     Optional message key. Use the booking_id so all events
                     for the same booking land on the same partition (ordering).
            headers: Optional dict of string key-value metadata headers.
                     Useful for passing correlation_id without embedding it
                     in the payload, e.g. for tracing integrations.

        Raises:
            RuntimeError:   If called before connect() has succeeded.
            KafkaException: If the message could not be queued (e.g. local
                            queue is full). Delivery failures after queuing
                            are reported via the delivery callback, not here.
        """
        if not self._producer:
            raise RuntimeError(
                "KafkaProducer.connect() must be called before publish(). "
                "Check that the FastAPI lifespan ran successfully."
            )

        # Encode strings to bytes. Kafka messages are raw bytes on the wire;
        # UTF-8 is the universal convention for JSON payloads.
        key_bytes   = key.encode("utf-8")   if key   else None
        value_bytes = value.encode("utf-8") if value else None

        # Convert headers dict to the list-of-tuples format confluent-kafka expects:
        # [("correlation_id", b"abc-123"), ("service", b"api-gateway")]
        kafka_headers = (
            [(k, v.encode("utf-8")) for k, v in headers.items()]
            if headers else []
        )

        async with self._lock:
            loop = asyncio.get_event_loop()

            # produce() is non-blocking — it queues the message in librdkafka's
            # internal buffer and returns immediately. The actual network send
            # happens when poll() or flush() is called.
            await loop.run_in_executor(
                None,
                lambda: self._producer.produce(
                    topic=topic,
                    key=key_bytes,
                    value=value_bytes,
                    headers=kafka_headers,
                    on_delivery=_delivery_callback,
                ),
            )

            # poll(0) processes any pending delivery callbacks without blocking.
            # Calling it after every produce() keeps the callback queue drained
            # so memory does not accumulate during high-throughput periods.
            await loop.run_in_executor(
                None,
                lambda: self._producer.poll(0),
            )

        logger.info(
            "Message queued for delivery",
            extra={
                "topic": topic,
                "key":   key,
            },
        )


    # ── FLUSH ─────────────────────────────────────────────────────────────────

    async def flush(self, timeout: float = 30.0) -> None:
        """
        Block until all buffered messages have been delivered or the timeout
        expires. Called during FastAPI shutdown (lifespan) to ensure no
        in-flight messages are dropped when the container stops.

        Args:
            timeout: Maximum seconds to wait for delivery. If messages are
                     still undelivered after this time, they are discarded.
                     30 seconds is generous for local dev; tune for production.
        """
        if not self._producer:
            return

        logger.info("Flushing Kafka producer buffer before shutdown")

        loop = asyncio.get_event_loop()
        remaining = await loop.run_in_executor(
            None,
            lambda: self._producer.flush(timeout),
        )

        if remaining > 0:
            logger.warning(
                "Kafka flush timed out with undelivered messages",
                extra={"undelivered_message_count": remaining},
            )
        else:
            logger.info("Kafka producer flushed successfully — all messages delivered")

        self._connected = False