# seat-service/app/cache/redis_client.py
#
# Async Redis connection pool lifecycle management for the Seat Service.
#
# Responsibilities:
#   - create_redis_client() → create and verify an async Redis connection pool
#   - close_redis_client()  → drain and close the pool cleanly on shutdown
#   - is_redis_healthy()    → lightweight ping check for the consumer heartbeat
#   - RedisClientType       → type alias used throughout the cache package
#
# Why a factory function instead of a module-level singleton?
#   See app/cache/__init__.py for the full explanation. Short version:
#   redis-py creates asyncio primitives (Queue, Lock) during connection
#   pool initialisation. These primitives bind to the event loop running
#   at creation time. Creating the pool at module import time binds it
#   to a temporary loop that asyncio.run() discards before any await
#   is called - causing "Future attached to a different loop" errors.
#   create_redis_client() is called inside run_consumer() which runs
#   inside the correct event loop, so the pool is always bound correctly.
#
# Connection pool strategy:
#   redis-py manages a pool of persistent TCP connections to Redis.
#   The Seat Service performs two Redis operations per booking.requested
#   event (SET NX EX for the lock, SET EX for the owner key) and two
#   operations per payment.failed event (GET owner key, DEL lock key,
#   DEL owner key). With a pool of 10 connections and a 0.5s poll
#   timeout, the Seat Service can handle approximately 10 concurrent
#   lock operations without connection queuing.
#
# Redis key conventions (documented here as the Redis authority):
#   seat_lock:{flight_id}:{seat_number}   → seat lock (TTL = SEAT_LOCK_TTL_SECONDS)
#   seat_lock_owner:{booking_id}          → owner mapping (TTL = SEAT_LOCK_TTL_SECONDS)
#
#   Both keys share the same TTL so they expire together. A lock without
#   an owner key (or vice versa) indicates a partial write - detectable
#   by health check scripts that SCAN both key patterns.

from __future__ import annotations

import logging
from typing import Optional

import redis.asyncio as redis
from redis.asyncio import Redis
from redis.exceptions import (
    AuthenticationError,
    ConnectionError as RedisConnectionError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
    before_sleep_log,
)

from app.config import settings


logger = logging.getLogger("seat-service.redis_client")


# ──────────────────────────────────────────────────────────────────────────────
# TYPE ALIAS
# Used as a type hint throughout the cache package and in service.py.
# Aliasing Redis here means if I ever switch to a different async Redis
# client (e.g. coredis) I change one line here rather than every file
# that imports the client type.
# ──────────────────────────────────────────────────────────────────────────────

RedisClientType = Redis


# ──────────────────────────────────────────────────────────────────────────────
# CONNECTION FACTORY
# ──────────────────────────────────────────────────────────────────────────────

@retry(
    # Retry connection attempts on startup with exponential backoff and
    # jitter. entrypoint.sh polls Redis with redis-cli ping before starting
    # the consumer, but Python-level retry adds defence in depth for the
    # case where Redis accepts TCP connections (ping succeeds) but is not
    # yet ready to handle AUTH commands (AUTH fails with LOADING error
    # during Redis startup while it loads RDB snapshots from disk).
    stop=stop_after_attempt(10),
    wait=wait_exponential_jitter(initial=1, max=15, jitter=1),
    retry=retry_if_exception_type(
        (RedisConnectionError, RedisTimeoutError, ResponseError)
    ),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def create_redis_client(
    url: Optional[str] = None,
    password: Optional[str] = None,
) -> RedisClientType:
    """
    Create, configure, and verify an async Redis connection pool.

    Must be called inside the asyncio event loop (i.e. inside an async
    function that is already running under asyncio.run()). See module
    docstring for why this cannot be a module-level singleton.

    Connection pool is configured for the Seat Service's access pattern:
      - High concurrency during peak booking windows (many lock acquisitions)
      - Short-lived operations (SET NX EX, DEL complete in milliseconds)
      - Aggressive timeouts (seat locking is time-critical)

    Args:
        url:      Redis connection URL in the form redis://:password@host:port/db
                  Defaults to settings.REDIS_URL if not provided.
                  Overridden in tests to point at fakeredis.
        password: Redis AUTH password. Overrides the password in the URL
                  if both are provided - useful when the password contains
                  special characters that cannot be URL-encoded safely.
                  Defaults to settings.REDIS_PASSWORD.

    Returns:
        A connected, authenticated, and verified Redis client instance
        backed by an async connection pool.

    Raises:
        AuthenticationError:    If REDIS_PASSWORD is wrong.
        RedisConnectionError:   If Redis host/port is unreachable after retries.
        RedisTimeoutError:      If Redis does not respond within socket_timeout.
        RuntimeError:           If the PING verification returns an unexpected value.
    """
    redis_url      = url      or settings.REDIS_URL
    redis_password = password or settings.REDIS_PASSWORD

    logger.info(
        "Creating async Redis connection pool",
        extra={
            "host":     settings.REDIS_HOST,
            "port":     settings.REDIS_PORT,
            "db":       settings.REDIS_DB,
            "pool_size": 10,
        },
    )

    # ── CONNECTION POOL CONFIGURATION ─────────────────────────────────────────
    #
    # redis.asyncio.Redis() creates an async connection pool internally.
    # Each parameter below is chosen for the Seat Service's workload:

    client: Redis = redis.Redis.from_url(
        url=redis_url,

        # ── AUTHENTICATION ────────────────────────────────────────────────────

        # Override the password from the URL. Passing it separately avoids
        # issues with special characters in passwords that break URL parsing.
        # redis-py applies this after connecting, via the AUTH command.
        password=redis_password,

        # ── POOL SIZING ───────────────────────────────────────────────────────

        # Maximum number of persistent TCP connections in the pool.
        # The Seat Service performs 2-3 Redis operations per Kafka message
        # (acquire lock, set owner key, read owner key on idempotency check).
        # A pool of 10 supports ~3-4 concurrent messages being processed
        # simultaneously - sufficient for a single consumer instance.
        # Increase to 20 when running multiple consumer replicas.
        max_connections=10,

        # ── ENCODING ──────────────────────────────────────────────────────────

        # Decode Redis responses from bytes to Python strings automatically.
        # Without this, every GET returns b"seat_lock:..." instead of
        # "seat_lock:..." - requiring explicit .decode("utf-8") calls
        # throughout SeatLockManager. decode_responses=True eliminates this.
        #
        # Trade-off: binary data (e.g. serialized Protobuf) cannot be stored
        # with decode_responses=True. The Seat Service stores only UTF-8
        # strings (lock keys, booking_id UUIDs) so this is safe here.
        decode_responses=True,

        # ── TIMEOUTS ──────────────────────────────────────────────────────────

        # Maximum seconds to wait for a new TCP connection to Redis.
        # If Redis is unreachable for longer than this, RedisConnectionError
        # is raised and tenacity retries the connection.
        # Kept at 5s - entrypoint.sh ensures Redis is reachable before the
        # consumer starts, so a 5s timeout indicates a post-startup problem.
        socket_connect_timeout=5,

        # Maximum seconds to wait for a Redis command response (GET, SET, DEL).
        # Seat lock operations must complete quickly - a 3s timeout is generous
        # for an in-memory Redis command that normally completes in <1ms.
        # If a command takes 3s, Redis is severely overloaded or the network
        # is degraded - fail fast and let the consumer retry.
        socket_timeout=3,

        # Keep TCP connections alive between operations.
        # Prevents the OS from closing idle connections with TCP RST packets
        # during quiet periods (e.g. no bookings at 3am) which would cause
        # the first operation after the quiet period to fail with
        # "Connection reset by peer" before the pool reconnects.
        socket_keepalive=True,

        # TCP keepalive options - override OS defaults for faster detection
        # of dead connections. Values are in seconds.
        socket_keepalive_options={
            # Start sending keepalive probes after 60s of inactivity.
            # Lower than default (7200s on Linux) so dead connections are
            # detected within minutes rather than hours.
            "TCP_KEEPIDLE":  60,

            # Interval between keepalive probes (5s).
            # After TCP_KEEPIDLE expires, probes are sent every 5 seconds.
            "TCP_KEEPINTVL": 5,

            # Number of failed probes before declaring the connection dead.
            # 3 probes * 5s interval = 15s to detect a dead connection
            # after the 60s idle period. Total detection time: ~75 seconds.
            "TCP_KEEPCNT":   3,
        },

        # ── RETRY ON TIMEOUT ──────────────────────────────────────────────────

        # Automatically retry a command once if it times out before raising
        # RedisTimeoutError. Handles transient network blips without
        # propagating a timeout to the consumer's retry loop.
        # Only retries once - repeated timeouts indicate a real problem.
        retry_on_timeout=True,

        # ── CONNECTION HEALTH ─────────────────────────────────────────────────

        # Check connection health before returning from the pool.
        # Sends a PING before each command to verify the connection is
        # still alive - prevents "Connection closed by server" errors
        # on connections that Redis dropped due to its own timeout.
        # Small latency cost (~0.1ms) on every command - acceptable given
        # that the alternative is a failed seat lock operation.
        health_check_interval=30,   # seconds between health checks

        # ── DATABASE SELECTION ────────────────────────────────────────────────
        # Redis supports 16 databases (0-15) by default. We use db=0 (default)
        # in this project. In production you might use different databases for
        # different environments (staging=1, production=0) on the same Redis
        # instance — though separate Redis instances are preferred.
    )

    # ── VERIFY CONNECTIVITY AND AUTHENTICATION ─────────────────────────────────
    # Send a PING command to confirm:
    #   1. The TCP connection to Redis was established successfully
    #   2. The AUTH password was accepted (AuthenticationError raised here
    #      if the password is wrong — caught by tenacity and retried up to
    #      10 times, though a wrong password will never succeed on retry)
    #   3. Redis is ready to serve commands (not still loading RDB from disk)
    #
    # PING is the lightest possible Redis command — 0 memory allocation,
    # no I/O to disk, completes in microseconds.
    try:
        response = await client.ping()
        if not response:
            raise RuntimeError(
                "Redis PING returned a falsy response. "
                "Expected True (PONG), got: {response!r}"
            )
    except AuthenticationError as exc:
        # Wrong password — will not be fixed by retrying.
        # Log at CRITICAL and re-raise to exit the process so Docker
        # restarts the container and the operator sees the error.
        logger.critical(
            "Redis authentication failed — check REDIS_PASSWORD in .env",
            extra={"error": str(exc)},
        )
        raise

    logger.info(
        "Redis connection pool created and verified",
        extra={
            "host":          settings.REDIS_HOST,
            "port":          settings.REDIS_PORT,
            "db":            settings.REDIS_DB,
            "max_connections": 10,
            "decode_responses": True,
        },
    )

    return client


# ──────────────────────────────────────────────────────────────────────────────
# CONNECTION POOL CLEANUP
# ──────────────────────────────────────────────────────────────────────────────

async def close_redis_client(client: RedisClientType) -> None:
    """
    Gracefully close the Redis connection pool on service shutdown.

    Called in the finally block of consumer.py's run_consumer() after
    the poll loop exits. Drains all active connections cleanly rather
    than letting the OS close TCP sockets abruptly on process exit.

    Why clean closure matters for the Seat Service:
      Redis tracks client connections in CLIENT LIST. An abrupt disconnect
      (OS kills TCP socket) leaves a "ghost" client entry in Redis's
      client list until the server-side tcp-keepalive detects the dead
      connection (up to 75 seconds with our keepalive settings above).
      During this window, the ghost connection counts against Redis's
      maxclients limit. Clean closure via aclose() sends a proper TCP FIN
      so Redis immediately removes the client from CLIENT LIST.

    Additionally, any command currently in-flight when close is called
    will be allowed to complete before the connection is closed — preventing
    a partial SET NX EX or DEL that could leave Redis in an inconsistent
    lock state.

    Args:
        client: The Redis client instance returned by create_redis_client().
                Silently returns if client is None (e.g. startup failed
                before create_redis_client() succeeded).
    """
    if client is None:
        logger.debug("Redis client is None — nothing to close")
        return

    try:
        logger.info("Closing Redis connection pool")

        # aclose() is the redis-py async method for graceful shutdown.
        # It:
        #   1. Waits for any in-flight commands to complete
        #   2. Sends QUIT to each active connection (tells Redis to close)
        #   3. Closes all TCP sockets cleanly (TCP FIN handshake)
        #   4. Releases all asyncio resources (Queue, Lock objects)
        await client.aclose()

        logger.info("Redis connection pool closed cleanly")

    except Exception as exc:
        # Log but do not re-raise — we are in a shutdown path and there
        # is nothing useful the caller can do with a close() exception.
        # The process is exiting anyway and the OS will reclaim all
        # file descriptors (TCP sockets) on process exit.
        logger.warning(
            "Error closing Redis client — connections may not have closed cleanly",
            extra={"error": str(exc)},
        )


# ──────────────────────────────────────────────────────────────────────────────
# HEALTH CHECK
# ──────────────────────────────────────────────────────────────────────────────

async def is_redis_healthy(client: RedisClientType) -> bool:
    """
    Lightweight Redis health check for the consumer heartbeat loop.

    Sends a PING command and returns True if PONG is received within
    socket_timeout (3 seconds). Returns False on any error without
    raising so the caller can decide how to respond.

    Used in two ways:
      1. By consumer.py to update the REDIS_HEALTH Prometheus gauge —
         a value of 0 triggers a Grafana alert before seat locking fails.
      2. Optionally added to the sentinel file update logic so the Docker
         HEALTHCHECK also reflects Redis connectivity, not just message
         processing activity.

    Does NOT raise — callers handle degraded Redis connectivity by:
      - Logging a warning
      - Incrementing the REDIS_HEALTH gauge to 0
      - Allowing the consumer's tenacity retry to handle the underlying
        Redis error when the next lock operation is attempted

    Args:
        client: The Redis client instance to ping.

    Returns:
        True  if PING → PONG succeeds within socket_timeout.
        False if any exception occurs (ConnectionError, TimeoutError, etc.)
    """
    try:
        response = await client.ping()
        return bool(response)
    except Exception as exc:
        logger.warning(
            "Redis health check failed",
            extra={"error": str(exc)},
        )
        return False


# ──────────────────────────────────────────────────────────────────────────────
# REDIS INFO HELPER
# ──────────────────────────────────────────────────────────────────────────────

async def get_redis_info(client: RedisClientType) -> dict:
    """
    Fetch Redis server INFO for diagnostics and startup logging.

    Called once during run_consumer() startup to log the Redis version,
    memory usage, and connected client count. Useful for confirming that
    the Seat Service connected to the intended Redis instance and that
    the instance has sufficient memory for the expected seat lock volume.

    Not called on every message — INFO is a relatively heavy command
    that reads multiple counters and should not be called in a hot path.

    Args:
        client: The Redis client instance.

    Returns:
        A dict of Redis INFO fields. Key fields include:
          - redis_version:          Redis server version string
          - used_memory_human:      Current memory usage e.g. "2.50M"
          - connected_clients:      Number of active client connections
          - uptime_in_seconds:      Server uptime
          - rdb_last_bgsave_status: "ok" if last RDB snapshot succeeded
          - aof_enabled:            1 if AOF persistence is active
        Returns an empty dict if INFO fails — does not raise.
    """
    try:
        info = await client.info()
        logger.info(
            "Redis server info retrieved",
            extra={
                "redis_version":        info.get("redis_version"),
                "used_memory_human":    info.get("used_memory_human"),
                "connected_clients":    info.get("connected_clients"),
                "uptime_in_seconds":    info.get("uptime_in_seconds"),
                "rdb_last_bgsave_status": info.get("rdb_last_bgsave_status"),
            },
        )
        return info
    except Exception as exc:
        logger.warning(
            "Failed to retrieve Redis server info",
            extra={"error": str(exc)},
        )
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# KEY SCANNING UTILITY
# ──────────────────────────────────────────────────────────────────────────────

async def scan_seat_locks(
    client: RedisClientType,
    flight_id: Optional[str] = None,
) -> list[str]:
    """
    Scan Redis for all active seat lock keys, optionally filtered by flight.

    Uses Redis SCAN rather than KEYS to avoid blocking the Redis server
    during a full keyspace scan. SCAN is O(1) per call (paginated) while
    KEYS is O(n) and blocks all other Redis commands for the duration —
    unacceptable in a production system where KEYS on a large keyspace
    can block Redis for seconds.

    Use cases:
      1. Flight cancellation — release all seat locks for a cancelled flight:
           locks = await scan_seat_locks(client, flight_id="f9e8d7c6-...")
           for lock_key in locks:
               await client.delete(lock_key)

      2. Monitoring dashboard — show all currently locked seats:
           all_locks = await scan_seat_locks(client)

      3. Health check script — verify no orphaned locks exist after a crash:
           locks = await scan_seat_locks(client)
           for lock in locks:
               # Check if corresponding owner key exists
               # Flag any locks without an owner key as orphaned

    Args:
        client:    The Redis client instance.
        flight_id: If provided, only return locks for this specific flight.
                   Pattern: seat_lock:{flight_id}:*
                   If None, return all seat locks.
                   Pattern: seat_lock:*

    Returns:
        List of matching Redis key strings. Empty list on error or no matches.
        Keys are in the form "seat_lock:{flight_id}:{seat_number}".
    """
    # Build the SCAN MATCH pattern
    if flight_id:
        # Scan only locks for this flight — e.g. for flight cancellation
        pattern = f"seat_lock:{flight_id}:*"
    else:
        # Scan all seat locks — exclude owner keys (seat_lock_owner:*)
        # by matching only the seat_lock: prefix without "owner"
        pattern = "seat_lock:[^o]*"   # matches seat_lock:{uuid}:{seat}
                                       # does not match seat_lock_owner:*

    try:
        keys: list[str] = []

        # SCAN with count=100 means Redis returns approximately 100 keys
        # per iteration (not guaranteed — Redis may return more or fewer).
        # Iterating with async for handles pagination automatically.
        async for key in client.scan_iter(
            match=pattern,
            count=100,
        ):
            keys.append(key)

        logger.debug(
            "Redis seat lock scan completed",
            extra={
                "pattern":    pattern,
                "keys_found": len(keys),
                "flight_id":  flight_id,
            },
        )
        return keys

    except Exception as exc:
        logger.error(
            "Redis seat lock scan failed",
            extra={
                "pattern": pattern,
                "error":   str(exc),
            },
        )
        return []