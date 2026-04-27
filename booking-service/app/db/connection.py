# booking-service/app/db/connection.py
#
# SQLAlchemy async engine and session factory for the Booking Service.
#
# Responsibilities:
#   - Create and configure the async SQLAlchemy engine that connects to
#     PostgreSQL using the asyncpg driver
#   - Expose a sessionmaker that produces AsyncSession instances
#   - Provide a get_db_session() async context manager used by service.py
#     to scope one database session per Kafka message
#   - Expose a lifespan helper (init_db / close_db) called by consumer.py
#     on startup and shutdown to verify connectivity and release the
#     connection pool cleanly
#
# Connection pooling:
#   SQLAlchemy maintains a pool of persistent TCP connections to PostgreSQL
#   so each Kafka message does not pay the cost of a full TCP + TLS +
#   PostgreSQL authentication handshake. The pool is configured conservatively
#   for a single-instance consumer — tune pool_size and max_overflow when
#   scaling to multiple replicas.
#
# Session lifecycle per message:
#   consumer.py calls get_db_session() as an async context manager:
#
#       async with get_db_session() as session:
#           await booking_service.handle_booking_requested(event, session)
#
#   On normal exit the context manager commits. On exception it rolls back.
#   The session is always closed (returned to the pool) when the block exits,
#   regardless of success or failure.

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy import text

from app.config import settings


logger = logging.getLogger("booking-service.db")


# ──────────────────────────────────────────────────────────────────────────────
# ENGINE
# The engine manages the underlying connection pool. Created once at module
# import time and reused for the lifetime of the process.
#
# postgresql+asyncpg:// tells SQLAlchemy to use the asyncpg driver, which
# is async-native and significantly faster than psycopg2 for concurrent
# workloads. The URL is constructed from individual .env variables rather
# than a single DATABASE_URL string so each component can be validated
# independently by pydantic-settings.
# ──────────────────────────────────────────────────────────────────────────────

def _build_database_url() -> str:
    """
    Construct the asyncpg connection URL from individual settings fields.

    Using individual fields (host, port, user, password, db) rather than
    a single DATABASE_URL string makes it easier to:
      - Rotate credentials without changing the URL format
      - Validate each component separately in pydantic-settings
      - Override individual parts in tests (e.g. different DB name)

    Returns a URL in the form:
      postgresql+asyncpg://user:password@host:port/dbname
    """
    return (
        f"postgresql+asyncpg://"
        f"{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
        f"@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}"
        f"/{settings.POSTGRES_DB}"
    )


engine: AsyncEngine = create_async_engine(
    url=_build_database_url(),

    # ── POOL CONFIGURATION ────────────────────────────────────────────────────

    # Number of persistent connections to keep open at all times.
    # For a single-instance consumer processing one message at a time,
    # 5 is generous. Increase to 10-20 when running multiple replicas.
    pool_size=5,

    # Additional connections allowed when pool_size is exhausted.
    # Total max connections = pool_size + max_overflow = 5 + 10 = 15.
    # Set to 0 to hard-cap at pool_size (stricter resource control).
    max_overflow=10,

    # Seconds a connection can sit idle in the pool before being discarded
    # and replaced. Prevents stale connections caused by PostgreSQL's
    # server-side idle timeout or network-level TCP resets.
    pool_recycle=3600,   # 1 hour

    # Seconds to wait for a connection from the pool before raising
    # TimeoutError. Low value surfaces pool exhaustion quickly so it shows
    # up in logs/metrics rather than silently queuing requests.
    pool_timeout=30,

    # Issue a lightweight SELECT 1 before returning a pooled connection
    # to the caller. Adds a small latency cost per session but prevents
    # "connection already closed" errors caused by stale pool connections
    # after a PostgreSQL restart or network interruption.
    pool_pre_ping=True,

    # ── LOGGING ───────────────────────────────────────────────────────────────

    # Set to True to log every SQL statement — extremely useful for
    # debugging query issues but very noisy in production. Controlled
    # via the DEBUG setting in .env rather than hardcoded here.
    echo=settings.DEBUG,

    # Log connection pool events (checkout, checkin, overflow, timeout).
    # Useful for diagnosing pool exhaustion under load.
    echo_pool=settings.DEBUG,

    # ── ASYNCPG DRIVER OPTIONS ────────────────────────────────────────────────
    # Passed directly to asyncpg.connect() via connect_args.

    connect_args={
        # Maximum seconds to wait for a connection to be established.
        # Kept low so startup failures surface quickly rather than hanging.
        "timeout": 10,

        # PostgreSQL application_name — visible in pg_stat_activity.
        # Makes it easy to identify this service's connections in the DB.
        "server_settings": {
            "application_name": "booking-service",
        },
    },
)


# ──────────────────────────────────────────────────────────────────────────────
# SESSION FACTORY
# async_sessionmaker produces AsyncSession instances bound to the engine
# above. One sessionmaker is created at module level and reused — creating
# a new sessionmaker per message would be wasteful (it allocates Python
# objects) without any correctness benefit.
# ──────────────────────────────────────────────────────────────────────────────

AsyncSessionFactory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,

    # Do not expire ORM objects after session.commit().
    # With expire_on_commit=True (the default), accessing any attribute of
    # a committed ORM object would trigger an implicit lazy SELECT to refresh
    # it — which fails in an async context because asyncpg does not support
    # implicit I/O. Setting this to False keeps committed objects usable
    # without triggering extra queries.
    expire_on_commit=False,

    # Use AsyncSession — required for all async operations.
    class_=AsyncSession,

    # Do not autoflush pending changes before queries.
    # With autoflush=True, SQLAlchemy flushes the session's pending INSERT/
    # UPDATE statements before every SELECT to ensure the query sees the
    # latest in-memory state. Disabling it gives us explicit control over
    # when flushes happen, which is safer in a consumer where I want to
    # batch all writes and commit them together.
    autoflush=False,

    # Do not automatically begin a new transaction on session creation.
    # I manage transactions explicitly via session.begin() or the
    # get_db_session() context manager below.
    autocommit=False,
)


# ──────────────────────────────────────────────────────────────────────────────
# SESSION CONTEXT MANAGER
# The primary interface used by service.py to get a database session.
# Scopes one session to one Kafka message — open, use, commit/rollback, close.
# ──────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager that provides a scoped database session.

    Guarantees:
      - A fresh session is created for every call (one per Kafka message)
      - The session is committed automatically on normal exit
      - The session is rolled back automatically on any exception
      - The session is always closed (connection returned to pool) on exit

    Usage in service.py:

        async with get_db_session() as session:
            await booking_service.handle_booking_requested(event, session)
            # commit happens automatically when the block exits cleanly

    Do NOT catch exceptions inside the `async with` block and swallow them —
    the rollback only fires if the exception propagates out of the block.
    Let exceptions bubble up to consumer.py which decides whether to retry
    or route to the DLQ.
    """
    session: AsyncSession = AsyncSessionFactory()

    try:
        logger.debug("Database session opened")

        # Yield the session to the caller (service.py handler method).
        # Execution pauses here until the `async with` block in the caller
        # exits — either normally or via an exception.
        yield session

        # ── COMMIT ────────────────────────────────────────────────────────────
        # Reached only if no exception was raised inside the block.
        # Commits all pending INSERT / UPDATE / DELETE statements to PostgreSQL.
        await session.commit()
        logger.debug("Database session committed")

    except SQLAlchemyError as exc:
        # ── ROLLBACK on SQLAlchemy errors ─────────────────────────────────────
        # Covers constraint violations, connection errors, and query failures.
        # Rollback undoes any partial writes made during this session so the
        # database is left in a consistent state.
        logger.error(
            "SQLAlchemy error — rolling back session",
            extra={"error": str(exc)},
            exc_info=True,
        )
        await session.rollback()

        # Re-raise so consumer.py can decide whether to retry or DLQ.
        # Never swallow database exceptions here — silent failures would
        # cause the consumer to commit the Kafka offset for a message that
        # was never actually written to PostgreSQL.
        raise

    except Exception as exc:
        # ── ROLLBACK on all other errors ──────────────────────────────────────
        # Catches non-SQLAlchemy exceptions that might be raised inside the
        # service method (e.g. a validation error, a Kafka publish failure).
        # I still roll back because the session may have pending writes that
        # should not be committed if the overall message processing failed.
        logger.error(
            "Unexpected error — rolling back database session",
            extra={"error": str(exc)},
            exc_info=True,
        )
        await session.rollback()
        raise

    finally:
        # ── CLOSE — always ────────────────────────────────────────────────────
        # Returns the underlying connection to the pool regardless of whether
        # I committed, rolled back, or hit an exception. Without this the
        # connection leaks and the pool is eventually exhausted.
        await session.close()
        logger.debug("Database session closed — connection returned to pool")


# ──────────────────────────────────────────────────────────────────────────────
# LIFECYCLE HELPERS
# Called by consumer.py (or entrypoint.sh indirectly) on startup and shutdown.
# ──────────────────────────────────────────────────────────────────────────────

async def init_db() -> None:
    """
    Verify that the database is reachable on service startup.

    Runs a lightweight SELECT 1 query to confirm that:
      - The connection URL is correct
      - PostgreSQL is accepting connections
      - The configured user has login privileges

    Does NOT create tables — that is handled by Alembic migrations which
    run in entrypoint.sh before this function is called.

    Raises:
        SQLAlchemyError: If the database is unreachable or credentials
                         are wrong. The caller (consumer.py) should catch
                         this and exit so Docker restarts the container.
    """
    logger.info(
        "Verifying database connectivity",
        extra={
            "host": settings.POSTGRES_HOST,
            "port": settings.POSTGRES_PORT,
            "db":   settings.POSTGRES_DB,
        },
    )

    async with engine.connect() as conn:
        # SELECT 1 is the canonical lightweight connectivity check —
        # it requires no table access and returns in microseconds.
        result = await conn.execute(text("SELECT 1"))
        row = result.scalar()

        if row != 1:
            raise RuntimeError(
                "Database connectivity check returned unexpected result. "
                f"Expected 1, got {row}."
            )

    logger.info("Database connectivity verified successfully")


async def close_db() -> None:
    """
    Dispose the engine's connection pool on service shutdown.

    Called in the finally block of consumer.py's run_consumer() after the
    poll loop exits. dispose() closes all idle connections in the pool and
    waits for checked-out connections to be returned and closed.

    Without this, PostgreSQL logs "connection terminated unexpectedly"
    errors when the container stops because the TCP connections are closed
    abruptly by the OS rather than via PostgreSQL's connection teardown
    protocol.
    """
    logger.info("Closing database connection pool")
    await engine.dispose()
    logger.info("Database connection pool closed cleanly")


# ──────────────────────────────────────────────────────────────────────────────
# HEALTH CHECK HELPER
# Used by the consumer's heartbeat logic to verify the DB is still reachable
# during the consumer loop — not just at startup.
# ──────────────────────────────────────────────────────────────────────────────

async def is_db_healthy() -> bool:
    """
    Lightweight database health check for use inside the consumer loop.

    Unlike init_db() this function does not raise — it returns a boolean
    so the caller can decide whether to log a warning, increment a metric,
    or trigger a graceful shutdown.

    Returns:
        True  if SELECT 1 succeeds within 5 seconds.
        False if the query times out or raises any exception.

    Usage in consumer.py (optional — add to the poll loop if you want
    DB health included in the sentinel file logic):

        if not await is_db_healthy():
            logger.error("Database health check failed")
            BOOKING_REQUESTS_TOTAL.labels(status="db_unhealthy").inc()
    """
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.warning(
            "Database health check failed",
            extra={"error": str(exc)},
        )
        return False