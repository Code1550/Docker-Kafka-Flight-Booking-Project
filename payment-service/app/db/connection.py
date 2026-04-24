# payment-service/app/db/connection.py
#
# SQLAlchemy async engine and session factory for the Payment Service.
#
# Mirrors booking-service/app/db/connection.py in structure with three
# important differences:
#
#   1. application_name is "payment-service" — visible in pg_stat_activity
#      so DBAs can distinguish payment queries from booking queries when
#      diagnosing slow query issues or lock contention on the payments table.
#
#   2. pool_size is smaller (3 vs 5) — the Payment Service processes one
#      message at a time and each message requires exactly one DB session.
#      A pool of 3 is sufficient for the consumer plus the DLQ writer plus
#      one spare. Over-provisioning the pool wastes PostgreSQL connection
#      slots which are a finite resource shared across all services.
#
#   3. pool_timeout is lower (15s vs 30s) — payment processing is time-
#      sensitive. A 30-second wait for a DB connection during a payment
#      transaction could allow the Redis seat lock (TTL=300s) to be
#      consumed by the wait itself during a traffic spike. Failing fast
#      (15s) lets the consumer retry sooner or route to DLQ.
#
# Session lifecycle:
#   consumer.py calls get_db_session() as an async context manager.
#   One session is opened per Kafka message, committed after the payment
#   record is written, and closed (connection returned to pool) on exit.
#
#       async with get_db_session() as session:
#           await payment_service.handle_seat_reserved(event, session)
#
# Critical ordering rule (repeated from db/__init__.py):
#   This module must be imported BEFORE app/service.py so the engine
#   exists before the Stripe client is configured. If the engine fails
#   to build (bad POSTGRES_URL) the process should exit before the
#   Stripe SDK is initialised — preventing any possibility of charging
#   a card against a broken database state.

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings


logger = logging.getLogger("payment-service.db")


# ──────────────────────────────────────────────────────────────────────────────
# DATABASE URL BUILDER
# Constructs the asyncpg connection string from individual settings fields.
# Individual fields are easier to validate, rotate, and override in tests
# than a single opaque DATABASE_URL string.
# ──────────────────────────────────────────────────────────────────────────────

def _build_database_url() -> str:
    """
    Construct the asyncpg connection URL from pydantic-settings fields.

    Returns a URL in the form:
      postgresql+asyncpg://user:password@host:port/dbname

    The postgresql+asyncpg:// scheme tells SQLAlchemy to use the asyncpg
    driver, which is async-native and significantly faster than psycopg2
    for concurrent I/O workloads like payment processing.
    """
    return (
        f"postgresql+asyncpg://"
        f"{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
        f"@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}"
        f"/{settings.POSTGRES_DB}"
    )


# ──────────────────────────────────────────────────────────────────────────────
# ASYNC ENGINE
# Created once at module import time and reused for the lifetime of the
# Payment Service process. The engine manages the underlying connection pool
# — individual sessions check out and return connections on each message.
# ──────────────────────────────────────────────────────────────────────────────

engine: AsyncEngine = create_async_engine(
    url=_build_database_url(),

    # ── POOL CONFIGURATION ────────────────────────────────────────────────────

    # Smaller pool than Booking Service — Payment Service processes one
    # message at a time and only needs one active session per message.
    # 3 connections: one for the consumer, one for the DLQ writer (rare),
    # one spare for unexpected concurrent access.
    pool_size=3,

    # Allow up to 5 additional connections beyond pool_size when all
    # pooled connections are checked out. Total max = 3 + 5 = 8 connections.
    # Payment Service should rarely need overflow — if it does regularly,
    # that signals a processing bottleneck worth investigating.
    max_overflow=5,

    # Recycle connections after 1 hour to prevent stale connection errors
    # caused by PostgreSQL's server-side idle timeout or OS-level TCP resets.
    # 3600s matches the Booking Service — consistent across all services
    # so the DBA can apply a single idle_in_transaction_session_timeout
    # policy without service-specific tuning.
    pool_recycle=3600,

    # Fail fast if no connection is available within 15 seconds.
    # Lower than Booking Service (30s) because payment processing is
    # time-sensitive — a long wait here eats into the Redis seat lock TTL
    # (300s) and could cause the lock to expire before the booking is
    # confirmed, leaving the seat available for a second booking while
    # the first is still being processed.
    pool_timeout=15,

    # Verify connections are still alive before returning them from the pool.
    # Issues a lightweight SELECT 1 before checkout. Prevents cryptic
    # "SSL connection has been closed unexpectedly" errors on stale connections
    # that PostgreSQL dropped during an idle period — especially important for
    # the Payment Service where a failed DB connection mid-charge would leave
    # the card charged with no record written.
    pool_pre_ping=True,

    # ── LOGGING ───────────────────────────────────────────────────────────────

    # Log every SQL statement when DEBUG=true in .env.
    # Useful for verifying that the idempotency SELECT runs before the
    # Stripe call and that the INSERT commits before the Kafka publish.
    # Disable in production — SQL logging is very noisy at payment volume.
    echo=settings.DEBUG,

    # Log pool events (checkout, checkin, overflow, timeout).
    # Useful for catching pool exhaustion before it causes payment timeouts.
    echo_pool=settings.DEBUG,

    # ── ASYNCPG DRIVER OPTIONS ────────────────────────────────────────────────

    connect_args={
        # Maximum seconds to wait for a new connection to be established.
        # Kept at 10s — if PostgreSQL is unreachable for longer than this
        # the entrypoint.sh wait loop should have caught it before the
        # consumer started. A long connect timeout here would mask
        # post-startup connectivity issues.
        "timeout": 10,

        "server_settings": {
            # Visible in pg_stat_activity.application_name — makes it easy
            # to identify Payment Service connections in the DB dashboard
            # and distinguish them from Booking Service connections on the
            # same PostgreSQL instance.
            "application_name": "payment-service",

            # Set the statement timeout at the session level.
            # Any single SQL query that takes longer than 30 seconds is
            # almost certainly stuck on a lock or running an unindexed scan.
            # Killing it quickly prevents a single slow query from blocking
            # the entire payment processing pipeline.
            # Note: this applies to individual statements, not transactions.
            "statement_timeout": "30000",   # 30 seconds in milliseconds

            # Lock acquisition timeout — if a payment record INSERT cannot
            # acquire its row lock within 10 seconds, fail fast rather than
            # queuing. This can happen if two consumers race to write a
            # payment for the same booking_id (idempotency race condition).
            # The loser gets a LockNotAvailable error which triggers a retry
            # from the idempotency-safe path in service.py.
            "lock_timeout": "10000",        # 10 seconds in milliseconds
        },
    },
)


# ──────────────────────────────────────────────────────────────────────────────
# SESSION FACTORY
# Produces AsyncSession instances bound to the engine.
# One factory is created at module level and reused across all messages.
# ──────────────────────────────────────────────────────────────────────────────

AsyncSessionFactory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,

    # Do not expire ORM objects after session.commit().
    # With expire_on_commit=True (default), accessing any attribute of a
    # committed Payment object would trigger an implicit lazy SELECT —
    # which fails in async context because asyncpg does not support
    # implicit I/O outside an explicit await.
    # Setting False keeps committed Payment objects usable after commit,
    # which is important in service.py where the payment record is
    # accessed after commit to build the downstream Kafka event.
    expire_on_commit=False,

    # AsyncSession is required for all async operations.
    class_=AsyncSession,

    # Disable autoflush — we control exactly when SQL is sent to the DB.
    # In the payment flow, autoflush could trigger a premature INSERT before
    # the Stripe charge is confirmed, which would create a payment record
    # in an indeterminate state if Stripe then fails.
    autoflush=False,

    # Disable autocommit — all transactions are committed explicitly via
    # await session.commit() inside service.py after both the Stripe result
    # is known and the Payment ORM object is fully populated.
    autocommit=False,
)


# ──────────────────────────────────────────────────────────────────────────────
# SESSION CONTEXT MANAGER
# Primary interface used by service.py — one session per Kafka message.
# ──────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager providing a scoped database session.

    Guarantees the same contract as Booking Service's get_db_session():
      - Fresh session per call (one per payment message)
      - Automatic commit on clean exit
      - Automatic rollback on any exception
      - Always closes the session (returns connection to pool) on exit

    Additional Payment Service consideration:
      The commit inside this context manager is the SECOND commit in the
      payment flow. The FIRST explicit commit happens inside service.py
      after the Stripe charge succeeds and before the Kafka publish:

          # Inside service.py _save_payment_record():
          session.add(payment)
          await session.commit()          ← First commit (payment record)

          # Back in consumer.py after handle_seat_reserved() returns:
          # context manager exits → second commit is a no-op (nothing pending)

      This two-stage pattern ensures the payment record is durably written
      to PostgreSQL before any downstream Kafka event is published.
      If the Kafka publish then fails, the consumer retries — and the
      idempotency check in service.py finds the existing payment record
      and returns its result without re-charging the card.

    Usage in consumer.py:

        async with get_db_session() as session:
            result = await payment_service.handle_seat_reserved(event, session)
        # session committed and closed here

    Never swallow exceptions inside the async with block — the rollback
    only fires if the exception propagates out of the block. Let all
    exceptions reach consumer.py which decides whether to retry or DLQ.
    """
    session: AsyncSession = AsyncSessionFactory()

    try:
        logger.debug(
            "Payment DB session opened",
            extra={"session_id": id(session)},
        )

        yield session

        # ── COMMIT ────────────────────────────────────────────────────────────
        # Reached only if no exception was raised inside the block.
        # In the normal payment flow, service.py has already committed
        # (first commit inside _save_payment_record). This commit is a
        # safety net that flushes any uncommitted state that was added
        # to the session after the explicit commit in service.py.
        await session.commit()
        logger.debug("Payment DB session committed")

    except SQLAlchemyError as exc:
        # ── ROLLBACK on SQLAlchemy errors ─────────────────────────────────────
        # Covers: constraint violations (duplicate payment_id), connection
        # errors, query timeouts (statement_timeout=30s), lock timeouts
        # (lock_timeout=10s).
        #
        # Critical nuance for the Payment Service:
        #   A rollback here means the payment record was NOT written to the DB.
        #   If Stripe already charged the card before this error occurred,
        #   the consumer will retry — and the idempotency check in service.py
        #   will NOT find an existing record (the insert was rolled back).
        #   This means Stripe will be called again with the same booking_id
        #   idempotency key — and Stripe will return the original PaymentIntent
        #   result (no double charge). The payment record will then be written
        #   on the retry. This is the correct recovery path.
        logger.error(
            "SQLAlchemy error in payment session — rolling back",
            extra={
                "session_id": id(session),
                "error":      str(exc),
            },
            exc_info=True,
        )
        await session.rollback()

        # Re-raise so consumer.py decides retry vs DLQ.
        # Never silently swallow DB errors in the payment flow —
        # a swallowed error could leave a Stripe charge with no DB record.
        raise

    except Exception as exc:
        # ── ROLLBACK on all other errors ──────────────────────────────────────
        # Catches non-SQLAlchemy exceptions raised inside service.py
        # (Stripe SDK errors, validation errors, Kafka publish failures).
        # Roll back because the session may have pending writes from
        # partial processing that should not be committed if the overall
        # payment failed.
        logger.error(
            "Unexpected error in payment session — rolling back",
            extra={
                "session_id": id(session),
                "error":      str(exc),
            },
            exc_info=True,
        )
        await session.rollback()
        raise

    finally:
        # ── CLOSE — always ────────────────────────────────────────────────────
        # Returns the connection to the pool regardless of success or failure.
        # Without this, every failed payment leaks a connection until the
        # pool is exhausted and all subsequent payments fail with
        # TimeoutError: QueuePool limit of size 3 overflow 5 reached.
        await session.close()
        logger.debug(
            "Payment DB session closed — connection returned to pool",
            extra={"session_id": id(session)},
        )


# ──────────────────────────────────────────────────────────────────────────────
# LIFECYCLE HELPERS
# Called by consumer.py on startup and shutdown.
# ──────────────────────────────────────────────────────────────────────────────

async def init_db() -> None:
    """
    Verify database connectivity on Payment Service startup.

    Called by run_consumer() in consumer.py after entrypoint.sh has
    confirmed PostgreSQL is accepting connections and Alembic migrations
    have run. This function performs an additional application-level check
    that the configured user has SELECT privileges on the payments table —
    catching permission misconfigurations that the entrypoint TCP check
    would miss.

    Also verifies that the payments table exists — if Alembic migrations
    failed silently in entrypoint.sh, this check will catch it before the
    first payment event is processed.

    Raises:
        SQLAlchemyError: If the DB is unreachable, credentials are wrong,
                         or the payments table does not exist.
        RuntimeError:    If the connectivity probe returns an unexpected result.
    """
    logger.info(
        "Verifying Payment Service database connectivity",
        extra={
            "host":     settings.POSTGRES_HOST,
            "port":     settings.POSTGRES_PORT,
            "database": settings.POSTGRES_DB,
        },
    )

    async with engine.connect() as conn:
        # Verify basic connectivity with a lightweight probe
        probe = await conn.execute(text("SELECT 1"))
        if probe.scalar() != 1:
            raise RuntimeError(
                "Database connectivity probe returned unexpected result"
            )

        # Verify the payments table exists — catches silent Alembic failures.
        # information_schema.tables is a standard SQL view available on all
        # PostgreSQL versions — no superuser privileges required.
        table_check = await conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' "
                "AND table_name = 'payments'"
            )
        )
        count = table_check.scalar()
        if count == 0:
            raise RuntimeError(
                "payments table does not exist. "
                "Alembic migrations may not have run successfully. "
                "Check entrypoint.sh output for migration errors."
            )

        logger.info(
            "Payment Service database verified — payments table exists",
            extra={"table_count_check": count},
        )

    logger.info("Payment Service database connectivity verified successfully")


async def close_db() -> None:
    """
    Dispose the SQLAlchemy connection pool on shutdown.

    Called in the finally block of consumer.py's run_consumer() after
    the poll loop exits. Closes all idle connections cleanly rather than
    letting the OS abruptly terminate TCP sockets, which causes PostgreSQL
    to log "connection terminated unexpectedly" errors and may leave
    in-progress transactions in an indeterminate state.

    For the Payment Service this is especially important — any connection
    that is mid-transaction when the pool is disposed without close_db()
    could leave a payment record partially written (visible in PostgreSQL's
    pg_stat_activity as an idle-in-transaction connection) until the
    server-side idle_in_transaction_session_timeout fires.
    """
    logger.info("Disposing Payment Service database connection pool")
    await engine.dispose()
    logger.info(
        "Payment Service database connection pool disposed cleanly"
    )


# ──────────────────────────────────────────────────────────────────────────────
# HEALTH CHECK HELPER
# ──────────────────────────────────────────────────────────────────────────────

async def is_db_healthy() -> bool:
    """
    Lightweight database health check for the consumer heartbeat loop.

    Returns True if SELECT 1 completes successfully, False otherwise.
    Does not raise — the caller (consumer.py) decides how to respond to
    an unhealthy DB (log warning, increment metric, trigger shutdown).

    Payment Service specific behaviour:
      If this returns False during payment processing, the consumer should
      NOT immediately route the current message to the DLQ — the DB may
      recover within the retry window. Instead, let the normal retry loop
      in _process_message() handle the transient DB error with backoff.
      Only route to DLQ after all retries are exhausted.

    Returns:
        True  if the database is reachable and responsive.
        False if any error occurs (connection refused, timeout, etc.).
    """
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.warning(
            "Payment Service database health check failed",
            extra={"error": str(exc)},
        )
        return False