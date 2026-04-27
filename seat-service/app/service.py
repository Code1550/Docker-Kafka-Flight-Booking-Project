# seat-service/app/service.py
#
# Core business logic for the Seat Service.
#
# SeatService is the single place where seat lock domain rules live.
# It is deliberately kept free of Kafka and Redis client concerns —
# it receives a validated Pydantic event, delegates lock operations to
# an injected SeatLockManager, and returns a result dict that consumer.py
# uses to decide which Kafka event to publish.
#
# Responsibilities:
#   - handle_booking_requested → acquire a Redis seat lock, return result
#   - handle_payment_failed    → release a Redis seat lock, return result
#   - _build_lock_key          → construct the canonical Redis key format
#   - _build_reserved_event    → construct SeatReservedEvent for publisher
#   - _build_unavailable_event → construct SeatUnavailableEvent for publisher
#
# Idempotency:
#   handle_booking_requested() checks whether the booking_id already has
#   an active lock before attempting SET NX EX. If it does (re-delivery
#   after crash), the existing lock state is returned and the corresponding
#   event is re-published without attempting a second lock acquisition.
#
#   handle_payment_failed() delegates to Redis DEL which is naturally
#   idempotent — deleting a non-existent key returns 0, not an error.
#   No explicit idempotency check is needed for lock release.
#
# Dependency injection:
#   SeatService receives a SeatLockManager instance via __init__ rather
#   than importing the Redis client directly. This makes SeatService fully
#   testable with a fakeredis-backed SeatLockManager without patching any
#   module-level state — pass in a fake and the full business logic runs
#   against an in-memory Redis implementation.
#
# Lock key format:
#   seat_lock:{flight_id}:{seat_number}
#   e.g. seat_lock:f9e8d7c6-b5a4-3210-fedc-ba9876543210:12A
#
#   This format ensures:
#     - Two bookings for the same seat on the same flight → same key → conflict
#     - Two bookings for different seats on the same flight → different keys
#     - Two bookings for the same seat on different flights → different keys
#     - The key is human-readable in redis-cli for debugging
#
# Booking ID ownership key:
#   seat_lock_owner:{booking_id}
#   e.g. seat_lock_owner:a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
#   Stored alongside the seat lock to answer the question "which seat does
#   this booking_id hold a lock on?" — needed when payment.failed arrives
#   with only a booking_id and I need to derive the seat lock key to DEL.
#   Value is the seat lock key: "seat_lock:{flight_id}:{seat_number}"

from __future__ import annotations

import logging
from typing import Any

from app.cache.seat_lock_manager import SeatLockManager
from app import producer
from shared.schemas import (
    BookingRequestedEvent,
    PaymentFailedEvent,
    SeatReservedEvent,
    SeatUnavailableEvent,
)
from app.config import settings


logger = logging.getLogger("seat-service.service")


# ──────────────────────────────────────────────────────────────────────────────
# SEAT SERVICE
# ──────────────────────────────────────────────────────────────────────────────

class SeatService:
    """
    Stateless service class containing all seat lock domain logic.

    Stateless means no mutable instance variables are set after __init__.
    The SeatLockManager dependency (and therefore the Redis client) is
    the only state — and it is shared safely across all messages because
    redis-py's async client is designed for concurrent coroutine use.

    All public methods are async because they delegate to SeatLockManager
    which performs async Redis I/O. They accept Pydantic event models and
    return plain result dicts so consumer.py can handle metrics and Kafka
    publishing without coupling to SeatService's internals.
    """

    def __init__(self, lock_manager: SeatLockManager) -> None:
        """
        Initialise SeatService with an injected SeatLockManager.

        Args:
            lock_manager: A SeatLockManager instance backed by either a real
                          Redis client (production) or fakeredis (tests).
                          The lock TTL is configured on the SeatLockManager
                          at construction time from settings.SEAT_LOCK_TTL_SECONDS.
        """
        self._lock_manager = lock_manager
        logger.info(
            "SeatService initialised",
            extra={
                "lock_ttl_seconds": lock_manager.lock_ttl,
            },
        )


    # ── HANDLE booking.requested ──────────────────────────────────────────────

    async def handle_booking_requested(
        self,
        event: BookingRequestedEvent,
    ) -> dict[str, Any]:
        """
        Process a BookingRequestedEvent by attempting to acquire a seat lock.

        Steps:
          1. Build the canonical seat lock key from flight_id + seat_number
          2. Check idempotency — if booking_id already owns a lock, return
             the existing lock state without re-acquiring
          3. Attempt Redis SET NX EX on the seat lock key
          4. If acquired → store the booking_id ownership key, publish
             SeatReservedEvent, return success result
          5. If rejected → publish SeatUnavailableEvent, return failure result

        Why check idempotency before attempting SET NX EX?
          SET NX EX is atomic — if the key already exists it returns False.
          However, it cannot distinguish between:
            a) Key exists because another booking holds the seat (conflict)
            b) Key exists because THIS booking already acquired it on a
               previous delivery attempt (idempotent re-delivery)
          The booking_id ownership key resolves this ambiguity:
          if {booking_id} owns the lock, case (b) — return cached result.
          If {booking_id} does not own the lock, case (a) — seat conflict.

        Args:
            event: The deserialized BookingRequestedEvent from Kafka.

        Returns:
            On lock acquired:
              {
                "lock_acquired": True,
                "lock_key":      str,   # seat_lock:{flight_id}:{seat_number}
                "owner_key":     str,   # seat_lock_owner:{booking_id}
                "reason":        None,
                "event":         SeatReservedEvent,
              }
            On seat unavailable:
              {
                "lock_acquired": False,
                "lock_key":      str,
                "owner_key":     None,
                "reason":        str,   # human-readable conflict reason
                "event":         SeatUnavailableEvent,
              }

        Raises:
            redis.exceptions.ConnectionError: If Redis is unreachable.
                consumer.py catches this and retries with backoff.
            redis.exceptions.TimeoutError: If Redis does not respond in time.
                consumer.py catches this and retries with backoff.
        """
        booking_id     = event.booking_id
        correlation_id = event.correlation_id
        flight_id      = event.flight.flight_id
        seat_number    = event.flight.seat_number

        # ── STEP 1: Build lock key ─────────────────────────────────────────────
        lock_key  = self._build_lock_key(flight_id, seat_number)
        owner_key = self._build_owner_key(booking_id)

        logger.info(
            "Attempting seat lock acquisition",
            extra={
                "booking_id":     booking_id,
                "correlation_id": correlation_id,
                "lock_key":       lock_key,
                "owner_key":      owner_key,
                "seat_class":     event.flight.seat_class.value,
            },
        )

        # ── STEP 2: Idempotency check ──────────────────────────────────────────
        # Check whether this booking_id already owns a lock from a previous
        # delivery attempt. If it does, return the cached result and re-publish
        # the downstream event without touching the seat lock state.
        existing_lock_key = await self._lock_manager.get_owner_lock_key(owner_key)

        if existing_lock_key:
            # This booking_id already acquired a lock on a previous delivery.
            # Verify the seat lock key matches what I would acquire now -
            # a mismatch indicates data corruption and should alert loudly.
            if existing_lock_key != lock_key:
                logger.error(
                    "Idempotency check found mismatched lock key — "
                    "booking_id owns a different seat than requested",
                    extra={
                        "booking_id":          booking_id,
                        "expected_lock_key":   lock_key,
                        "existing_lock_key":   existing_lock_key,
                    },
                )
                # Treat as seat unavailable — safer than overwriting a lock
                reason = (
                    f"Lock key mismatch: booking {booking_id} already owns "
                    f"{existing_lock_key}, requested {lock_key}"
                )
                unavailable_event = self._build_unavailable_event(
                    event=event,
                    reason=reason,
                )
                await producer.publish_seat_unavailable(unavailable_event)
                return {
                    "lock_acquired": False,
                    "lock_key":      lock_key,
                    "owner_key":     None,
                    "reason":        reason,
                    "event":         unavailable_event,
                }

            # Keys match — this is a genuine re-delivery. Return cached result.
            logger.warning(
                "Idempotent re-delivery detected — "
                "booking_id already holds lock, re-publishing SeatReservedEvent",
                extra={
                    "booking_id": booking_id,
                    "lock_key":   lock_key,
                },
            )
            reserved_event = self._build_reserved_event(event=event)
            await producer.publish_seat_reserved(reserved_event)
            return {
                "lock_acquired": True,
                "lock_key":      lock_key,
                "owner_key":     owner_key,
                "reason":        None,
                "event":         reserved_event,
            }

        # ── STEP 3: Attempt lock acquisition ──────────────────────────────────
        # SET NX EX — atomic. Returns True if the key was set (lock acquired),
        # False if the key already exists (seat locked by another booking).
        lock_acquired = await self._lock_manager.acquire_lock(
            lock_key=lock_key,
            booking_id=booking_id,
        )

        if lock_acquired:
            # ── STEP 4: Lock acquired ─────────────────────────────────────────

            # Store the booking_id → lock_key mapping so payment.failed can
            # derive the lock key from the booking_id for DEL.
            # TTL matches the seat lock TTL — owner key expires at the same
            # time as the lock so no orphaned owner keys accumulate in Redis.
            await self._lock_manager.set_owner_key(
                owner_key=owner_key,
                lock_key=lock_key,
            )

            logger.info(
                "Seat lock acquired successfully",
                extra={
                    "booking_id":  booking_id,
                    "lock_key":    lock_key,
                    "owner_key":   owner_key,
                    "ttl_seconds": settings.SEAT_LOCK_TTL_SECONDS,
                },
            )

            # Publish SeatReservedEvent to trigger Payment Service
            reserved_event = self._build_reserved_event(event=event)
            await producer.publish_seat_reserved(reserved_event)

            return {
                "lock_acquired": True,
                "lock_key":      lock_key,
                "owner_key":     owner_key,
                "reason":        None,
                "event":         reserved_event,
            }

        else:
            # ── STEP 5: Seat unavailable ──────────────────────────────────────
            # Another booking holds the Redis lock on this seat.
            # Publish SeatUnavailableEvent so Booking Service marks the
            # booking FAILED and the passenger is notified to choose another seat.

            # Retrieve the booking_id that currently holds the lock for
            # logging — helps operators debug race conditions in real time.
            current_owner = await self._lock_manager.get_lock_owner(lock_key)

            reason = (
                f"Seat {seat_number} on flight {flight_id} is already locked"
                + (f" by booking {current_owner}" if current_owner else "")
            )

            logger.warning(
                "Seat lock acquisition failed — seat already locked",
                extra={
                    "booking_id":    booking_id,
                    "lock_key":      lock_key,
                    "current_owner": current_owner,
                    "reason":        reason,
                },
            )

            unavailable_event = self._build_unavailable_event(
                event=event,
                reason=reason,
            )
            await producer.publish_seat_unavailable(unavailable_event)

            return {
                "lock_acquired": False,
                "lock_key":      lock_key,
                "owner_key":     None,
                "reason":        reason,
                "event":         unavailable_event,
            }


    # ── HANDLE payment.failed ─────────────────────────────────────────────────

    async def handle_payment_failed(
        self,
        event: PaymentFailedEvent,
    ) -> dict[str, Any]:
        """
        Process a PaymentFailedEvent by releasing the Redis seat lock.

        Called when Payment Service could not charge the passenger — either
        a card decline or exhausted retries. The seat must be unlocked so
        other passengers can book it.

        Steps:
          1. Derive the seat lock key from the booking_id ownership key
          2. Delete the seat lock key (Redis DEL — idempotent)
          3. Delete the booking_id ownership key (cleanup)
          4. Return the release result

        Why derive the lock key from the owner key instead of the event?
          PaymentFailedEvent carries booking_id, passenger details, flight
          details, and payment details — but NOT the exact seat_number and
          flight_id combination from the original booking.requested event.
          (In the current schema, PaymentFailedEvent uses placeholder flight
          data — see payment-service/app/service.py _build_failed_event().)
          Using the owner key avoids any dependency on the event's flight
          data accuracy — the owner key always points to the correct lock.

        Args:
            event: The deserialized PaymentFailedEvent from Kafka.

        Returns:
            {
              "lock_released": bool,   # True if DEL deleted a key
              "lock_key":      str,    # the lock key that was targeted
              "owner_key":     str,    # the owner key that was cleaned up
              "was_present":   bool,   # False if lock had already expired
            }

        Raises:
            redis.exceptions.ConnectionError: If Redis is unreachable.
                consumer.py catches this and retries. If all retries fail,
                the seat lock expires automatically via TTL (self-healing).
        """
        booking_id     = event.booking_id
        correlation_id = event.correlation_id
        owner_key      = self._build_owner_key(booking_id)

        logger.info(
            "Attempting seat lock release for failed payment",
            extra={
                "booking_id":     booking_id,
                "correlation_id": correlation_id,
                "failure_reason": event.payment.failure_reason,
                "retry_attempt":  event.retry_attempt,
                "owner_key":      owner_key,
            },
        )

        # ── STEP 1: Derive lock key from owner key ─────────────────────────────
        # Look up which seat lock this booking_id owns in Redis.
        # Returns None if the lock has already expired via TTL or was
        # never acquired (booking.requested went to DLQ before lock was set).
        lock_key = await self._lock_manager.get_owner_lock_key(owner_key)

        if not lock_key:
            # No owner key found — lock was never acquired or already expired.
            # This is not an error — log at INFO and return gracefully.
            # The seat is already available (lock expired via TTL or never set).
            logger.info(
                "No seat lock found for booking — already released or expired",
                extra={
                    "booking_id": booking_id,
                    "owner_key":  owner_key,
                },
            )
            return {
                "lock_released": False,
                "lock_key":      None,
                "owner_key":     owner_key,
                "was_present":   False,
            }

        # ── STEP 2: Delete seat lock key ──────────────────────────────────────
        # Redis DEL is atomic and idempotent — safe to call even if the
        # key has already expired. Returns the number of keys deleted (0 or 1).
        keys_deleted = await self._lock_manager.release_lock(lock_key=lock_key)

        was_present = keys_deleted > 0

        if was_present:
            logger.info(
                "Seat lock released — seat now available for other bookings",
                extra={
                    "booking_id": booking_id,
                    "lock_key":   lock_key,
                },
            )
        else:
            # Lock key expired between get_owner_lock_key and release_lock.
            # TTL race — the key expired in the ~1ms between lookup and DEL.
            # Seat is already available — no action needed.
            logger.info(
                "Seat lock expired between lookup and release — seat already available",
                extra={
                    "booking_id": booking_id,
                    "lock_key":   lock_key,
                },
            )

        # ── STEP 3: Delete owner key ───────────────────────────────────────────
        # Clean up the booking_id → lock_key mapping now that the lock is
        # released. Without this, the owner key would linger in Redis until
        # its TTL expires — wasting memory and potentially confusing future
        # idempotency checks if booking_ids were ever reused (they are not,
        # but defensive cleanup is always good practice).
        await self._lock_manager.delete_owner_key(owner_key=owner_key)

        logger.info(
            "Seat lock owner key cleaned up",
            extra={
                "booking_id": booking_id,
                "owner_key":  owner_key,
            },
        )

        return {
            "lock_released": True,
            "lock_key":      lock_key,
            "owner_key":     owner_key,
            "was_present":   was_present,
        }


    # ── EVENT BUILDERS ────────────────────────────────────────────────────────

    def _build_reserved_event(
        self,
        event: BookingRequestedEvent,
    ) -> SeatReservedEvent:
        """
        Construct a SeatReservedEvent from the original BookingRequestedEvent.

        The SeatReservedEvent is intentionally lean — it carries only the
        data Payment Service needs to identify the booking and begin charging:
          - booking_id     → Kafka message key and Stripe idempotency key
          - flight_id      → for Payment Service logging and DB record
          - seat_number    → for Payment Service DB record
          - seat_class     → used by Payment Service to look up the fare price
          - lock_ttl       → how long the Redis seat lock is valid

        It does NOT embed full PassengerDetail or FlightInfo — those are
        carried by PaymentProcessedEvent and PaymentFailedEvent after the
        charge outcome is known. Keeping SeatReservedEvent lean minimises
        Kafka message size and reduces the data Payment Service needs to
        validate before charging.

        Args:
            event: The original BookingRequestedEvent to derive fields from.

        Returns:
            A fully populated SeatReservedEvent ready to publish.
        """
        return SeatReservedEvent(
            correlation_id=event.correlation_id,
            booking_id=event.booking_id,
            flight_id=event.flight.flight_id,
            seat_number=event.flight.seat_number,
            seat_class=event.flight.seat_class,
            lock_ttl=settings.SEAT_LOCK_TTL_SECONDS,
        )


    def _build_unavailable_event(
        self,
        event: BookingRequestedEvent,
        reason: str,
    ) -> SeatUnavailableEvent:
        """
        Construct a SeatUnavailableEvent when a seat lock cannot be acquired.

        Called when:
          - SET NX EX returns False (another booking holds the lock)
          - An idempotency check finds a mismatched lock key (data error)

        The reason string is stored by Booking Service in the
        failure_reason column of the bookings table and shown to
        customer support agents when investigating failed bookings.
        It should be human-readable and specific enough to diagnose
        the cause without access to internal logs.

        Args:
            event:  The original BookingRequestedEvent.
            reason: A clear, human-readable explanation of why the seat
                    is unavailable e.g. "Seat 12A on flight AA123 is
                    already locked by booking b2c3d4e5-...".

        Returns:
            A fully populated SeatUnavailableEvent ready to publish.
        """
        return SeatUnavailableEvent(
            correlation_id=event.correlation_id,
            booking_id=event.booking_id,
            flight_id=event.flight.flight_id,
            seat_number=event.flight.seat_number,
            reason=reason,
        )


    # ── PRIVATE HELPERS ───────────────────────────────────────────────────────

    @staticmethod
    def _build_lock_key(flight_id: str, seat_number: str) -> str:
        """
        Build the canonical Redis key for a seat lock.

        Format: seat_lock:{flight_id}:{seat_number}
        Example: seat_lock:f9e8d7c6-b5a4-3210-fedc-ba9876543210:12A

        Why this format?
          - Prefixed with "seat_lock:" for Redis key namespace clarity.
            All seat lock keys can be found with SCAN MATCH seat_lock:*
            in redis-cli without matching other key types.
          - flight_id before seat_number — flight_id is the higher-cardinality
            component. Grouping by flight first means SCAN MATCH
            seat_lock:{flight_id}:* returns all locked seats on a specific
            flight — useful for flight cancellation workflows where all seats
            for a flight must be released simultaneously.
          - seat_number last — human-readable suffix for debugging.
            "seat_lock:...:12A" immediately communicates which seat is locked
            when browsing keys in redis-cli or Redis Insight.

        Args:
            flight_id:   UUID of the flight.
            seat_number: Seat identifier e.g. "12A".

        Returns:
            The Redis key string for this seat lock.
        """
        return f"seat_lock:{flight_id}:{seat_number}"


    @staticmethod
    def _build_owner_key(booking_id: str) -> str:
        """
        Build the Redis key that maps a booking_id to its seat lock key.

        Format: seat_lock_owner:{booking_id}
        Example: seat_lock_owner:a1b2c3d4-e5f6-7890-abcd-ef1234567890

        Stored when a seat lock is acquired. Value is the seat lock key:
          "seat_lock:{flight_id}:{seat_number}"

        Used in two scenarios:
          1. Idempotency check in handle_booking_requested() — does this
             booking_id already own a lock? (re-delivery detection)
          2. Lock release in handle_payment_failed() — which seat lock
             does this booking_id hold? (derive lock_key from booking_id)

        Without this key, handle_payment_failed() would need to scan all
        seat_lock:* keys to find the one owned by the booking_id — O(n)
        instead of O(1) for a single Redis GET.

        Args:
            booking_id: UUID of the booking.

        Returns:
            The Redis key string for this booking's lock ownership record.
        """
        return f"seat_lock_owner:{booking_id}"