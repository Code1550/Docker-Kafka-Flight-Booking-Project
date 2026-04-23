# booking-service/app/service.py
#
# Core business logic for the Booking Service.
#
# BookingService is the single place where domain rules live. It is
# deliberately kept free of Kafka and HTTP concerns — it receives a
# validated Pydantic event, interacts with the database via an injected
# SQLAlchemy async session, and delegates publishing to producer.py.
#
# This separation means BookingService can be unit-tested with a mock
# session and mock producer without a running Kafka broker or PostgreSQL
# instance — the most valuable tests in the project.
#
# Responsibilities:
#   - handle_booking_requested  → create booking record, publish seat.reserved
#   - handle_payment_processed  → confirm booking, publish booking.confirmed
#   - handle_payment_failed     → mark booking failed, release state
#   - handle_seat_unavailable   → mark booking failed, notify via DLQ
#
# Idempotency:
#   Every handler checks whether the booking already exists before writing.
#   This is essential because Kafka guarantees at-least-once delivery —
#   the same message can arrive more than once after a consumer restart.
#   A second delivery of the same event must produce the same outcome
#   as the first, not a duplicate database record or a duplicate charge.

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Booking, Flight
from app import producer
from shared.schemas import (
    BookingRequestedEvent,
    BookingConfirmedEvent,
    BookingFailedDLQEvent,
    PaymentProcessedEvent,
    SeatUnavailableEvent,
    SeatReservedEvent,
    BookingStatus,
    PassengerDetail,
    FlightInfo,
    PaymentDetail,
    serialize_event,
)
from app.config import settings


logger = logging.getLogger("booking-service.service")


# ──────────────────────────────────────────────────────────────────────────────
# BOOKING SERVICE
# ──────────────────────────────────────────────────────────────────────────────

class BookingService:
    """
    Stateless service class containing all booking domain logic.

    Stateless means no instance variables are set after __init__ —
    every method receives all the data it needs as arguments. This makes
    the class safe to instantiate once and reuse across thousands of
    Kafka messages without risk of state leaking between messages.

    All public methods are async because they perform I/O (database reads
    and writes). They accept an AsyncSession rather than creating one
    internally so the caller (consumer.py) controls the session lifecycle
    and can roll back on error.
    """

    # ── HANDLE booking.requested ──────────────────────────────────────────────

    async def handle_booking_requested(
        self,
        event: BookingRequestedEvent,
        session: AsyncSession,
    ) -> None:
        """
        Process a BookingRequestedEvent consumed from `booking.requested`.

        Steps:
          1. Check idempotency — skip if booking already exists
          2. Verify the flight exists in the database
          3. Create a new Booking record with status REQUESTED
          4. Commit the database transaction
          5. Publish SeatReservedEvent to `seat.reserved`

        The SeatReservedEvent signals the Seat Service to acquire a Redis
        lock on the seat. Payment Service listens to `seat.reserved` and
        will charge the passenger once the lock is confirmed.

        Why publish AFTER committing the DB write?
          If we published to Kafka before committing and then the DB commit
          failed, Kafka would have a seat.reserved event for a booking that
          does not exist in PostgreSQL — a phantom booking. Always write to
          the DB first, then publish to Kafka.

        Args:
            event:   The deserialized BookingRequestedEvent from Kafka.
            session: An open AsyncSession — committed inside this method,
                     rolled back by the caller on exception.
        """
        booking_id     = event.booking_id
        correlation_id = event.correlation_id

        logger.info(
            "Handling booking.requested",
            extra={
                "booking_id":     booking_id,
                "correlation_id": correlation_id,
                "flight_id":      event.flight.flight_id,
                "seat_number":    event.flight.seat_number,
            },
        )

        # ── STEP 1: Idempotency check ─────────────────────────────────────────
        # If a booking with this booking_id already exists it means this
        # message has been delivered more than once (Kafka at-least-once).
        # Return early without writing anything — the previous delivery
        # already did the work.
        existing = await self._get_booking(booking_id, session)
        if existing:
            logger.warning(
                "Duplicate booking.requested event — skipping (idempotent)",
                extra={
                    "booking_id":     booking_id,
                    "correlation_id": correlation_id,
                    "existing_status": existing.status,
                },
            )
            return

        # ── STEP 2: Verify flight exists ──────────────────────────────────────
        # The API Gateway does not verify flight existence — it trusts the
        # client. Booking Service is the first service with DB access, so
        # this is the correct place to enforce referential integrity.
        flight = await self._get_flight(event.flight.flight_id, session)
        if not flight:
            # Flight does not exist — this is a permanent error. Route to DLQ
            # rather than retrying (retrying will not make the flight appear).
            error_msg = (
                f"Flight {event.flight.flight_id} not found in database. "
                "Booking cannot be created."
            )
            logger.error(
                error_msg,
                extra={
                    "booking_id": booking_id,
                    "flight_id":  event.flight.flight_id,
                },
            )
            await producer.publish_to_dlq(
                BookingFailedDLQEvent(
                    correlation_id=correlation_id,
                    booking_id=booking_id,
                    failed_topic=settings.TOPIC_BOOKING_REQUESTED,
                    error_message=error_msg,
                    raw_payload=serialize_event(event),
                    retry_count=0,
                )
            )
            return

        # ── STEP 3: Create booking record ─────────────────────────────────────
        booking = Booking(
            booking_id=booking_id,
            correlation_id=correlation_id,
            passenger_id=event.passenger.passenger_id,
            passenger_first_name=event.passenger.first_name,
            passenger_last_name=event.passenger.last_name,
            passenger_email=event.passenger.email,
            passenger_phone=event.passenger.phone,
            passport_number=event.passenger.passport_number,
            flight_id=event.flight.flight_id,
            flight_number=event.flight.flight_number,
            origin=event.flight.origin,
            destination=event.flight.destination,
            departure_time=event.flight.departure_time,
            arrival_time=event.flight.arrival_time,
            seat_number=event.flight.seat_number,
            seat_class=event.flight.seat_class.value,
            total_price=event.total_price,
            currency=event.currency,
            notification_type=event.notification_type.value,
            status=BookingStatus.REQUESTED.value,
            requested_at=datetime.utcnow(),
        )

        try:
            session.add(booking)
            # ── STEP 4: Commit ────────────────────────────────────────────────
            # Commit before publishing to Kafka. If the commit fails we raise
            # and the consumer retries. If Kafka publish fails after the commit
            # the consumer retries — idempotency check at the top prevents
            # a duplicate booking record on re-delivery.
            await session.commit()

        except IntegrityError:
            # Another process committed the same booking_id between our
            # idempotency check and the insert (race condition under load).
            # Roll back and treat as a duplicate — safe to ignore.
            await session.rollback()
            logger.warning(
                "IntegrityError on booking insert — treating as duplicate",
                extra={"booking_id": booking_id},
            )
            return

        logger.info(
            "Booking record created in PostgreSQL",
            extra={
                "booking_id": booking_id,
                "status":     BookingStatus.REQUESTED.value,
            },
        )

        # ── STEP 5: Publish seat.reserved ─────────────────────────────────────
        # Seat Service consumes this event and attempts to acquire a Redis
        # distributed lock on the seat. Payment Service also listens and
        # will begin the charge flow once the lock is confirmed.
        seat_reserved_event = SeatReservedEvent(
            correlation_id=correlation_id,
            booking_id=booking_id,
            flight_id=event.flight.flight_id,
            seat_number=event.flight.seat_number,
            seat_class=event.flight.seat_class,
            # TTL matches SEAT_LOCK_TTL_SECONDS in .env (default: 300s)
            # After this window the lock expires and the seat is released
            # automatically if payment has not completed.
            lock_ttl=settings.SEAT_LOCK_TTL_SECONDS,
        )

        await producer.publish_seat_reserved(seat_reserved_event)

        logger.info(
            "SeatReservedEvent published — awaiting seat lock confirmation",
            extra={
                "booking_id":  booking_id,
                "seat_number": event.flight.seat_number,
                "lock_ttl":    settings.SEAT_LOCK_TTL_SECONDS,
            },
        )


    # ── HANDLE payment.processed ──────────────────────────────────────────────

    async def handle_payment_processed(
        self,
        event: PaymentProcessedEvent,
        session: AsyncSession,
    ) -> None:
        """
        Process a PaymentProcessedEvent consumed from `payment.processed`.

        Called after the Payment Service successfully charges the passenger.

        Steps:
          1. Idempotency check — skip if booking already CONFIRMED
          2. Update booking status to CONFIRMED in PostgreSQL
          3. Publish BookingConfirmedEvent to `booking.confirmed`
             (Notification Service sends the confirmation email/SMS)

        Args:
            event:   The deserialized PaymentProcessedEvent from Kafka.
            session: An open AsyncSession.
        """
        booking_id     = event.booking_id
        correlation_id = event.correlation_id

        logger.info(
            "Handling payment.processed",
            extra={
                "booking_id":     booking_id,
                "correlation_id": correlation_id,
                "payment_id":     event.payment.payment_id,
                "amount":         str(event.payment.amount),
            },
        )

        # ── STEP 1: Idempotency check ─────────────────────────────────────────
        booking = await self._get_booking(booking_id, session)

        if not booking:
            # Payment received for an unknown booking — something is wrong
            # upstream. Route to DLQ for manual inspection.
            logger.error(
                "Received payment.processed for unknown booking — routing to DLQ",
                extra={"booking_id": booking_id},
            )
            await producer.publish_to_dlq(
                BookingFailedDLQEvent(
                    correlation_id=correlation_id,
                    booking_id=booking_id,
                    failed_topic=settings.TOPIC_PAYMENT_PROCESSED,
                    error_message=f"No booking found for booking_id={booking_id}",
                    raw_payload=serialize_event(event),
                    retry_count=0,
                )
            )
            return

        if booking.status == BookingStatus.CONFIRMED.value:
            # Already confirmed — this is a duplicate delivery. Safe to skip.
            logger.warning(
                "Duplicate payment.processed event — booking already confirmed",
                extra={"booking_id": booking_id},
            )
            return

        # ── STEP 2: Update booking status to CONFIRMED ────────────────────────
        await session.execute(
            update(Booking)
            .where(Booking.booking_id == booking_id)
            .values(
                status=BookingStatus.CONFIRMED.value,
                payment_id=event.payment.payment_id,
                payment_amount=event.payment.amount,
                payment_currency=event.payment.currency,
                payment_method=event.payment.payment_method,
                transaction_ref=event.payment.transaction_ref,
                confirmed_at=datetime.utcnow(),
            )
        )
        await session.commit()

        logger.info(
            "Booking status updated to CONFIRMED",
            extra={
                "booking_id":  booking_id,
                "payment_id":  event.payment.payment_id,
            },
        )

        # ── STEP 3: Publish booking.confirmed ─────────────────────────────────
        # Notification Service listens to this topic and sends the passenger
        # their confirmation email or SMS. This is the last event in the
        # happy path — the booking journey is complete after this publish.
        confirmed_event = BookingConfirmedEvent(
            correlation_id=correlation_id,
            booking_id=booking_id,
            passenger=event.passenger,
            flight=event.flight,
            payment=event.payment,
            status=BookingStatus.CONFIRMED,
            notification_type=booking.notification_type,
        )

        await producer.publish_booking_confirmed(confirmed_event)

        logger.info(
            "BookingConfirmedEvent published — booking journey complete",
            extra={
                "booking_id":      booking_id,
                "passenger_email": event.passenger.email,
            },
        )


    # ── HANDLE payment.failed ─────────────────────────────────────────────────

    async def handle_payment_failed(
        self,
        event,   # PaymentFailedEvent from shared/schemas.py
        session: AsyncSession,
    ) -> None:
        """
        Process a PaymentFailedEvent consumed from `payment.failed`.

        Called when the Payment Service cannot charge the passenger after
        exhausting its own retries (e.g. card declined, gateway timeout).

        Steps:
          1. Idempotency check — skip if booking already FAILED
          2. Update booking status to FAILED in PostgreSQL
          3. Publish BookingFailedDLQEvent so the passenger can be notified
             and engineers can monitor failure rates in Grafana

        Note: Seat Service also listens to `payment.failed` and releases
        the Redis seat lock independently — Booking Service does not need
        to coordinate with Seat Service directly.

        Args:
            event:   The deserialized PaymentFailedEvent from Kafka.
            session: An open AsyncSession.
        """
        booking_id     = event.booking_id
        correlation_id = event.correlation_id

        logger.warning(
            "Handling payment.failed",
            extra={
                "booking_id":      booking_id,
                "correlation_id":  correlation_id,
                "failure_reason":  event.payment.failure_reason,
                "retry_attempt":   event.retry_attempt,
            },
        )

        # ── STEP 1: Idempotency check ─────────────────────────────────────────
        booking = await self._get_booking(booking_id, session)

        if not booking:
            logger.error(
                "Received payment.failed for unknown booking — ignoring",
                extra={"booking_id": booking_id},
            )
            return

        if booking.status == BookingStatus.FAILED.value:
            logger.warning(
                "Duplicate payment.failed event — booking already marked FAILED",
                extra={"booking_id": booking_id},
            )
            return

        # ── STEP 2: Update booking status to FAILED ───────────────────────────
        await session.execute(
            update(Booking)
            .where(Booking.booking_id == booking_id)
            .values(
                status=BookingStatus.FAILED.value,
                failure_reason=event.payment.failure_reason,
                failed_at=datetime.utcnow(),
            )
        )
        await session.commit()

        logger.warning(
            "Booking status updated to FAILED",
            extra={
                "booking_id":     booking_id,
                "failure_reason": event.payment.failure_reason,
            },
        )

        # ── STEP 3: Route to DLQ for monitoring and alerting ──────────────────
        # The DLQ entry ensures the failure is visible in Kafka UI and triggers
        # the Grafana alert wired to the DLQ_MESSAGES_TOTAL Prometheus counter.
        # Notification Service can also consume the DLQ to send the passenger
        # a "booking failed" email if wired up to do so.
        await producer.publish_to_dlq(
            BookingFailedDLQEvent(
                correlation_id=correlation_id,
                booking_id=booking_id,
                failed_topic=settings.TOPIC_PAYMENT_FAILED,
                error_message=(
                    f"Payment failed after {event.retry_attempt} attempts: "
                    f"{event.payment.failure_reason}"
                ),
                raw_payload=serialize_event(event),
                retry_count=event.retry_attempt,
            )
        )


    # ── HANDLE seat.unavailable ───────────────────────────────────────────────

    async def handle_seat_unavailable(
        self,
        event: SeatUnavailableEvent,
        session: AsyncSession,
    ) -> None:
        """
        Process a SeatUnavailableEvent consumed from `seat.unavailable`.

        Called when Seat Service cannot acquire the Redis lock because
        another booking already holds it (concurrent booking race condition).

        Steps:
          1. Idempotency check — skip if booking already FAILED
          2. Update booking status to FAILED with the reason from Seat Service
          3. Route to DLQ so the failure is visible in monitoring

        The passenger will receive a "seat unavailable" notification via
        Notification Service (if wired to consume DLQ events) and should
        be prompted to select a different seat.

        Args:
            event:   The deserialized SeatUnavailableEvent from Kafka.
            session: An open AsyncSession.
        """
        booking_id     = event.booking_id
        correlation_id = event.correlation_id

        logger.warning(
            "Handling seat.unavailable",
            extra={
                "booking_id":   booking_id,
                "correlation_id": correlation_id,
                "seat_number":  event.seat_number,
                "reason":       event.reason,
            },
        )

        # ── STEP 1: Idempotency check ─────────────────────────────────────────
        booking = await self._get_booking(booking_id, session)

        if not booking:
            logger.error(
                "Received seat.unavailable for unknown booking — ignoring",
                extra={"booking_id": booking_id},
            )
            return

        if booking.status == BookingStatus.FAILED.value:
            logger.warning(
                "Duplicate seat.unavailable event — booking already marked FAILED",
                extra={"booking_id": booking_id},
            )
            return

        # ── STEP 2: Mark booking as FAILED ────────────────────────────────────
        await session.execute(
            update(Booking)
            .where(Booking.booking_id == booking_id)
            .values(
                status=BookingStatus.FAILED.value,
                failure_reason=event.reason,
                failed_at=datetime.utcnow(),
            )
        )
        await session.commit()

        logger.warning(
            "Booking status updated to FAILED due to seat unavailability",
            extra={
                "booking_id":  booking_id,
                "seat_number": event.seat_number,
                "reason":      event.reason,
            },
        )

        # ── STEP 3: Route to DLQ ──────────────────────────────────────────────
        await producer.publish_to_dlq(
            BookingFailedDLQEvent(
                correlation_id=correlation_id,
                booking_id=booking_id,
                failed_topic=settings.TOPIC_SEAT_UNAVAILABLE,
                error_message=(
                    f"Seat {event.seat_number} unavailable: {event.reason}"
                ),
                raw_payload=serialize_event(event),
                retry_count=0,
            )
        )


    # ── PRIVATE HELPERS ───────────────────────────────────────────────────────

    async def _get_booking(
        self,
        booking_id: str,
        session: AsyncSession,
    ) -> Booking | None:
        """
        Fetch a booking by its primary key.

        Used by every handler for the idempotency check at the top of each
        method. Returns None if no booking with that ID exists yet.

        Args:
            booking_id: The UUID string identifying the booking.
            session:    The current AsyncSession.

        Returns:
            The Booking ORM instance, or None if not found.
        """
        result = await session.execute(
            select(Booking).where(Booking.booking_id == booking_id)
        )
        return result.scalar_one_or_none()


    async def _get_flight(
        self,
        flight_id: str,
        session: AsyncSession,
    ) -> Flight | None:
        """
        Fetch a flight record by its primary key.

        Used in handle_booking_requested to verify the flight exists before
        creating a booking. Flights are seeded into the database by
        database/init/01_schema.sql and represent the available inventory.

        Args:
            flight_id: The UUID string identifying the flight.
            session:   The current AsyncSession.

        Returns:
            The Flight ORM instance, or None if not found.
        """
        result = await session.execute(
            select(Flight).where(Flight.flight_id == flight_id)
        )
        return result.scalar_one_or_none()


    async def _update_booking_status(
        self,
        booking_id: str,
        status: BookingStatus,
        session: AsyncSession,
        **extra_fields,
    ) -> None:
        """
        Generic status updater used by handlers that only need to change
        the status field and one or two extra columns.

        Not used by handle_booking_requested (which also sets many other
        fields on insert) but available for any future handler that needs
        a simple status transition.

        Args:
            booking_id:   The booking to update.
            status:       The new BookingStatus enum value.
            session:      The current AsyncSession.
            **extra_fields: Any additional columns to set alongside status,
                            e.g. failure_reason="card declined", failed_at=now.
        """
        await session.execute(
            update(Booking)
            .where(Booking.booking_id == booking_id)
            .values(status=status.value, **extra_fields)
        )
        await session.commit()

        logger.info(
            "Booking status updated",
            extra={
                "booking_id": booking_id,
                "new_status": status.value,
            },
        )