# payment-service/app/service.py
#
# Core business logic for the Payment Service.
#
# PaymentService is the single place where payment domain rules live.
# It is deliberately kept free of Kafka and HTTP concerns — it receives
# a validated SeatReservedEvent, interacts with the Stripe SDK (or mock),
# writes the result to PostgreSQL via an injected AsyncSession, and returns
# a result dict that consumer.py uses to decide which Kafka event to publish.
#
# Responsibilities:
#   - handle_seat_reserved   → charge the passenger via Stripe, write
#                              payment record, return success/failure result
#   - _create_payment_intent → build and confirm a Stripe PaymentIntent
#   - _handle_stripe_error   → classify Stripe errors as retryable or permanent
#   - _save_payment_record   → write the payment transaction to PostgreSQL
#   - _build_processed_event → construct PaymentProcessedEvent for publisher
#   - _build_failed_event    → construct PaymentFailedEvent for publisher
#
# Idempotency:
#   Before calling Stripe, handle_seat_reserved() checks whether a payment
#   record already exists for the booking_id. If it does, the method returns
#   the existing result without re-charging the card. This is essential because
#   Kafka guarantees at-least-once delivery — the same seat.reserved event
#   can arrive more than once after a consumer restart.
#
#   Additionally, Stripe PaymentIntents are created with an idempotency key
#   equal to the booking_id. This means even if we accidentally call Stripe
#   twice with the same booking_id (e.g. due to a race condition between the
#   DB check and the Stripe call), Stripe deduplicates the charge and returns
#   the same PaymentIntent result both times — no double charge.
#
# Mock mode:
#   When STRIPE_MOCK_ENABLED=true (default in .env for local development),
#   PaymentService skips the real Stripe SDK and returns synthetic success
#   or failure responses based on a set of test card numbers defined in
#   MOCK_CARD_OUTCOMES below. This allows the full event chain to be
#   exercised locally without a real Stripe account or network access.

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

import stripe
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.models import Payment
from app import producer
from shared.schemas import (
    SeatReservedEvent,
    PaymentProcessedEvent,
    PaymentFailedEvent,
    PaymentDetail,
    PaymentStatus,
    BookingFailedDLQEvent,
    serialize_event,
)


logger = logging.getLogger("payment-service.service")


# ──────────────────────────────────────────────────────────────────────────────
# STRIPE ERROR CLASSIFICATION
#
# Stripe errors fall into two categories:
#
#   Retryable — caused by transient infrastructure issues. The same charge
#   will likely succeed if attempted again after a short delay:
#     - rate_limit_error    → too many requests, back off and retry
#     - api_connection_error → network issue between us and Stripe
#     - api_error           → Stripe internal error (5xx)
#     - idempotency_error   → concurrent requests with same idempotency key
#
#   Permanent — caused by the card or the cardholder's situation. Retrying
#   will not change the outcome and wastes Stripe API quota:
#     - card_error          → card declined, insufficient funds, expired card
#     - authentication_error → invalid API key (configuration problem)
#     - invalid_request_error → bad parameters (programming error)
#
# The consumer uses this classification to decide whether to retry the
# message or route it to the DLQ immediately.
# ──────────────────────────────────────────────────────────────────────────────

RETRYABLE_STRIPE_ERRORS = {
    "rate_limit_error",
    "api_connection_error",
    "api_error",
    "idempotency_error",
}

PERMANENT_STRIPE_ERRORS = {
    "card_error",
    "authentication_error",
    "invalid_request_error",
}

# Stripe decline codes that should never be retried because the card
# is permanently blocked or the charge is fraudulent.
# See: https://stripe.com/docs/declines/codes
PERMANENT_DECLINE_CODES = {
    "card_not_supported",
    "card_velocity_exceeded",
    "currency_not_supported",
    "do_not_honor",
    "do_not_try_again",
    "fraudulent",
    "invalid_account",
    "invalid_amount",
    "lost_card",
    "merchant_blacklist",
    "no_action_taken",
    "not_permitted",
    "restricted_card",
    "revocation_of_all_authorizations",
    "revocation_of_authorization",
    "security_violation",
    "service_not_allowed",
    "stolen_card",
    "stop_payment_order",
    "transaction_not_allowed",
}


# ──────────────────────────────────────────────────────────────────────────────
# MOCK CARD OUTCOMES
# Used when STRIPE_MOCK_ENABLED=true. Maps a booking_id suffix pattern to
# a synthetic payment outcome so the full event chain can be tested locally
# without a real Stripe account.
#
# In tests you can control the outcome by setting the last 4 characters of
# the booking_id:
#   booking_id ending in "0000" → success
#   booking_id ending in "4242" → card declined (permanent)
#   booking_id ending in "9999" → rate limit error (retryable)
#   any other suffix            → success (default)
# ──────────────────────────────────────────────────────────────────────────────

MOCK_CARD_OUTCOMES = {
    "0000": {"status": "success"},
    "4242": {
        "status":         "failed",
        "failure_reason": "card_declined",
        "is_retryable":   False,
    },
    "9999": {
        "status":         "error",
        "failure_reason": "rate_limit_error",
        "is_retryable":   True,
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# PAYMENT SERVICE
# ──────────────────────────────────────────────────────────────────────────────

class PaymentService:
    """
    Stateless service class containing all payment domain logic.

    Configured once on __init__ — the Stripe API key is set globally
    on the stripe module (the SDK's recommended pattern) and the mock
    flag is read from settings. All public methods are async because
    they perform I/O (Stripe API calls, PostgreSQL reads and writes).

    Stateless design means the single instance created in consumer.py's
    run_consumer() is safely shared across all Kafka messages without
    risk of payment state leaking between messages.
    """

    def __init__(self) -> None:
        """
        Configure the Stripe SDK and set the mock mode flag.

        Stripe's Python SDK uses a module-level API key rather than a
        client instance. Setting stripe.api_key here affects all subsequent
        Stripe calls made anywhere in the process — there is no per-request
        credential configuration.

        In mock mode the API key is set to a test value but is never
        actually sent to Stripe — all calls are intercepted by the
        mock response methods.
        """
        if settings.STRIPE_MOCK_ENABLED:
            # Use a fake key in mock mode — the real Stripe API is never called
            stripe.api_key = "sk_test_mock_payment_service"
            logger.info(
                "PaymentService initialised in MOCK mode — "
                "no real Stripe API calls will be made"
            )
        else:
            # Use the real Stripe secret key from .env
            stripe.api_key = settings.STRIPE_API_KEY
            logger.info(
                "PaymentService initialised with real Stripe API key",
                extra={
                    # Log only the prefix to confirm the right key type
                    # without exposing the full secret
                    "key_prefix": settings.STRIPE_API_KEY[:12] + "...",
                },
            )

        self._mock_enabled: bool = settings.STRIPE_MOCK_ENABLED


    # ── HANDLE seat.reserved ──────────────────────────────────────────────────

    async def handle_seat_reserved(
        self,
        event: SeatReservedEvent,
        session: AsyncSession,
    ) -> dict[str, Any]:
        """
        Process a SeatReservedEvent — the main entry point for payments.

        Steps:
          1. Idempotency check — return existing result if already processed
          2. Call Stripe (or mock) to charge the passenger
          3. Write the payment transaction to PostgreSQL
          4. Publish PaymentProcessedEvent or PaymentFailedEvent to Kafka
          5. Return a result dict to consumer.py for metrics and logging

        Returns a dict with the following structure:
          On success:
            {
              "status":       "success",
              "payment_id":   str,       # UUID of the payment record
              "amount":       Decimal,   # amount charged
              "currency":     str,       # e.g. "USD"
              "event":        PaymentProcessedEvent,
            }
          On permanent failure (card declined):
            {
              "status":       "failed",
              "payment_id":   str,
              "amount":       Decimal,
              "currency":     str,
              "error_code":   str,       # Stripe error code
              "is_retryable": False,
              "event":        PaymentFailedEvent,
            }

        Raises:
          Exception: For retryable errors (network timeouts, Stripe 5xx).
                     consumer.py catches these and retries with backoff.

        Args:
            event:   The deserialized SeatReservedEvent from Kafka.
            session: An open AsyncSession — committed inside this method.
        """
        booking_id     = event.booking_id
        correlation_id = event.correlation_id

        logger.info(
            "Processing payment for seat.reserved event",
            extra={
                "booking_id":     booking_id,
                "correlation_id": correlation_id,
                "seat_number":    event.seat_number,
                "flight_id":      event.flight_id,
                "lock_ttl":       event.lock_ttl,
            },
        )

        # ── STEP 1: Idempotency check ─────────────────────────────────────────
        # Check whether a payment record already exists for this booking_id.
        # If it does, return the existing outcome without re-charging the card.
        # This handles the case where the consumer crashes after processing
        # but before committing the Kafka offset — the re-delivered message
        # should not result in a double charge.
        existing_payment = await self._get_existing_payment(booking_id, session)
        if existing_payment:
            logger.warning(
                "Payment already processed for this booking — returning cached result",
                extra={
                    "booking_id": booking_id,
                    "payment_id": existing_payment.payment_id,
                    "status":     existing_payment.status,
                },
            )
            return self._result_from_existing_payment(existing_payment, event)

        # ── STEP 2: Process payment ────────────────────────────────────────────
        # Call Stripe (real or mock) to charge the passenger.
        # Returns a result dict with status, payment_id, amount, and
        # optionally error_code and is_retryable on failure.
        payment_result = await self._process_stripe_payment(event)

        # ── STEP 3: Save payment record ───────────────────────────────────────
        # Write the payment transaction to PostgreSQL regardless of whether
        # the charge succeeded or failed. We need a record of every attempt
        # for audit trails, refund processing, and customer support.
        payment_id = str(uuid.uuid4())
        await self._save_payment_record(
            payment_id=payment_id,
            booking_id=booking_id,
            correlation_id=correlation_id,
            event=event,
            payment_result=payment_result,
            session=session,
        )

        # ── STEP 4: Publish downstream event ──────────────────────────────────
        if payment_result["status"] == "success":
            processed_event = self._build_processed_event(
                payment_id=payment_id,
                booking_id=booking_id,
                correlation_id=correlation_id,
                event=event,
                payment_result=payment_result,
            )
            await producer.publish_payment_processed(processed_event)
            payment_result["event"]      = processed_event
            payment_result["payment_id"] = payment_id
            return payment_result

        else:
            failed_event = self._build_failed_event(
                payment_id=payment_id,
                booking_id=booking_id,
                correlation_id=correlation_id,
                event=event,
                payment_result=payment_result,
            )
            await producer.publish_payment_failed(failed_event)
            payment_result["event"]      = failed_event
            payment_result["payment_id"] = payment_id
            return payment_result


    # ── STRIPE PAYMENT PROCESSING ─────────────────────────────────────────────

    async def _process_stripe_payment(
        self,
        event: SeatReservedEvent,
    ) -> dict[str, Any]:
        """
        Call Stripe (or mock) to charge the passenger.

        Returns a result dict — does not raise on card declines (permanent
        failures) so the caller can write a FAILED payment record to the DB.
        Does raise on transient Stripe errors (rate limits, network issues)
        so the consumer's retry logic can back off and try again.

        Args:
            event: The SeatReservedEvent containing booking and pricing details.

        Returns:
            On success:  {"status": "success", "transaction_ref": str, ...}
            On decline:  {"status": "failed",  "failure_reason": str,
                          "is_retryable": False, ...}

        Raises:
            stripe.error.RateLimitError:     Too many requests — retryable
            stripe.error.APIConnectionError: Network issue — retryable
            stripe.error.APIError:           Stripe 5xx — retryable
            Exception:                       Any other unexpected error
        """
        if self._mock_enabled:
            return await self._mock_stripe_payment(event)
        return await self._real_stripe_payment(event)


    async def _real_stripe_payment(
        self,
        event: SeatReservedEvent,
    ) -> dict[str, Any]:
        """
        Create and confirm a real Stripe PaymentIntent.

        Uses the booking_id as the Stripe idempotency key — if we call
        Stripe twice with the same booking_id (e.g. due to a race condition)
        Stripe returns the same PaymentIntent result both times, preventing
        a double charge.

        Stripe amounts are integers in the smallest currency unit
        (cents for USD, pence for GBP). We multiply by 100 and convert
        to int — e.g. $299.99 becomes 29999 cents.

        Args:
            event: The SeatReservedEvent with booking_id and pricing details.

        Returns:
            Result dict — see _process_stripe_payment() docstring.
        """
        booking_id = event.booking_id

        try:
            logger.info(
                "Creating Stripe PaymentIntent",
                extra={
                    "booking_id": booking_id,
                    "seat_class": event.seat_class.value,
                },
            )

            # Retrieve the total price and currency from the event.
            # SeatReservedEvent does not carry pricing — we look it up from
            # the booking record. For simplicity in this project, a mock
            # price is derived from the seat class. In production, the
            # BookingRequestedEvent's total_price would be stored in the
            # bookings table and fetched here.
            amount_decimal = self._get_price_for_seat_class(event.seat_class.value)
            amount_cents   = int(amount_decimal * 100)
            currency       = "usd"

            # Create and confirm the PaymentIntent in one API call.
            # confirm=True means Stripe immediately attempts the charge
            # rather than requiring a separate confirmation step.
            intent = stripe.PaymentIntent.create(
                amount=amount_cents,
                currency=currency,
                payment_method="pm_card_visa",  # test card in sandbox
                confirm=True,
                # Idempotency key prevents duplicate charges on retry.
                # Stripe deduplicates requests with the same key within 24h.
                idempotency_key=booking_id,
                metadata={
                    # Stored on the Stripe dashboard for support lookups
                    "booking_id":     booking_id,
                    "flight_id":      event.flight_id,
                    "seat_number":    event.seat_number,
                    "seat_class":     event.seat_class.value,
                    "correlation_id": event.correlation_id,
                },
                # Automatic payment methods disabled — we specify the
                # payment method explicitly for predictable test behaviour
                automatic_payment_methods={"enabled": False},
            )

            # PaymentIntent status after confirm=True:
            #   "succeeded"       → charge successful, funds captured
            #   "requires_action" → 3D Secure authentication needed
            #   "requires_payment_method" → card declined
            if intent.status == "succeeded":
                logger.info(
                    "Stripe PaymentIntent succeeded",
                    extra={
                        "booking_id":    booking_id,
                        "intent_id":     intent.id,
                        "amount_cents":  amount_cents,
                        "currency":      currency,
                    },
                )
                return {
                    "status":          "success",
                    "transaction_ref": intent.id,
                    "amount":          amount_decimal,
                    "currency":        currency.upper(),
                    "payment_method":  "stripe",
                    "is_retryable":    False,
                }

            elif intent.status == "requires_action":
                # 3D Secure required — cannot complete without user interaction.
                # Treat as a permanent failure in this async flow because there
                # is no mechanism to redirect the user for 3DS authentication
                # from inside a Kafka consumer. A production system would
                # send the user a payment link instead.
                logger.warning(
                    "Stripe PaymentIntent requires 3D Secure action — "
                    "cannot complete in async consumer",
                    extra={
                        "booking_id": booking_id,
                        "intent_id":  intent.id,
                    },
                )
                return {
                    "status":         "failed",
                    "failure_reason": "requires_3d_secure_authentication",
                    "transaction_ref": intent.id,
                    "amount":         amount_decimal,
                    "currency":       currency.upper(),
                    "payment_method": "stripe",
                    "is_retryable":   False,
                    "error_code":     "requires_action",
                }

            else:
                # Unexpected status — treat as permanent failure
                return {
                    "status":         "failed",
                    "failure_reason": f"unexpected_intent_status:{intent.status}",
                    "transaction_ref": intent.id,
                    "amount":         amount_decimal,
                    "currency":       currency.upper(),
                    "payment_method": "stripe",
                    "is_retryable":   False,
                    "error_code":     intent.status,
                }

        except stripe.error.CardError as exc:
            # Card declined, insufficient funds, expired card, etc.
            # These are permanent failures — retrying the same card will
            # not change the outcome.
            decline_code = exc.decline_code or exc.code or "card_error"
            logger.warning(
                "Stripe card error — permanent failure",
                extra={
                    "booking_id":   booking_id,
                    "decline_code": decline_code,
                    "message":      exc.user_message,
                },
            )
            return {
                "status":         "failed",
                "failure_reason": decline_code,
                "amount":         self._get_price_for_seat_class(
                                      event.seat_class.value
                                  ),
                "currency":       "USD",
                "payment_method": "stripe",
                "is_retryable":   False,
                "error_code":     decline_code,
            }

        except stripe.error.RateLimitError as exc:
            # Too many requests — back off and retry
            logger.warning(
                "Stripe rate limit hit — will retry",
                extra={"booking_id": booking_id, "error": str(exc)},
            )
            raise   # Let consumer.py handle retry

        except stripe.error.APIConnectionError as exc:
            # Network issue between Payment Service and Stripe
            logger.warning(
                "Stripe API connection error — will retry",
                extra={"booking_id": booking_id, "error": str(exc)},
            )
            raise

        except stripe.error.APIError as exc:
            # Stripe internal error (5xx) — transient, retry
            logger.warning(
                "Stripe API error (5xx) — will retry",
                extra={"booking_id": booking_id, "error": str(exc)},
            )
            raise

        except stripe.error.AuthenticationError as exc:
            # Invalid API key — this is a configuration error, not a
            # transient failure. Log at CRITICAL and raise so the process
            # exits and Docker restarts with a clear error message.
            logger.critical(
                "Stripe authentication failed — check STRIPE_API_KEY in .env",
                extra={"booking_id": booking_id, "error": str(exc)},
            )
            raise

        except stripe.error.InvalidRequestError as exc:
            # Bad parameters — programming error. Log and treat as permanent.
            logger.error(
                "Stripe invalid request — check PaymentIntent parameters",
                extra={"booking_id": booking_id, "error": str(exc)},
            )
            return {
                "status":         "failed",
                "failure_reason": f"invalid_request: {exc.user_message}",
                "amount":         self._get_price_for_seat_class(
                                      event.seat_class.value
                                  ),
                "currency":       "USD",
                "payment_method": "stripe",
                "is_retryable":   False,
                "error_code":     "invalid_request_error",
            }


    async def _mock_stripe_payment(
        self,
        event: SeatReservedEvent,
    ) -> dict[str, Any]:
        """
        Return a synthetic Stripe payment result without calling the real API.

        Used when STRIPE_MOCK_ENABLED=true (local development default).
        The outcome is determined by the last 4 characters of the booking_id
        as defined in MOCK_CARD_OUTCOMES above.

        Simulates a 100ms network delay so the consumer loop timing
        resembles real Stripe API behaviour — prevents tests from passing
        only because the mock is unrealistically fast.

        Args:
            event: The SeatReservedEvent — booking_id suffix determines outcome.

        Returns:
            Result dict matching the structure of _real_stripe_payment().
        """
        import asyncio
        booking_id  = event.booking_id
        suffix      = booking_id[-4:] if len(booking_id) >= 4 else "0000"
        outcome     = MOCK_CARD_OUTCOMES.get(suffix, {"status": "success"})

        # Simulate Stripe API network latency (100ms)
        await asyncio.sleep(0.1)

        amount   = self._get_price_for_seat_class(event.seat_class.value)
        currency = "USD"

        if outcome["status"] == "success":
            mock_transaction_ref = f"mock_pi_{uuid.uuid4().hex[:24]}"
            logger.info(
                "Mock Stripe payment succeeded",
                extra={
                    "booking_id":      booking_id,
                    "transaction_ref": mock_transaction_ref,
                    "amount":          str(amount),
                },
            )
            return {
                "status":          "success",
                "transaction_ref": mock_transaction_ref,
                "amount":          amount,
                "currency":        currency,
                "payment_method":  "stripe_mock",
                "is_retryable":    False,
            }

        elif outcome["status"] == "failed":
            logger.warning(
                "Mock Stripe payment declined",
                extra={
                    "booking_id":   booking_id,
                    "decline_code": outcome["failure_reason"],
                },
            )
            return {
                "status":         "failed",
                "failure_reason": outcome["failure_reason"],
                "amount":         amount,
                "currency":       currency,
                "payment_method": "stripe_mock",
                "is_retryable":   outcome.get("is_retryable", False),
                "error_code":     outcome["failure_reason"],
            }

        elif outcome["status"] == "error":
            # Simulate a retryable error — raise so consumer.py retries
            logger.warning(
                "Mock Stripe transient error — consumer will retry",
                extra={
                    "booking_id":   booking_id,
                    "error_type":   outcome["failure_reason"],
                },
            )
            raise Exception(
                f"Mock Stripe error: {outcome['failure_reason']} "
                f"(booking_id={booking_id})"
            )

        # Default fallback — should not be reached
        return {
            "status":          "success",
            "transaction_ref": f"mock_pi_{uuid.uuid4().hex[:24]}",
            "amount":          amount,
            "currency":        currency,
            "payment_method":  "stripe_mock",
            "is_retryable":    False,
        }


    # ── DATABASE OPERATIONS ───────────────────────────────────────────────────

    async def _save_payment_record(
        self,
        payment_id: str,
        booking_id: str,
        correlation_id: str,
        event: SeatReservedEvent,
        payment_result: dict[str, Any],
        session: AsyncSession,
    ) -> None:
        """
        Write a payment transaction record to PostgreSQL.

        Called after every Stripe attempt — both successes and failures
        are recorded for audit trails and customer support lookups.

        On IntegrityError (duplicate payment_id — should not happen but
        handled defensively) we roll back and log a warning rather than
        raising, because the payment has already been processed by Stripe
        and we do not want to trigger a consumer retry that might attempt
        a double charge.

        Args:
            payment_id:     UUID for the new payment record.
            booking_id:     The booking this payment belongs to.
            correlation_id: Trace ID for log correlation.
            event:          The SeatReservedEvent — used for seat/flight details.
            payment_result: The dict returned by _process_stripe_payment().
            session:        An open AsyncSession — committed inside this method.
        """
        amount   = payment_result.get("amount", Decimal("0.00"))
        currency = payment_result.get("currency", "USD")
        status   = payment_result.get("status", "failed")

        payment = Payment(
            payment_id=payment_id,
            booking_id=booking_id,
            correlation_id=correlation_id,
            # Seat and flight details from the event
            flight_id=event.flight_id,
            seat_number=event.seat_number,
            seat_class=event.seat_class.value,
            # Payment outcome from Stripe
            amount=amount,
            currency=currency,
            payment_method=payment_result.get("payment_method", "stripe"),
            transaction_ref=payment_result.get("transaction_ref"),
            status=status,
            failure_reason=payment_result.get("failure_reason"),
            stripe_error_code=payment_result.get("error_code"),
            # Timestamps
            attempted_at=datetime.utcnow(),
            completed_at=datetime.utcnow() if status == "success" else None,
        )

        try:
            session.add(payment)
            await session.commit()

            logger.info(
                "Payment record saved to PostgreSQL",
                extra={
                    "payment_id": payment_id,
                    "booking_id": booking_id,
                    "status":     status,
                    "amount":     str(amount),
                },
            )

        except IntegrityError:
            # Duplicate payment_id — race condition between idempotency
            # check and insert. Roll back and log — the existing record
            # is the source of truth.
            await session.rollback()
            logger.warning(
                "Duplicate payment_id on insert — rolling back",
                extra={
                    "payment_id": payment_id,
                    "booking_id": booking_id,
                },
            )


    # ── EVENT BUILDERS ────────────────────────────────────────────────────────

    def _build_processed_event(
        self,
        payment_id: str,
        booking_id: str,
        correlation_id: str,
        event: SeatReservedEvent,
        payment_result: dict[str, Any],
    ) -> PaymentProcessedEvent:
        """
        Construct a PaymentProcessedEvent from the Stripe result.

        The event carries full PassengerDetail and FlightInfo so downstream
        consumers (Booking Service, Notification Service) do not need to
        query the database to build their own views of the booking.

        Note: SeatReservedEvent does not embed PassengerDetail or FlightInfo
        directly (it only has flight_id and seat_number). In a production
        system the Payment Service would fetch the passenger details from the
        bookings table here. For this project, placeholder values are used
        to keep the service self-contained.

        Args:
            payment_id:     UUID of the saved payment record.
            booking_id:     The booking being confirmed.
            correlation_id: Trace ID.
            event:          The original SeatReservedEvent.
            payment_result: The Stripe result dict.

        Returns:
            A fully populated PaymentProcessedEvent ready to publish.
        """
        from shared.schemas import (
            PassengerDetail,
            FlightInfo,
            SeatClass,
        )
        from datetime import timedelta

        # In production: fetch passenger and flight details from the
        # bookings table using booking_id. Placeholder values used here.
        passenger = PassengerDetail(
            passenger_id=str(uuid.uuid4()),
            first_name="Passenger",
            last_name="Name",
            email="passenger@example.com",
        )

        flight = FlightInfo(
            flight_id=event.flight_id,
            flight_number="XX000",
            origin="JFK",
            destination="LAX",
            departure_time=datetime.utcnow() + timedelta(days=7),
            arrival_time=datetime.utcnow() + timedelta(days=7, hours=6),
            seat_number=event.seat_number,
            seat_class=event.seat_class,
        )

        payment_detail = PaymentDetail(
            payment_id=payment_id,
            amount=payment_result["amount"],
            currency=payment_result["currency"],
            payment_method=payment_result["payment_method"],
            transaction_ref=payment_result.get("transaction_ref"),
        )

        return PaymentProcessedEvent(
            correlation_id=correlation_id,
            booking_id=booking_id,
            passenger=passenger,
            flight=flight,
            payment=payment_detail,
            status=PaymentStatus.SUCCESS,
        )


    def _build_failed_event(
        self,
        payment_id: str,
        booking_id: str,
        correlation_id: str,
        event: SeatReservedEvent,
        payment_result: dict[str, Any],
        retry_attempt: int = 0,
    ) -> PaymentFailedEvent:
        """
        Construct a PaymentFailedEvent from the Stripe failure result.

        The retry_attempt counter tells Booking Service and Seat Service
        how many times the payment was attempted before giving up. A
        retry_attempt of 0 means the card was declined on the first try
        (permanent failure) vs retry_attempt of 3 which means three
        transient errors exhausted the retry budget.

        Args:
            payment_id:     UUID of the saved payment record.
            booking_id:     The booking that failed.
            correlation_id: Trace ID.
            event:          The original SeatReservedEvent.
            payment_result: The Stripe failure result dict.
            retry_attempt:  How many processing attempts were made.

        Returns:
            A fully populated PaymentFailedEvent ready to publish.
        """
        from shared.schemas import (
            PassengerDetail,
            FlightInfo,
        )
        from datetime import timedelta

        # Placeholder passenger and flight — same note as _build_processed_event
        passenger = PassengerDetail(
            passenger_id=str(uuid.uuid4()),
            first_name="Passenger",
            last_name="Name",
            email="passenger@example.com",
        )

        flight = FlightInfo(
            flight_id=event.flight_id,
            flight_number="XX000",
            origin="JFK",
            destination="LAX",
            departure_time=datetime.utcnow() + timedelta(days=7),
            arrival_time=datetime.utcnow() + timedelta(days=7, hours=6),
            seat_number=event.seat_number,
            seat_class=event.seat_class,
        )

        payment_detail = PaymentDetail(
            payment_id=payment_id,
            amount=payment_result.get("amount", Decimal("0.00")),
            currency=payment_result.get("currency", "USD"),
            payment_method=payment_result.get("payment_method", "stripe"),
            transaction_ref=payment_result.get("transaction_ref"),
            failure_reason=payment_result.get("failure_reason", "unknown"),
        )

        return PaymentFailedEvent(
            correlation_id=correlation_id,
            booking_id=booking_id,
            passenger=passenger,
            flight=flight,
            payment=payment_detail,
            status=PaymentStatus.FAILED,
            retry_attempt=retry_attempt,
        )


    # ── PRIVATE HELPERS ───────────────────────────────────────────────────────

    async def _get_existing_payment(
        self,
        booking_id: str,
        session: AsyncSession,
    ) -> Payment | None:
        """
        Fetch an existing payment record for a booking_id.

        Used by handle_seat_reserved() for the idempotency check at the
        top of every invocation. Returns None if no payment has been
        attempted yet for this booking_id.

        Args:
            booking_id: The booking to look up.
            session:    The current AsyncSession.

        Returns:
            The Payment ORM instance if found, None otherwise.
        """
        result = await session.execute(
            select(Payment).where(Payment.booking_id == booking_id)
        )
        return result.scalar_one_or_none()


    def _result_from_existing_payment(
        self,
        payment: Payment,
        event: SeatReservedEvent,
    ) -> dict[str, Any]:
        """
        Build a result dict from an existing Payment ORM object.

        Called when the idempotency check finds an existing payment record.
        Returns the same shape dict as _process_stripe_payment() so
        consumer.py can handle the idempotent case without special casing.

        Args:
            payment: The existing Payment ORM object from PostgreSQL.
            event:   The original SeatReservedEvent (needed for context).

        Returns:
            Result dict with the original payment outcome.
        """
        return {
            "status":          payment.status,
            "payment_id":      payment.payment_id,
            "amount":          payment.amount,
            "currency":        payment.currency,
            "payment_method":  payment.payment_method,
            "transaction_ref": payment.transaction_ref,
            "failure_reason":  payment.failure_reason,
            "error_code":      payment.stripe_error_code,
            "is_retryable":    False,
        }


    @staticmethod
    def _get_price_for_seat_class(seat_class: str) -> Decimal:
        """
        Return a fixed price for a given seat class.

        In production this would query a dynamic pricing service or the
        fare stored in the bookings table. For this project, fixed prices
        per cabin class are used to keep the service self-contained.

        Args:
            seat_class: One of "economy", "premium_economy", "business", "first"

        Returns:
            The price as a Decimal with 2 decimal places.
        """
        prices = {
            "economy":         Decimal("299.99"),
            "premium_economy": Decimal("599.99"),
            "business":        Decimal("1299.99"),
            "first":           Decimal("2499.99"),
        }
        # Default to economy price if seat class is unrecognised
        return prices.get(seat_class, Decimal("299.99"))