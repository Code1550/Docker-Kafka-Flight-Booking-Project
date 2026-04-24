# payment-service/app/db/models.py
#
# SQLAlchemy ORM model for the Payment Service.
#
# Defines one table:
#   - payments → written by the Payment Service for every charge attempt,
#                both successful and failed. Read by the idempotency check
#                in service.py before every Stripe call.
#
# Design principles:
#
#   Single table, single owner:
#     The Payment Service is the only service that writes to the payments
#     table. No other service has INSERT or UPDATE access. This is enforced
#     by convention (each service owns its own schema) and should be enforced
#     by PostgreSQL role permissions in production.
#
#   Record every attempt, not just successes:
#     A payment record is written for every Stripe call — success, decline,
#     and transient error. This gives a complete audit trail for customer
#     support ("why was my card charged twice?"), finance reconciliation,
#     and fraud analysis. A payments table with only successful rows is
#     incomplete and hard to debug.
#
#   No foreign key to bookings table:
#     The payments and bookings tables live in the same PostgreSQL database
#     but are owned by different services. Adding a FK constraint would
#     create a hard schema dependency between two independently deployable
#     services — a booking would have to be written before a payment could
#     be inserted, which breaks the event-driven model where services process
#     events concurrently. Referential integrity is enforced at the application
#     level in service.py (Booking Service verifies the flight exists before
#     inserting a booking record).
#
#   Numeric not Float for money:
#     All monetary amounts use Numeric(10, 2) — exact decimal arithmetic.
#     Float is a binary floating-point type that cannot represent values
#     like 0.10 exactly, causing rounding errors in financial calculations.
#     Python's Decimal type maps directly to PostgreSQL's NUMERIC type
#     through SQLAlchemy and preserves exactness end-to-end.
#
#   UTC timestamps set by the application:
#     All timestamps are set by the Payment Service in UTC rather than by
#     PostgreSQL DEFAULT NOW(). Application-set timestamps reflect when the
#     event was processed, not when the row was written — the two can differ
#     under retry conditions where a message is processed, the DB write fails,
#     and the message is re-processed several seconds later.

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# ──────────────────────────────────────────────────────────────────────────────
# DECLARATIVE BASE
# All ORM models in the Payment Service inherit from this Base.
# Alembic reads Base.metadata to detect schema changes and generate
# migration scripts via `alembic revision --autogenerate`.
#
# A separate Base class per service (rather than a shared one) means
# each service's Alembic environment only manages its own tables —
# the Payment Service cannot accidentally generate a migration that
# modifies the bookings table owned by the Booking Service.
# ──────────────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    """
    Base class for all ORM models in the Payment Service.

    Import this in payment-service/alembic/env.py:
        from app.db.models import Base
        target_metadata = Base.metadata
    """
    pass


# ──────────────────────────────────────────────────────────────────────────────
# PAYMENT MODEL
# One row per Stripe charge attempt. Written immediately after every Stripe
# call — before the downstream Kafka event is published — to ensure a record
# exists even if the Kafka publish subsequently fails.
#
# Status lifecycle:
#
#   "pending"   → row created before Stripe call (not used in current impl,
#                 reserved for future two-phase payment flows)
#   "success"   → Stripe PaymentIntent status = "succeeded"
#   "failed"    → card declined or permanent Stripe error
#   "refunded"  → charge reversed after confirmation (future feature)
#   "disputed"  → chargeback filed by passenger (future feature)
#
# The idempotency check in service.py queries this table by booking_id
# before every Stripe call. If a row exists the cached result is returned
# without re-charging the card — the core double-charge prevention mechanism.
# ──────────────────────────────────────────────────────────────────────────────

class Payment(Base):
    """
    Represents a single Stripe payment attempt for a flight booking.

    Written by PaymentService._save_payment_record() after every Stripe
    call. Read by PaymentService._get_existing_payment() for idempotency
    checks before every subsequent Stripe call on the same booking_id.
    """

    __tablename__ = "payments"


    # ── PRIMARY KEY ───────────────────────────────────────────────────────────

    payment_id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        comment=(
            "UUID generated by the Payment Service for this charge attempt. "
            "Distinct from the Stripe PaymentIntent ID (stored in transaction_ref) "
            "so the internal record is decoupled from the Stripe object model."
        ),
    )


    # ── BOOKING REFERENCE ─────────────────────────────────────────────────────

    booking_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        comment=(
            "UUID of the booking this payment belongs to. "
            "Not a foreign key — see module docstring for rationale. "
            "Used as the Stripe idempotency key and the Kafka message key "
            "so all events for the same booking land on the same partition."
        ),
    )

    correlation_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        comment=(
            "Trace ID shared across all services for this booking request. "
            "Join logs on this column to reconstruct the full event journey "
            "from API Gateway through Booking, Seat, Payment, and Notification."
        ),
    )


    # ── FLIGHT AND SEAT REFERENCE ─────────────────────────────────────────────
    # Denormalised from the SeatReservedEvent payload. Stored here so a
    # payment record is self-contained for customer support lookups —
    # "show me all payments for flight AA123 on seat 12A" without joining
    # the bookings table owned by a different service.

    flight_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        comment="UUID of the flight that was booked — from SeatReservedEvent",
    )

    seat_number: Mapped[str] = mapped_column(
        String(4),
        nullable=False,
        comment="Seat identifier e.g. 12A — from SeatReservedEvent",
    )

    seat_class: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="Cabin class: economy | premium_economy | business | first",
    )


    # ── PAYMENT AMOUNTS ───────────────────────────────────────────────────────

    amount: Mapped[Decimal] = mapped_column(
        # Numeric(10, 2) stores up to 99,999,999.99 with exact decimal precision.
        # Never use Float for monetary amounts — binary floating-point cannot
        # represent values like 0.10 exactly, causing rounding errors that
        # compound over many transactions and cause reconciliation failures.
        Numeric(10, 2),
        nullable=False,
        comment=(
            "Amount charged in the specified currency. "
            "Stored as exact decimal — matches the amount sent to Stripe in cents "
            "divided by 100. e.g. Stripe amount=29999 → amount=299.99"
        ),
    )

    currency: Mapped[str] = mapped_column(
        String(3),
        nullable=False,
        default="USD",
        comment="ISO 4217 currency code e.g. USD, EUR, GBP",
    )

    # Amount actually captured by Stripe — may differ from amount on refunds
    # or partial captures. NULL until Stripe confirms the capture.
    captured_amount: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 2),
        nullable=True,
        comment=(
            "Amount actually captured by Stripe. Normally equals amount. "
            "Differs on partial refunds: original amount=299.99, "
            "captured_amount=149.99 after a 50% refund."
        ),
    )


    # ── PAYMENT METHOD ────────────────────────────────────────────────────────

    payment_method: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment=(
            "Payment method used for this charge. "
            "Examples: 'stripe', 'stripe_mock', 'paypal' (future). "
            "Used to filter payments by method in finance reports."
        ),
    )

    # Stripe card brand and last 4 digits — stored for customer support
    # ("which card was charged?") without storing the full card number
    # (PCI DSS compliance requirement).
    card_brand: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        comment=(
            "Card brand returned by Stripe e.g. 'visa', 'mastercard', 'amex'. "
            "NULL for non-card payment methods or mock payments."
        ),
    )

    card_last4: Mapped[str | None] = mapped_column(
        String(4),
        nullable=True,
        comment=(
            "Last 4 digits of the card number returned by Stripe. "
            "Stored for customer support reference — never store full card numbers. "
            "NULL for non-card payment methods or mock payments."
        ),
    )


    # ── STRIPE REFERENCE ──────────────────────────────────────────────────────

    transaction_ref: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment=(
            "Stripe PaymentIntent ID e.g. 'pi_3OqrV2LkdIwHu7ix1234abcd'. "
            "Used to look up the charge in the Stripe dashboard for refunds, "
            "disputes, and customer support. NULL for mock payments and "
            "payments that failed before a PaymentIntent was created."
        ),
    )

    stripe_error_code: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment=(
            "Stripe error code on failure e.g. 'card_declined', "
            "'insufficient_funds', 'rate_limit_error'. "
            "NULL on success. Used to categorise failure reasons in "
            "Grafana dashboards and customer support workflows."
        ),
    )

    stripe_decline_code: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment=(
            "Stripe decline code — more specific than stripe_error_code. "
            "e.g. 'insufficient_funds', 'stolen_card', 'do_not_honor'. "
            "NULL on success or non-card errors. Used for fraud analysis."
        ),
    )


    # ── STATUS ────────────────────────────────────────────────────────────────

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="pending",
        comment=(
            "Current status of this payment attempt. "
            "Lifecycle: pending → success | failed → refunded | disputed"
        ),
    )

    # Human-readable failure description — populated when status=failed.
    # Stored separately from stripe_error_code so non-Stripe failures
    # (e.g. "DB write failed after successful charge") can also be recorded.
    failure_reason: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment=(
            "Human-readable failure reason populated when status='failed'. "
            "Examples: 'card_declined', 'insufficient_funds', "
            "'requires_3d_secure_authentication', "
            "'Payment processing failed after 3 attempts: network timeout'. "
            "Used in customer support tickets and failure analysis."
        ),
    )

    # Number of Stripe API calls made for this booking_id before success
    # or permanent failure. Helps identify bookings that required retries
    # for SLA analysis and Stripe cost optimisation.
    retry_count: Mapped[int] = mapped_column(
        # Using String not Integer for retry_count to avoid a default=0
        # ambiguity with SQLAlchemy's nullable detection. Stored and read
        # as int in application code via the @property below.
        # Actually store as int directly — simpler and correct.
        nullable=False,
        default=0,
        comment=(
            "Number of Stripe API calls made for this booking_id. "
            "0 = succeeded or failed on first attempt. "
            "Used to identify bookings that consumed multiple retry attempts."
        ),
    )


    # ── REFUND TRACKING ───────────────────────────────────────────────────────
    # Populated when status transitions to "refunded" (future feature).

    refund_amount: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 2),
        nullable=True,
        comment=(
            "Amount refunded. May be less than amount for partial refunds. "
            "NULL until a refund is issued. Full refund: refund_amount == amount."
        ),
    )

    refund_ref: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment=(
            "Stripe Refund ID e.g. 're_3OqrV2LkdIwHu7ix1234abcd'. "
            "Used to look up the refund in the Stripe dashboard. "
            "NULL until a refund is issued."
        ),
    )

    refund_reason: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment=(
            "Reason for the refund e.g. 'flight_cancelled', 'passenger_request', "
            "'duplicate_charge', 'fraud'. NULL until a refund is issued. "
            "Stored for finance reporting and Stripe dispute evidence."
        ),
    )


    # ── AUDIT TIMESTAMPS ──────────────────────────────────────────────────────
    # All set by the Payment Service in UTC.
    # See module docstring for why application-set timestamps are preferred
    # over PostgreSQL DEFAULT NOW() in an event-driven retry system.

    attempted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        comment=(
            "UTC time the Stripe API call was initiated. "
            "Set before the Stripe call so the timestamp reflects when the "
            "charge was attempted, not when the DB row was written."
        ),
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=False),
        nullable=True,
        comment=(
            "UTC time the Stripe charge was confirmed as succeeded. "
            "NULL for failed or pending payments. "
            "completed_at - attempted_at = Stripe API response latency."
        ),
    )

    refunded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=False),
        nullable=True,
        comment="UTC time the refund was issued. NULL until refunded.",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        default=datetime.utcnow,
        comment=(
            "UTC time the payment row was inserted into PostgreSQL. "
            "Normally very close to attempted_at — differs only when "
            "the DB write was delayed by connection pool contention."
        ),
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        comment=(
            "UTC time this row was last modified. "
            "Updated when status transitions to refunded or disputed."
        ),
    )


    # ── TABLE ARGS ────────────────────────────────────────────────────────────

    __table_args__ = (

        # ── UNIQUENESS CONSTRAINTS ────────────────────────────────────────────

        # One payment record per booking_id — the idempotency check in
        # service.py relies on this constraint to detect re-delivery.
        # If two consumers race past the SELECT check simultaneously,
        # the second INSERT raises IntegrityError which service.py catches
        # and handles by returning the first record's result.
        UniqueConstraint(
            "booking_id",
            name="uq_payments_booking_id",
        ),

        # transaction_ref (Stripe PaymentIntent ID) must be unique when set.
        # Prevents two payment records from referencing the same Stripe charge,
        # which would indicate a logic error in the idempotency flow.
        # NULLS are excluded from unique constraints in PostgreSQL so NULL
        # transaction_ref (failed payments before PaymentIntent creation)
        # does not violate this constraint.
        UniqueConstraint(
            "transaction_ref",
            name="uq_payments_transaction_ref",
        ),


        # ── CHECK CONSTRAINTS ─────────────────────────────────────────────────

        # Status must be one of the known lifecycle values.
        # Rejects any INSERT or UPDATE that sets an invalid status string —
        # catches typos and logic errors in service.py before they reach the DB.
        CheckConstraint(
            "status IN ('pending', 'success', 'failed', 'refunded', 'disputed')",
            name="ck_payments_status_valid",
        ),

        # Payment amount must be positive — a zero or negative amount
        # indicates a data error (free tickets use a promo_code flow).
        CheckConstraint(
            "amount > 0",
            name="ck_payments_amount_positive",
        ),

        # Captured amount must not exceed the original charged amount.
        # Prevents data errors where a refund reversal is recorded as larger
        # than the original charge.
        CheckConstraint(
            "captured_amount IS NULL OR captured_amount <= amount",
            name="ck_payments_captured_lte_amount",
        ),

        # Refund amount must not exceed the original amount.
        # A refund_amount > amount would mean more was refunded than charged —
        # impossible in Stripe but worth enforcing at the DB level for safety.
        CheckConstraint(
            "refund_amount IS NULL OR refund_amount <= amount",
            name="ck_payments_refund_lte_amount",
        ),

        # Refunded payments must have a refund_ref.
        # If status='refunded' the Stripe Refund ID must be recorded so the
        # refund can be looked up on the Stripe dashboard for verification.
        CheckConstraint(
            "status != 'refunded' OR refund_ref IS NOT NULL",
            name="ck_payments_refunded_has_ref",
        ),

        # completed_at must be after attempted_at when both are set.
        # A completion timestamp before the attempt timestamp indicates
        # a clock skew issue or a data entry error.
        CheckConstraint(
            "completed_at IS NULL OR completed_at >= attempted_at",
            name="ck_payments_completed_after_attempted",
        ),

        # retry_count must be non-negative.
        CheckConstraint(
            "retry_count >= 0",
            name="ck_payments_retry_count_non_negative",
        ),


        # ── INDEXES ───────────────────────────────────────────────────────────

        # Primary idempotency lookup — used by service.py before every
        # Stripe call. booking_id is already covered by the unique constraint
        # (uq_payments_booking_id) which creates an implicit B-tree index.
        # No additional index needed — the unique constraint suffices.

        # Used by the Grafana payment dashboard query:
        # "show all payments by status in the last 24 hours"
        Index(
            "ix_payments_status_attempted_at",
            "status",
            "attempted_at",
        ),

        # Used by customer support lookup:
        # "find the payment for booking X"
        Index("ix_payments_booking_id", "booking_id"),

        # Used by distributed tracing queries:
        # "find all DB records for correlation_id X"
        Index("ix_payments_correlation_id", "correlation_id"),

        # Used by Stripe reconciliation jobs:
        # "find all payments for flight X"
        Index("ix_payments_flight_id", "flight_id"),

        # Used by finance reporting:
        # "total revenue by currency for the month"
        Index(
            "ix_payments_currency_status",
            "currency",
            "status",
        ),

        # Used by fraud analysis queries:
        # "find all failed payments with stripe_error_code=stolen_card"
        Index(
            "ix_payments_stripe_error_code",
            "stripe_error_code",
        ),

        # Partial index on successful payments only —
        # much smaller than a full index on status because most queries
        # that care about transaction_ref are looking at successful charges.
        # PostgreSQL partial indexes significantly reduce index size and
        # improve write performance when most rows do not match the condition.
        Index(
            "ix_payments_transaction_ref_success",
            "transaction_ref",
            postgresql_where="status = 'success'",
        ),

        {"comment": "Payment transaction records — written by Payment Service"},
    )


    # ── DUNDER METHODS ────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"<Payment "
            f"payment_id={self.payment_id!r} "
            f"booking_id={self.booking_id!r} "
            f"amount={self.amount!r} "
            f"currency={self.currency!r} "
            f"status={self.status!r}"
            f">"
        )


    # ── CONVENIENCE PROPERTIES ────────────────────────────────────────────────

    @property
    def is_successful(self) -> bool:
        """
        True if this payment attempt resulted in a successful charge.
        Used in service.py to decide which downstream Kafka event to publish.
        """
        return self.status == "success"

    @property
    def is_refunded(self) -> bool:
        """
        True if this payment has been fully or partially refunded.
        Used by the refund processing flow (future feature) to prevent
        issuing a second refund on an already-refunded payment.
        """
        return self.status == "refunded"

    @property
    def is_terminal(self) -> bool:
        """
        True if this payment has reached a state from which no further
        Stripe calls will be made. Terminal states are: success, failed,
        refunded, disputed.

        Used in service.py to short-circuit idempotent re-processing:
        if the existing payment is terminal, return its result immediately
        without re-evaluating the Stripe outcome.
        """
        return self.status in ("success", "failed", "refunded", "disputed")

    @property
    def stripe_latency_ms(self) -> int | None:
        """
        Return the Stripe API response latency in milliseconds.

        Calculated as the difference between completed_at and attempted_at.
        Returns None if either timestamp is absent (failed payments where
        Stripe was never reached, or pending payments).

        Used in Grafana to track p50/p95/p99 Stripe API latency over time
        and alert when Stripe response times degrade.
        """
        if self.attempted_at and self.completed_at:
            delta = self.completed_at - self.attempted_at
            return int(delta.total_seconds() * 1000)
        return None

    @property
    def net_amount(self) -> Decimal:
        """
        Return the net amount after any refunds.

        net_amount = amount - refund_amount (or just amount if no refund).
        Used in finance reporting to calculate actual revenue collected
        from a booking after partial or full refunds are applied.
        """
        if self.refund_amount:
            return self.amount - self.refund_amount
        return self.amount