# shared/schemas.py
#
# Pydantic models for all Kafka event payloads shared across microservices.
#
# Every message published to a Kafka topic must be serialized to JSON using
# one of these models. Every consumer must deserialize the raw JSON back into
# one of these models before processing. This guarantees that all services
# speak the same schema and that breaking changes are caught at parse time
# rather than buried in downstream logic.
#
# Naming convention:
#   - Events that describe something that happened  → <Noun><PastTense>Event
#     e.g. BookingRequestedEvent, PaymentProcessedEvent
#   - Shared sub-models reused inside events        → <Noun>Detail / <Noun>Info
#     e.g. PassengerDetail, FlightInfo

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ──────────────────────────────────────────────────────────────────────────────
# ENUMS
# Used as controlled vocabularies inside event models. Defining them here
# prevents magic strings from spreading across services.
# ──────────────────────────────────────────────────────────────────────────────

class BookingStatus(str, Enum):
    """Lifecycle states of a booking, in order of progression."""
    REQUESTED   = "requested"    # Initial event fired by API Gateway
    SEAT_HELD   = "seat_held"    # Seat lock acquired in Redis
    PAYMENT_PENDING = "payment_pending"  # Handed off to Payment Service
    CONFIRMED   = "confirmed"    # Payment succeeded, booking complete
    FAILED      = "failed"       # Unrecoverable failure, sent to DLQ
    CANCELLED   = "cancelled"    # Cancelled by user after confirmation


class PaymentStatus(str, Enum):
    """Possible outcomes from the Payment Service."""
    SUCCESS  = "success"
    FAILED   = "failed"
    REFUNDED = "refunded"


class SeatClass(str, Enum):
    """Cabin class options. Maps directly to fare pricing tiers."""
    ECONOMY        = "economy"
    PREMIUM_ECONOMY = "premium_economy"
    BUSINESS       = "business"
    FIRST          = "first"


class NotificationType(str, Enum):
    """Channels the Notification Service can dispatch messages through."""
    EMAIL = "email"
    SMS   = "sms"
    BOTH  = "both"


# ──────────────────────────────────────────────────────────────────────────────
# SUB-MODELS
# Reusable building blocks embedded inside event payloads. Keeping these
# separate avoids duplicating field definitions across multiple events.
# ──────────────────────────────────────────────────────────────────────────────

class PassengerDetail(BaseModel):
    """Core passenger data attached to every booking event."""

    passenger_id: str = Field(
        description="UUID of the passenger from the users table"
    )
    first_name: str = Field(min_length=1, max_length=100)
    last_name:  str = Field(min_length=1, max_length=100)
    email:      str = Field(description="Used by Notification Service")
    phone:      Optional[str] = Field(
        default=None,
        description="Required only when notification_type includes SMS"
    )
    passport_number: Optional[str] = Field(
        default=None,
        description="Required for international flights"
    )


class FlightInfo(BaseModel):
    """Flight metadata copied into events so consumers don't need a DB lookup."""

    flight_id:      str = Field(description="UUID of the flight record")
    flight_number:  str = Field(examples=["AA123", "BA456"])
    origin:         str = Field(min_length=3, max_length=3, description="IATA airport code")
    destination:    str = Field(min_length=3, max_length=3, description="IATA airport code")
    departure_time: datetime
    arrival_time:   datetime
    seat_number:    str  = Field(examples=["12A", "34C"])
    seat_class:     SeatClass

    @field_validator("origin", "destination")
    @classmethod
    def must_be_uppercase(cls, v: str) -> str:
        # IATA codes are always uppercase (e.g. LAX, JFK, LHR)
        return v.upper()


class PaymentDetail(BaseModel):
    """Payment outcome data embedded in payment events."""

    payment_id:        str     = Field(description="UUID of the payment transaction")
    amount:            Decimal = Field(gt=0, decimal_places=2)
    currency:          str     = Field(default="USD", min_length=3, max_length=3)
    payment_method:    str     = Field(examples=["credit_card", "paypal", "stripe"])
    transaction_ref:   Optional[str] = Field(
        default=None,
        description="External reference from Stripe or payment provider"
    )
    failure_reason:    Optional[str] = Field(
        default=None,
        description="Populated only when PaymentStatus is FAILED"
    )


# ──────────────────────────────────────────────────────────────────────────────
# BASE EVENT
# All events inherit from this. It guarantees that every message on every
# Kafka topic carries a unique ID, a timestamp, and a correlation ID that
# ties together all events belonging to the same booking request — essential
# for distributed tracing and debugging.
# ──────────────────────────────────────────────────────────────────────────────

class BaseEvent(BaseModel):
    """Fields present on every Kafka event regardless of topic."""

    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique ID for this specific event instance"
    )
    correlation_id: str = Field(
        description=(
            "Shared across all events for a single booking request. "
            "Set once by API Gateway and propagated through every downstream event. "
            "Use this to trace a full booking flow in logs."
        )
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC time the event was created"
    )
    version: str = Field(
        default="1.0",
        description=(
            "Schema version. Increment when making breaking changes so "
            "consumers can handle both old and new payloads during deploys."
        )
    )

    class Config:
        # Allows datetime objects to serialize to ISO 8601 strings in JSON,
        # which Kafka consumers can parse back with datetime.fromisoformat()
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Decimal:  lambda v: str(v),
        }


# ──────────────────────────────────────────────────────────────────────────────
# KAFKA EVENT PAYLOADS
# One model per topic. The topic name is documented above each class.
# ──────────────────────────────────────────────────────────────────────────────

class BookingRequestedEvent(BaseEvent):
    """
    Topic: booking.requested
    Producer: API Gateway
    Consumers: Booking Service, Seat Service

    Fired the moment a user submits a booking request. This is the entry
    point of the entire event chain. Seat Service will attempt to acquire
    a Redis lock on the seat; Booking Service will create a DB record.
    """

    booking_id:        str = Field(default_factory=lambda: str(uuid.uuid4()))
    passenger:         PassengerDetail
    flight:            FlightInfo
    total_price:       Decimal = Field(gt=0, decimal_places=2)
    currency:          str = Field(default="USD")
    notification_type: NotificationType = Field(default=NotificationType.EMAIL)


class SeatReservedEvent(BaseEvent):
    """
    Topic: seat.reserved
    Producer: Seat Service
    Consumers: Booking Service, Payment Service

    Fired when the Redis seat lock is successfully acquired. Payment Service
    listens to this to know it is safe to charge the passenger.
    """

    booking_id:   str
    flight_id:    str
    seat_number:  str
    seat_class:   SeatClass
    lock_ttl:     int = Field(description="Seconds until the Redis lock expires")


class SeatUnavailableEvent(BaseEvent):
    """
    Topic: seat.unavailable
    Producer: Seat Service
    Consumers: Booking Service

    Fired when the requested seat is already locked or sold. Booking Service
    should update the booking status to FAILED and notify the passenger.
    """

    booking_id:  str
    flight_id:   str
    seat_number: str
    reason:      str = Field(
        description="Human-readable reason, e.g. 'Seat already reserved'"
    )


class PaymentProcessedEvent(BaseEvent):
    """
    Topic: payment.processed
    Producer: Payment Service
    Consumers: Booking Service, Notification Service

    Fired after a successful charge. Booking Service marks the booking
    CONFIRMED; Notification Service sends the confirmation email/SMS.
    """

    booking_id: str
    passenger:  PassengerDetail
    flight:     FlightInfo
    payment:    PaymentDetail
    status:     PaymentStatus = PaymentStatus.SUCCESS


class PaymentFailedEvent(BaseEvent):
    """
    Topic: payment.failed
    Producer: Payment Service
    Consumers: Booking Service, Seat Service

    Fired when the charge is declined or errors out. Seat Service must
    release the Redis lock; Booking Service marks the booking FAILED.
    """

    booking_id:     str
    passenger:      PassengerDetail
    flight:         FlightInfo
    payment:        PaymentDetail
    status:         PaymentStatus = PaymentStatus.FAILED
    retry_attempt:  int = Field(
        default=0,
        description="How many times payment has been retried before giving up"
    )


class BookingConfirmedEvent(BaseEvent):
    """
    Topic: booking.confirmed
    Producer: Booking Service
    Consumers: Notification Service

    The final success event in the happy path. Carries everything the
    Notification Service needs to send a rich confirmation message.
    """

    booking_id:        str
    passenger:         PassengerDetail
    flight:            FlightInfo
    payment:           PaymentDetail
    status:            BookingStatus = BookingStatus.CONFIRMED
    notification_type: NotificationType


class BookingFailedDLQEvent(BaseEvent):
    """
    Topic: booking.failed.dlq  (Dead Letter Queue)
    Producer: Any service that exhausts retries
    Consumers: Monitored manually / alerting pipeline

    Sent when an event cannot be processed after all retry attempts. Includes
    the original raw payload so engineers can inspect or replay the message.
    Alerts should be wired to this topic in Grafana / PagerDuty.
    """

    booking_id:     Optional[str] = None
    failed_topic:   str = Field(
        description="The topic the message originally came from"
    )
    error_message:  str
    raw_payload:    str = Field(
        description="JSON string of the original unprocessable message"
    )
    retry_count:    int = Field(
        default=0,
        description="Total number of processing attempts before DLQ"
    )


# ──────────────────────────────────────────────────────────────────────────────
# HELPER UTILITIES
# Thin wrappers used by both producers and consumers to keep serialization
# logic in one place rather than scattered across services.
# ──────────────────────────────────────────────────────────────────────────────

# Maps each Kafka topic name to its corresponding event model.
# Used by consumers to deserialize raw JSON into the correct Pydantic model:
#
#   model_class = TOPIC_EVENT_MAP["booking.requested"]
#   event = model_class.model_validate_json(raw_message)
#
TOPIC_EVENT_MAP: dict[str, type[BaseEvent]] = {
    "booking.requested":  BookingRequestedEvent,
    "seat.reserved":      SeatReservedEvent,
    "seat.unavailable":   SeatUnavailableEvent,
    "payment.processed":  PaymentProcessedEvent,
    "payment.failed":     PaymentFailedEvent,
    "booking.confirmed":  BookingConfirmedEvent,
    "booking.failed.dlq": BookingFailedDLQEvent,
}


def serialize_event(event: BaseEvent) -> str:
    """
    Serialize a Pydantic event model to a JSON string for publishing to Kafka.

    Usage in a producer:
        from shared.schemas import BookingRequestedEvent, serialize_event

        event = BookingRequestedEvent(
            correlation_id=str(uuid.uuid4()),
            passenger=passenger_detail,
            flight=flight_info,
            total_price=Decimal("299.99"),
        )
        producer.produce(topic="booking.requested", value=serialize_event(event))
    """
    return event.model_dump_json()


def deserialize_event(topic: str, raw: str) -> BaseEvent:
    """
    Deserialize a raw JSON string from Kafka into the correct Pydantic model
    based on the topic it was consumed from.

    Usage in a consumer:
        from shared.schemas import deserialize_event

        event = deserialize_event(topic="booking.requested", raw=msg.value().decode())
        print(event.correlation_id)   # shared across the full booking flow
        print(event.passenger.email)  # typed, validated, IDE-friendly
    """
    model_class = TOPIC_EVENT_MAP.get(topic)
    if not model_class:
        raise ValueError(f"No event model registered for topic: '{topic}'")
    return model_class.model_validate_json(raw)