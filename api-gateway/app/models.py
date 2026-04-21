# api-gateway/app/models.py
#
# Pydantic models for the API Gateway's HTTP request and response payloads.
#
# These are deliberately separate from the Kafka event schemas in
# shared/schemas.py for one important reason: the HTTP API contract
# (what the client sends and receives) should be able to evolve
# independently from the internal Kafka event schema (what services
# publish and consume). Keeping them separate means you can add a new
# HTTP field without touching the event schema, and vice versa.
#
# Model roles:
#   BookingRequest  → validates the incoming POST /bookings request body
#   BookingResponse → shapes the 202 Accepted response back to the client
#   HealthResponse  → shapes the GET /health response

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import (
    BaseModel,
    EmailStr,
    Field,
    field_validator,
    model_validator,
)

from shared.schemas import SeatClass, NotificationType


# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# Centralised here so validation rules are easy to find and adjust.
# ──────────────────────────────────────────────────────────────────────────────

# IATA airport codes are always exactly 3 uppercase letters (e.g. LAX, JFK)
IATA_CODE_LENGTH = 3

# Reasonable price bounds — protects against fat-finger entries and
# potential abuse (e.g. a $0 booking or a $999,999 booking)
MIN_PRICE = Decimal("0.01")
MAX_PRICE = Decimal("99999.99")

# Supported currencies — extend this list as needed
SUPPORTED_CURRENCIES = {"USD", "EUR", "GBP", "CAD", "AUD", "JPY", "SGD"}

# Flight number format: 2–3 uppercase letters followed by 1–4 digits
# Examples: AA123, BA4567, QF12
import re
FLIGHT_NUMBER_PATTERN = re.compile(r"^[A-Z]{2,3}[0-9]{1,4}$")


# ──────────────────────────────────────────────────────────────────────────────
# REQUEST MODEL
# Represents the JSON body the client sends to POST /bookings.
# Every field is validated before the route handler runs — invalid requests
# are rejected with a 422 Unprocessable Entity before touching Kafka.
# ──────────────────────────────────────────────────────────────────────────────

class BookingRequest(BaseModel):
    """
    Incoming HTTP request body for POST /bookings.

    Example request body:
    {
        "passenger_id":    "a1b2c3d4-...",
        "first_name":      "Jane",
        "last_name":       "Doe",
        "email":           "jane.doe@example.com",
        "phone":           "+14155552671",
        "passport_number": "P12345678",
        "flight_id":       "f9e8d7c6-...",
        "flight_number":   "AA123",
        "origin":          "JFK",
        "destination":     "LAX",
        "departure_time":  "2024-09-01T08:00:00",
        "arrival_time":    "2024-09-01T11:30:00",
        "seat_number":     "12A",
        "seat_class":      "economy",
        "total_price":     299.99,
        "currency":        "USD",
        "notification_type": "email"
    }
    """

    # ── PASSENGER FIELDS ──────────────────────────────────────────────────────

    passenger_id: str = Field(
        description="UUID of the authenticated passenger from the users table",
        examples=["a1b2c3d4-e5f6-7890-abcd-ef1234567890"],
    )

    first_name: str = Field(
        min_length=1,
        max_length=100,
        description="Passenger's legal first name as it appears on their passport",
        examples=["Jane"],
    )

    last_name: str = Field(
        min_length=1,
        max_length=100,
        description="Passenger's legal last name as it appears on their passport",
        examples=["Doe"],
    )

    # EmailStr performs RFC 5322 email validation — requires the
    # email-validator package to be installed (included in requirements.txt)
    email: EmailStr = Field(
        description="Contact email — used by Notification Service to send confirmation",
        examples=["jane.doe@example.com"],
    )

    phone: Optional[str] = Field(
        default=None,
        description=(
            "E.164 formatted phone number — required when notification_type "
            "is 'sms' or 'both'. Example: +14155552671"
        ),
        examples=["+14155552671"],
    )

    passport_number: Optional[str] = Field(
        default=None,
        max_length=20,
        description="Required for international flights (origin != destination country)",
        examples=["P12345678"],
    )

    # ── FLIGHT FIELDS ─────────────────────────────────────────────────────────

    flight_id: str = Field(
        description="UUID of the flight record in the database",
        examples=["f9e8d7c6-b5a4-3210-fedc-ba9876543210"],
    )

    flight_number: str = Field(
        description="IATA flight number — 2-3 uppercase letters followed by 1-4 digits",
        examples=["AA123", "BA4567"],
    )

    origin: str = Field(
        min_length=IATA_CODE_LENGTH,
        max_length=IATA_CODE_LENGTH,
        description="IATA 3-letter departure airport code",
        examples=["JFK", "LHR"],
    )

    destination: str = Field(
        min_length=IATA_CODE_LENGTH,
        max_length=IATA_CODE_LENGTH,
        description="IATA 3-letter arrival airport code",
        examples=["LAX", "CDG"],
    )

    departure_time: datetime = Field(
        description="Scheduled departure time in UTC (ISO 8601)",
        examples=["2024-09-01T08:00:00"],
    )

    arrival_time: datetime = Field(
        description="Scheduled arrival time in UTC (ISO 8601)",
        examples=["2024-09-01T11:30:00"],
    )

    seat_number: str = Field(
        min_length=2,
        max_length=4,
        description="Seat identifier — row number followed by column letter",
        examples=["12A", "34C", "1A"],
    )

    seat_class: SeatClass = Field(
        description="Cabin class — economy, premium_economy, business, or first",
        examples=["economy", "business"],
    )

    # ── PRICING FIELDS ────────────────────────────────────────────────────────

    total_price: Decimal = Field(
        ge=MIN_PRICE,
        le=MAX_PRICE,
        decimal_places=2,
        description="Total fare in the specified currency",
        examples=[299.99, 1250.00],
    )

    currency: str = Field(
        default="USD",
        min_length=3,
        max_length=3,
        description="ISO 4217 currency code",
        examples=["USD", "EUR", "GBP"],
    )

    # ── NOTIFICATION PREFERENCE ───────────────────────────────────────────────

    notification_type: Optional[NotificationType] = Field(
        default=NotificationType.EMAIL,
        description="How the passenger wants to receive their booking confirmation",
        examples=["email", "sms", "both"],
    )


    # ── FIELD VALIDATORS ──────────────────────────────────────────────────────
    # Run after Pydantic's built-in type coercion, before the route handler.
    # Returning the (possibly transformed) value replaces the field's value.

    @field_validator("origin", "destination")
    @classmethod
    def uppercase_iata(cls, v: str) -> str:
        """IATA codes must be uppercase. Coerce silently rather than reject —
        a client sending 'jfk' should get the same result as 'JFK'."""
        return v.upper()

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        """Reject unsupported currencies early so the Kafka event never
        contains a currency the Payment Service cannot handle."""
        v = v.upper()
        if v not in SUPPORTED_CURRENCIES:
            raise ValueError(
                f"Unsupported currency '{v}'. "
                f"Supported currencies: {', '.join(sorted(SUPPORTED_CURRENCIES))}"
            )
        return v

    @field_validator("flight_number")
    @classmethod
    def validate_flight_number(cls, v: str) -> str:
        """Enforce the IATA flight number format (e.g. AA123, BA4567)."""
        v = v.upper()
        if not FLIGHT_NUMBER_PATTERN.match(v):
            raise ValueError(
                f"Invalid flight number '{v}'. "
                "Expected format: 2-3 uppercase letters followed by 1-4 digits (e.g. AA123)"
            )
        return v

    @field_validator("seat_number")
    @classmethod
    def validate_seat_number(cls, v: str) -> str:
        """Seat numbers must be a row integer followed by a single column letter.
        Coerce to uppercase and validate the pattern."""
        v = v.upper()
        if not re.match(r"^\d{1,3}[A-K]$", v):
            raise ValueError(
                f"Invalid seat number '{v}'. "
                "Expected format: row number (1-3 digits) followed by a letter A-K (e.g. 12A)"
            )
        return v

    @field_validator("departure_time", "arrival_time")
    @classmethod
    def must_be_future(cls, v: datetime) -> datetime:
        """Reject bookings for flights that have already departed.
        Comparison is naive (no timezone) — in production, enforce UTC."""
        if v < datetime.utcnow():
            raise ValueError(
                f"Flight time {v.isoformat()} is in the past. "
                "Please select a future departure."
            )
        return v

    @field_validator("first_name", "last_name")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        """Strip leading/trailing whitespace from name fields.
        A client sending '  Jane  ' should be treated the same as 'Jane'."""
        return v.strip()


    # ── MODEL VALIDATORS ──────────────────────────────────────────────────────
    # Run after all field validators. Used for cross-field validation where
    # the rule depends on more than one field at the same time.

    @model_validator(mode="after")
    def arrival_after_departure(self) -> BookingRequest:
        """Arrival must be after departure. Catches obvious data errors
        (e.g. reversed dates) before they propagate into Kafka events."""
        if self.arrival_time <= self.departure_time:
            raise ValueError(
                "arrival_time must be after departure_time. "
                f"Got departure={self.departure_time.isoformat()}, "
                f"arrival={self.arrival_time.isoformat()}"
            )
        return self

    @model_validator(mode="after")
    def origin_differs_from_destination(self) -> BookingRequest:
        """Origin and destination airports must be different."""
        if self.origin == self.destination:
            raise ValueError(
                f"Origin and destination cannot be the same airport ({self.origin}). "
                "Please check your flight details."
            )
        return self

    @model_validator(mode="after")
    def phone_required_for_sms(self) -> BookingRequest:
        """Phone number is required when the passenger opts into SMS notifications.
        Validated here (not at field level) because it depends on notification_type."""
        if self.notification_type in (
            NotificationType.SMS,
            NotificationType.BOTH,
        ) and not self.phone:
            raise ValueError(
                "phone is required when notification_type is 'sms' or 'both'. "
                "Please provide a valid E.164 phone number (e.g. +14155552671)."
            )
        return self

    class Config:
        # Allows the model to be initialised from ORM objects as well as
        # dicts — useful if you add a booking lookup endpoint later that
        # reads from SQLAlchemy models.
        from_attributes = True

        # Show a worked example in the auto-generated /docs Swagger UI
        json_schema_extra = {
            "example": {
                "passenger_id":    "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "first_name":      "Jane",
                "last_name":       "Doe",
                "email":           "jane.doe@example.com",
                "phone":           "+14155552671",
                "passport_number": "P12345678",
                "flight_id":       "f9e8d7c6-b5a4-3210-fedc-ba9876543210",
                "flight_number":   "AA123",
                "origin":          "JFK",
                "destination":     "LAX",
                "departure_time":  "2025-09-01T08:00:00",
                "arrival_time":    "2025-09-01T11:30:00",
                "seat_number":     "12A",
                "seat_class":      "economy",
                "total_price":     299.99,
                "currency":        "USD",
                "notification_type": "email",
            }
        }


# ──────────────────────────────────────────────────────────────────────────────
# RESPONSE MODELS
# Shape the JSON the API Gateway sends back to the client.
# Keeping these as explicit Pydantic models (rather than returning raw dicts)
# means FastAPI validates outgoing data too, and the /docs page shows the
# exact response schema the client can expect.
# ──────────────────────────────────────────────────────────────────────────────

class BookingResponse(BaseModel):
    """
    Response body for a successful POST /bookings (HTTP 202 Accepted).

    The client receives a booking_id to poll for status updates and a
    correlation_id to include in support requests for faster debugging.
    """

    booking_id: str = Field(
        description="UUID assigned to this booking — use this to poll for status",
        examples=["c3d4e5f6-a1b2-3456-cdef-012345678901"],
    )

    correlation_id: str = Field(
        description=(
            "Trace ID shared across all internal services for this booking. "
            "Include this in support requests to help engineers locate logs."
        ),
        examples=["d4e5f6a7-b2c3-4567-defa-123456789012"],
    )

    status: str = Field(
        description="Current lifecycle status of the booking",
        examples=["requested"],
    )

    message: str = Field(
        description="Human-readable description of the current status",
        examples=["Your booking request has been received and is being processed."],
    )

    submitted_at: datetime = Field(
        description="UTC timestamp when the booking request was accepted by the gateway",
        examples=["2024-09-01T07:45:00"],
    )

    class Config:
        json_schema_extra = {
            "example": {
                "booking_id":     "c3d4e5f6-a1b2-3456-cdef-012345678901",
                "correlation_id": "d4e5f6a7-b2c3-4567-defa-123456789012",
                "status":         "requested",
                "message":        (
                    "Your booking request has been received and is being processed. "
                    "You will receive a confirmation email shortly."
                ),
                "submitted_at":   "2024-09-01T07:45:00",
            }
        }


class HealthResponse(BaseModel):
    """
    Response body for GET /health.
    Used by Docker HEALTHCHECK and load balancer liveness probes.
    """

    status: str = Field(
        description="Overall service health — 'healthy' or 'unhealthy'",
        examples=["healthy"],
    )

    kafka: str = Field(
        description="Kafka producer connection state — 'connected' or 'disconnected'",
        examples=["connected"],
    )

    timestamp: datetime = Field(
        description="UTC time the health check was evaluated",
        examples=["2024-09-01T07:45:00"],
    )

    class Config:
        json_schema_extra = {
            "example": {
                "status":    "healthy",
                "kafka":     "connected",
                "timestamp": "2024-09-01T07:45:00",
            }
        }
