# api-gateway/app/main.py
#
# Entry point for the API Gateway service.
#
# Responsibilities:
#   - Define the FastAPI application and all HTTP routes
#   - Manage the Kafka producer lifecycle (startup / shutdown)
#   - Validate incoming requests via Pydantic models
#   - Publish BookingRequestedEvent messages to Kafka
#   - Expose /health and /metrics endpoints for observability
#
# What this file deliberately does NOT do:
#   - Business logic (that lives in booking-service)
#   - Database writes (the gateway only publishes events)
#   - Payment processing (that lives in payment-service)

from __future__ import annotations

import uuid
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pythonjsonlogger import jsonlogger
from starlette.requests import Request
from starlette.responses import Response

from app.models import BookingRequest, BookingResponse, HealthResponse
from app.producer import KafkaProducer
from app.config import settings
from shared.schemas import (
    BookingRequestedEvent,
    PassengerDetail,
    FlightInfo,
    NotificationType,
    serialize_event,
)


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# Structured JSON logging so every log line is parseable by Grafana Loki,
# Datadog, or any other log aggregator without regex hacks.
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("api-gateway")
logger.setLevel(logging.INFO)

log_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)


# ──────────────────────────────────────────────────────────────────────────────
# PROMETHEUS METRICS
# These counters and histograms are scraped by Prometheus every 15 seconds
# and visualised in the Grafana dashboard.
# ──────────────────────────────────────────────────────────────────────────────

# Counts every booking request broken down by HTTP status code outcome
BOOKING_REQUESTS_TOTAL = Counter(
    "booking_requests_total",
    "Total number of booking requests received",
    ["status"],           # label: "success" | "validation_error" | "kafka_error"
)

# Tracks how long each booking request takes end-to-end (validation + Kafka publish)
BOOKING_REQUEST_DURATION = Histogram(
    "booking_request_duration_seconds",
    "Time taken to validate and publish a booking request",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

# Counts Kafka publish failures separately so alerts can be wired to this metric
KAFKA_PUBLISH_ERRORS_TOTAL = Counter(
    "kafka_publish_errors_total",
    "Total number of failed Kafka publish attempts",
)


# ──────────────────────────────────────────────────────────────────────────────
# KAFKA PRODUCER (module-level singleton)
# Initialised inside the lifespan context manager below so it is created
# after environment variables are loaded and torn down cleanly on shutdown.
# ──────────────────────────────────────────────────────────────────────────────

kafka_producer: KafkaProducer | None = None


# ──────────────────────────────────────────────────────────────────────────────
# LIFESPAN
# FastAPI's recommended way to run startup and shutdown logic.
# Replaces the deprecated @app.on_event("startup") / ("shutdown") pattern.
# Everything before `yield` runs on startup; everything after runs on shutdown.
# ──────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── STARTUP ──────────────────────────────────────────────────────────────
    global kafka_producer

    logger.info(
        "Starting API Gateway",
        extra={"kafka_servers": settings.KAFKA_BOOTSTRAP_SERVERS},
    )

    # Initialise the Kafka producer. KafkaProducer.connect() uses tenacity
    # to retry with exponential backoff in case Kafka isn't ready yet —
    # this is common during `docker compose up` when all containers start
    # simultaneously and Kafka takes ~15s to become healthy.
    kafka_producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    )
    await kafka_producer.connect()
    logger.info("Kafka producer connected successfully")

    yield   # ← application runs here

    # ── SHUTDOWN ─────────────────────────────────────────────────────────────
    # Flush ensures any buffered messages are delivered before the process
    # exits. Without this, in-flight Kafka messages can be silently dropped.
    if kafka_producer:
        await kafka_producer.flush()
        logger.info("Kafka producer flushed and closed")


# ──────────────────────────────────────────────────────────────────────────────
# FASTAPI APPLICATION
# ──────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Flight Booking — API Gateway",
    description=(
        "Single entry point for the flight booking system. "
        "Validates requests and publishes events to Kafka."
    ),
    version="1.0.0",
    docs_url="/docs",       # Swagger UI at http://localhost:8000/docs
    redoc_url="/redoc",     # ReDoc UI  at http://localhost:8000/redoc
    lifespan=lifespan,
)


# ──────────────────────────────────────────────────────────────────────────────
# MIDDLEWARE
# ──────────────────────────────────────────────────────────────────────────────

# CORS — allows the frontend (or Postman) to call the API from a browser.
# In production, replace allow_origins=["*"] with your actual frontend domain.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Middleware that logs every incoming request and outgoing response.
    Attaches a unique request_id so individual requests can be traced
    across log lines even when multiple requests are in flight simultaneously.
    """
    request_id = str(uuid.uuid4())
    logger.info(
        "Incoming request",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "client_ip": request.client.host if request.client else "unknown",
        },
    )

    # Attach request_id to the request state so route handlers can log it
    request.state.request_id = request_id

    response = await call_next(request)

    logger.info(
        "Outgoing response",
        extra={
            "request_id": request_id,
            "status_code": response.status_code,
        },
    )
    return response


# ──────────────────────────────────────────────────────────────────────────────
# DEPENDENCY INJECTION
# ──────────────────────────────────────────────────────────────────────────────

def get_kafka_producer() -> KafkaProducer:
    """
    FastAPI dependency that injects the Kafka producer into route handlers.
    Raises a 503 if the producer is not initialised (e.g. Kafka is down),
    so the client receives a meaningful error rather than an AttributeError.
    """
    if kafka_producer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka producer is not available. Please try again shortly.",
        )
    return kafka_producer


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES
# ──────────────────────────────────────────────────────────────────────────────

@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    tags=["Observability"],
)
async def health_check():
    """
    Used by Docker HEALTHCHECK and docker-compose depends_on condition.
    Returns 200 when the service is running and the Kafka producer is ready.
    Returns 503 when the Kafka producer has not yet connected.
    """
    producer_ready = kafka_producer is not None and kafka_producer.is_connected

    if not producer_ready:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "kafka": "disconnected",
                "timestamp": datetime.utcnow().isoformat(),
            },
        )

    return HealthResponse(
        status="healthy",
        kafka="connected",
        timestamp=datetime.utcnow(),
    )


@app.get(
    "/metrics",
    summary="Prometheus metrics",
    tags=["Observability"],
)
async def metrics():
    """
    Exposes Prometheus metrics at /metrics.
    Prometheus scrapes this endpoint every 15 seconds as configured in
    monitoring/prometheus.yml.
    """
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.post(
    "/bookings",
    response_model=BookingResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a flight booking request",
    tags=["Bookings"],
)
async def create_booking(
    request: Request,
    booking: BookingRequest,
    producer: KafkaProducer = Depends(get_kafka_producer),
):
    """
    Accepts a booking request, validates it, and publishes a
    BookingRequestedEvent to the `booking.requested` Kafka topic.

    Returns HTTP 202 Accepted immediately — the booking is not yet confirmed
    at this point. The client should poll GET /bookings/{booking_id} or
    listen for a webhook/notification once the full event chain completes.

    Flow triggered by this endpoint:
        POST /bookings
            → booking.requested  (Kafka)
                → Booking Service  (creates DB record)
                → Seat Service     (acquires Redis lock)
                    → seat.reserved / seat.unavailable
                        → Payment Service
                            → payment.processed / payment.failed
                                → Notification Service
    """
    # Generate a booking_id and correlation_id here in the gateway.
    # correlation_id is propagated through every downstream event so the
    # entire booking journey can be traced with a single ID in the logs.
    booking_id     = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())

    logger.info(
        "Processing booking request",
        extra={
            "booking_id":     booking_id,
            "correlation_id": correlation_id,
            "request_id":     request.state.request_id,
            "flight_id":      booking.flight_id,
            "passenger_id":   booking.passenger_id,
            "seat_number":    booking.seat_number,
        },
    )

    # Build the Pydantic event model from the incoming request.
    # This is the single point where HTTP request data is translated
    # into the Kafka event schema defined in shared/schemas.py.
    with BOOKING_REQUEST_DURATION.time():
        try:
            event = BookingRequestedEvent(
                booking_id=booking_id,
                correlation_id=correlation_id,
                passenger=PassengerDetail(
                    passenger_id=booking.passenger_id,
                    first_name=booking.first_name,
                    last_name=booking.last_name,
                    email=booking.email,
                    phone=booking.phone,
                    passport_number=booking.passport_number,
                ),
                flight=FlightInfo(
                    flight_id=booking.flight_id,
                    flight_number=booking.flight_number,
                    origin=booking.origin,
                    destination=booking.destination,
                    departure_time=booking.departure_time,
                    arrival_time=booking.arrival_time,
                    seat_number=booking.seat_number,
                    seat_class=booking.seat_class,
                ),
                total_price=Decimal(str(booking.total_price)),
                currency=booking.currency,
                notification_type=booking.notification_type or NotificationType.EMAIL,
            )

            # Publish the event to Kafka. The producer uses the booking_id
            # as the message key so all events for the same booking are
            # routed to the same Kafka partition, preserving ordering.
            await producer.publish(
                topic=settings.TOPIC_BOOKING_REQUESTED,
                key=booking_id,
                value=serialize_event(event),
            )

            BOOKING_REQUESTS_TOTAL.labels(status="success").inc()

            logger.info(
                "Booking event published to Kafka",
                extra={
                    "booking_id":     booking_id,
                    "correlation_id": correlation_id,
                    "topic":          settings.TOPIC_BOOKING_REQUESTED,
                },
            )

        except Exception as exc:
            # Any failure here — Kafka unavailable, serialization error, etc —
            # is treated as a 500. The client should retry. We do NOT silently
            # swallow the error and return 202, because that would make the
            # client believe the booking was accepted when it was not.
            BOOKING_REQUESTS_TOTAL.labels(status="kafka_error").inc()
            KAFKA_PUBLISH_ERRORS_TOTAL.inc()

            logger.error(
                "Failed to publish booking event",
                extra={
                    "booking_id":     booking_id,
                    "correlation_id": correlation_id,
                    "error":          str(exc),
                },
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to submit booking request. Please try again.",
            )

    # Return 202 Accepted with the booking_id and correlation_id.
    # The client uses booking_id to poll for status updates.
    # The correlation_id is included so frontend logs can be joined
    # with backend logs for support investigations.
    return BookingResponse(
        booking_id=booking_id,
        correlation_id=correlation_id,
        status="requested",
        message=(
            "Your booking request has been received and is being processed. "
            "You will receive a confirmation email shortly."
        ),
        submitted_at=datetime.utcnow(),
    )


@app.get(
    "/bookings/{booking_id}",
    summary="Get booking status",
    tags=["Bookings"],
)
async def get_booking_status(booking_id: str):
    """
    Returns the current status of a booking by its ID.

    Note: In this architecture the API Gateway does not own the bookings
    database — that belongs to the Booking Service. This endpoint is a
    placeholder showing where you would either:
      (a) query the Booking Service via an internal HTTP call, or
      (b) read from a shared Redis cache that Booking Service writes to
          after processing each event.

    For the resume project, option (b) is recommended — it keeps services
    decoupled and avoids synchronous inter-service HTTP calls.
    """
    # TODO: query Redis cache or call Booking Service internal API
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Status polling not yet implemented. Check your email for confirmation.",
    )