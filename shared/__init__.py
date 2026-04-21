# This package contains shared code used across all microservices in the
# flight booking system. It is mounted into every service container via
# docker-compose volumes, so any change here is immediately reflected in
# all services without rebuilding images.
#
# Current modules:
#   - schemas.py  →  Pydantic models for all Kafka event payloads
#
# Usage in any service:
#   from shared.schemas import BookingRequestedEvent, PaymentProcessedEvent