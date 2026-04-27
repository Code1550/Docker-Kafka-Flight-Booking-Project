from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:29092"
    KAFKA_CONSUMER_GROUP: str = "booking-service-group"
    POSTGRES_USER: str = "flightuser"
    POSTGRES_PASSWORD: str = "flightsecret"
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "flightbooking"
    POSTGRES_URL: str = "postgresql+asyncpg://flightuser:flightsecret@postgres:5432/flightbooking"
    TOPIC_BOOKING_REQUESTED: str = "booking.requested"
    TOPIC_SEAT_RESERVED: str = "seat.reserved"
    TOPIC_SEAT_UNAVAILABLE: str = "seat.unavailable"
    TOPIC_PAYMENT_PROCESSED: str = "payment.processed"
    TOPIC_PAYMENT_FAILED: str = "payment.failed"
    TOPIC_BOOKING_CONFIRMED: str = "booking.confirmed"
    TOPIC_BOOKING_FAILED_DLQ: str = "booking.failed.dlq"
    SEAT_LOCK_TTL_SECONDS: int = 300
    PROCESSING_RETRY_ATTEMPTS: int = 3
    DEBUG: bool = False

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
