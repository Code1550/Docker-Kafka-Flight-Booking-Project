from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:29092"
    KAFKA_CONSUMER_GROUP: str = "seat-service-group"
    REDIS_URL: str = "redis://:redissecret@redis:6379/0"
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = "redissecret"
    REDIS_DB: int = 0
    SEAT_LOCK_TTL_SECONDS: int = 300
    TOPIC_BOOKING_REQUESTED: str = "booking.requested"
    TOPIC_PAYMENT_FAILED: str = "payment.failed"
    TOPIC_SEAT_RESERVED: str = "seat.reserved"
    TOPIC_SEAT_UNAVAILABLE: str = "seat.unavailable"
    TOPIC_BOOKING_FAILED_DLQ: str = "booking.failed.dlq"
    PROCESSING_RETRY_ATTEMPTS: int = 3

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
