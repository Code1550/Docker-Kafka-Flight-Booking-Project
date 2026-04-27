from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:29092"
    KAFKA_CONSUMER_GROUP: str = "notification-service-group"
    TOPIC_BOOKING_CONFIRMED: str = "booking.confirmed"
    TOPIC_BOOKING_FAILED_DLQ: str = "booking.failed.dlq"
    SENDGRID_MOCK_ENABLED: bool = True
    SENDGRID_API_KEY: str = "SG.mock"
    SENDGRID_FROM_EMAIL: str = "noreply@flightbooking.com"
    SENDGRID_FROM_NAME: str = "Flight Booking System"
    TWILIO_MOCK_ENABLED: bool = True
    TWILIO_ACCOUNT_SID: str = "ACmock"
    TWILIO_AUTH_TOKEN: str = "mock"
    TWILIO_FROM_NUMBER: str = "+15005550006"
    NOTIFICATION_FROM_EMAIL: str = "support@flightbooking.com"
    NOTIFICATION_RETRY_ATTEMPTS: int = 3

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
