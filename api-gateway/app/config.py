from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:29092"
    TOPIC_BOOKING_REQUESTED: str = "booking.requested"
    ALLOWED_ORIGINS: list[str] = ["*"]
    SECRET_KEY: str = "changeme"
    DEBUG: bool = True

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
