from pydantic_settings import BaseSettings
from pydantic import PostgresDsn, field_validator
from typing import List, Optional
import secrets

class Settings(BaseSettings):
    # Project
    PROJECT_NAME: str = "AegisOps"
    API_V1_STR: str = "/api/v1"
    VERSION: str = "0.1.0"

    # Security
    SECRET_KEY: str = secrets.token_urlsafe(32)
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8  # 8 days
    ALGORITHM: str = "HS256"

    # Database
    DATABASE_URL: PostgresDsn
    DATABASE_TEST_URL: Optional[PostgresDsn] = None

    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: Optional[str] = None
    REDIS_DB: int = 0

    # CORS
    BACKEND_CORS_ORIGINS: List[str] = ["*"]

    # Celery
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"

    @field_validator("DATABASE_URL")
    @classmethod
    def assemble_db_connection(cls, v: Optional[str], info) -> str:
        if isinstance(v, str):
            return v
        # For SQLite fallback in development (if needed)
        return "sqlite:///./test.db"

    class Config:
        case_sensitive = True
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()