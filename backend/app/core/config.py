from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

class Settings(BaseSettings):
    """Runtime configuration loaded from the repository-level ``.env`` file."""

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=REPOSITORY_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Project
    PROJECT_NAME: str = "AegisOps"
    API_V1_STR: str = "/api/v1"
    VERSION: str = "0.1.0"

    # Security
    # This is deliberately conspicuous. A deployment must supply a real value.
    SECRET_KEY: str = "change-this-development-secret-before-deployment"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8  # 8 days
    ALGORITHM: str = "HS256"

    # Database
    # Local default; shared and deployed environments must override this value.
    DATABASE_URL: str = "postgresql+asyncpg://aegisops:aegisops@localhost:5432/aegisops"
    DATABASE_TEST_URL: Optional[str] = None

    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: Optional[str] = None
    REDIS_DB: int = 0

    # CORS
    BACKEND_CORS_ORIGINS: list[str] = ["http://localhost:3000"]

    # Celery
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CLIENT_ID: str = "aegisops-telemetry-producer"
    KAFKA_REQUEST_TIMEOUT_MS: int = 5000
    KAFKA_METRICS_CONSUMER_GROUP: str = "aegis-metrics-consumer-group-v1"
    KAFKA_EVIDENCE_CONSUMER_GROUP: str = "aegis-evidence-consumer-group-v1"

    # VictoriaMetrics
    VICTORIAMETRICS_URL: str = "http://localhost:8428"
    VICTORIAMETRICS_TIMEOUT_SECONDS: float = 5.0
    TELEMETRY_PERSISTENCE_RETRY_MAX_ATTEMPTS: int = 3

    # OpenSearch
    OPENSEARCH_URL: str = "http://localhost:9200"
    OPENSEARCH_USER: Optional[str] = None
    OPENSEARCH_PASSWORD: Optional[str] = None
    OPENSEARCH_TIMEOUT_SECONDS: float = 5.0

    # Telemetry Reliability & Observability
    TELEMETRY_QUARANTINE_PATH: str = ".aegis/quarantine/failed_events.jsonl"
    TELEMETRY_OBSERVABILITY_SAMPLE_WINDOW: int = 1000

settings = Settings()
