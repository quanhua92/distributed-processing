"""Application settings powered by Pydantic Settings."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # NATS JetStream (Generalized Jobs Stream)
    nats_url: str = "nats://127.0.0.1:4222"
    nats_stream: str = "JOBS"
    nats_subject_request: str = "jobs.request"
    nats_subject_events: str = "jobs.events"
    nats_subject_dlq: str = "jobs.dlq"
    nats_consumer_group: str = "job-workers"

    # PostgreSQL 18
    postgres_host: str = "127.0.0.1"
    postgres_port: int = 5432
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    postgres_db: str = "processing"

    # RustFS (S3-compatible)
    s3_endpoint: str = "http://127.0.0.1:9000"
    s3_access_key: str = "rustfsadmin"
    s3_secret_key: str = "rustfsadmin123"
    s3_bucket_data: str = "processing-data"
    s3_bucket_logs: str = "processing-logs"
    s3_region: str = "us-east-1"

    # OpenObserve / OpenTelemetry
    o2_otlp_endpoint: str = "http://127.0.0.1:5080/api/default"
    o2_user: str = "admin@local.dev"
    o2_password: str = "Admin123!@#"
    otel_service_name: str = "distributed-processing"

    # Worker Settings
    worker_concurrency: int = 5
    worker_max_deliver: int = 3

    # Audit Consumer Settings
    audit_batch_size: int = 100
    audit_flush_interval_seconds: int = 15

    # API Settings
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
