from typing import Any

from pydantic import Field, field_validator

from scenario_pipeliner.db.engine import (
    normalize_postgres_url_env,
    resolve_postgres_urls,
)
from scenario_pipeliner.db.schemes_names import DEFAULT_DB_SCHEMA, validate_schema_name
from scenario_pipeliner.db.settings import PostgresPoolSettings, PostgresUrls
from scenario_pipeliner.worker.core.settings import ClientSettings


class SQLiteClientSettings(ClientSettings):
    """Settings for the unused SQLite client skeleton."""

    DB_PATH: str = ""

    @property
    def async_url(self) -> str:
        return f"sqlite+aiosqlite:///{self.DB_PATH}"

    @property
    def sync_url(self) -> str:
        return f"sqlite:///{self.DB_PATH}"


class APIClientSettings(ClientSettings):
    """HTTP client settings (base URL, timeout, optional OAuth2 password flow)."""

    API_BASE_URL: str = ""
    API_TIMEOUT: int = Field(default=30, ge=1, le=300)

    # Static token: when set, login is not called.
    API_TOKEN: str | None = None

    # OAuth2 password flow (compatible with FastAPI OAuth2PasswordRequestForm)
    API_AUTH_PATH: str = "/api/v1/auth/login"
    API_USERNAME: str | None = None
    API_PASSWORD: str | None = None
    API_TOKEN_RESPONSE_KEY: str = "access_token"

    API_TOKEN_HEADER: str = "Authorization"
    API_TOKEN_PREFIX: str = "Bearer"


class RabbitMQSettings(ClientSettings):
    """Connection settings for AsyncRabbitMQClient."""

    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBITMQ_VHOST: str = "/"


class KafkaSettings(ClientSettings):
    """Settings for the unused Kafka client skeleton."""

    bootstrap_servers: str = ""
    topic: str = ""
    group_id: str = "default-group"


class RedisSettings(ClientSettings):
    """Settings for the unused Redis client skeleton."""

    host: str = "localhost"
    port: int = 6379
    stream_key: str = "events_stream"
    group_name: str = "pipeline_group"
    consumer_name: str | None = "consumer-1"
    password: str | None = None


class PostgreSQLClientSettings(PostgresPoolSettings, ClientSettings):
    """Env-backed settings for AsyncPostgreSQLClient."""

    POSTGRES_URL: str | None = None
    POSTGRES_HOST: str = ""
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = ""
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    SCENARIO_PIPELINER_DB_SCHEMA: str = DEFAULT_DB_SCHEMA

    @field_validator("POSTGRES_URL", mode="before")
    @classmethod
    def normalize_postgres_url(cls, value: Any) -> str | None:
        return normalize_postgres_url_env(value)

    @field_validator("SCENARIO_PIPELINER_DB_SCHEMA")
    @classmethod
    def normalize_db_schema(cls, value: str) -> str:
        return validate_schema_name(value)

    def _urls(self) -> PostgresUrls:
        return resolve_postgres_urls(
            url=self.POSTGRES_URL,
            host=self.POSTGRES_HOST or None,
            port=self.POSTGRES_PORT,
            database=self.POSTGRES_DB or None,
            user=self.POSTGRES_USER or None,
            password=self.POSTGRES_PASSWORD,
        )

    @property
    def async_url(self) -> str:
        return self._urls().async_url

    @property
    def sync_url(self) -> str:
        return self._urls().dsn
