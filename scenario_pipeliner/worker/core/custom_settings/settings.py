from typing import Self
from urllib.parse import quote_plus

from pydantic import Field, model_validator

from scenario_pipeliner.worker.core.settings import ClientSettings


class DBClientSettings(ClientSettings):
    """Класс для настроек базы данных."""

    TASKS_TABLE_NAME: str = "tasks"
    RESULTS_TABLE_NAME: str = "results"
    SETTINGS_TABLE_NAME: str = "settings"


class SQLiteClientSettings(DBClientSettings):
    """Класс для настроек AsyncSQLiteClient."""

    DB_PATH: str = ""

    @property
    def async_url(self) -> str:
        return f"sqlite+aiosqlite:///{self.DB_PATH}"

    @property
    def sync_url(self) -> str:
        return f"sqlite:///{self.DB_PATH}"


class APIClientSettings(ClientSettings):
    """Класс для настроек AsyncHTTPxAPIClient."""

    API_BASE_URL: str = ""
    API_TIMEOUT: int = Field(default=30, ge=1, le=300)

    # Статический токен (если задан — login не вызывается)
    API_TOKEN: str | None = None

    # OAuth2 password flow (совместим с FastAPI OAuth2PasswordRequestForm)
    API_AUTH_PATH: str = "/api/v1/auth/login"
    API_USERNAME: str | None = None
    API_PASSWORD: str | None = None
    API_TOKEN_RESPONSE_KEY: str = "access_token"

    API_TOKEN_HEADER: str = "Authorization"
    API_TOKEN_PREFIX: str = "Bearer"


class RabbitMQSettings(ClientSettings):
    """Класс для настроек RabbitMQ."""

    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBITMQ_QUEUE: str = "tasks_queue"
    RABBITMQ_VHOST: str = "/"
    RABBITMQ_TIMEOUT: float = 1.0


class KafkaSettings(ClientSettings):
    """Класс для настроек Kafka."""

    bootstrap_servers: str = ""
    topic: str = ""
    group_id: str = "default-group"


class RedisSettings(ClientSettings):
    """Класс для настроек Redis."""

    host: str = "localhost"
    port: int = 6379
    stream_key: str = "events_stream"
    group_name: str = "pipeline_group"
    consumer_name: str | None = "consumer-1"
    password: str | None = None


class PostgreSQLClientSettings(DBClientSettings):
    """Класс для настроек AsyncPostgreSQLClient."""

    POSTGRES_HOST: str = ""
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = ""
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    DB_POOL_MIN_SIZE: int = Field(default=1, ge=1, le=10)
    DB_POOL_MAX_SIZE: int = Field(default=5, ge=1, le=10)

    @model_validator(mode="after")
    def validate_pool_bounds(self) -> Self:
        if self.DB_POOL_MIN_SIZE > self.DB_POOL_MAX_SIZE:
            raise ValueError(
                f"DB_POOL_MIN_SIZE ({self.DB_POOL_MIN_SIZE}) must be "
                f"<= DB_POOL_MAX_SIZE ({self.DB_POOL_MAX_SIZE})"
            )
        return self

    @property
    def async_url(self) -> str:
        return f"postgresql+asyncpg://{self.__con_str}"

    @property
    def sync_url(self) -> str:
        return f"postgresql://{self.__con_str}"

    @property
    def __con_str(self) -> str:
        user: str = quote_plus(self.POSTGRES_USER)
        password: str = quote_plus(self.POSTGRES_PASSWORD)
        return f"{user}:{password}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
