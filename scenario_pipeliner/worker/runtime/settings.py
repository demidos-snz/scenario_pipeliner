from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

from pydantic import (
    AliasChoices,
    BeforeValidator,
    Field,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from scenario_pipeliner.api.enums import DbBackend, Mode
from scenario_pipeliner.api.settings import ScenarioPipelinerConfig
from scenario_pipeliner.db.engine import (
    normalize_postgres_url_env,
    resolve_postgres_urls,
)
from scenario_pipeliner.db.schemes_names import DEFAULT_DB_SCHEMA, validate_schema_name
from scenario_pipeliner.db.settings import PostgresPoolSettings, PostgresUrls
from scenario_pipeliner.version import __version__
from scenario_pipeliner.worker.core.settings import (
    RunnerBrokerSettings,
    RunnerDBSettings,
)


def _empty_str_as_none(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() in {"", "none", "null"}:
        return None
    return value


class RuntimeEnvSettings(BaseSettings):
    """Runner process settings loaded from environment via pydantic-settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    mode: Mode = Field(
        default=Mode.DEV,
        validation_alias="SCENARIO_PIPELINER_MODE",
    )
    db_backend: DbBackend = Field(
        default=DbBackend.POSTGRESQL,
        validation_alias="RUNNER_DB_BACKEND",
    )
    plugins_root: Path = Field(
        default_factory=lambda: Path("plugins"),
        validation_alias="SCENARIO_PIPELINER_PLUGINS_ROOT",
    )
    core_version: str = Field(
        default=__version__,
        validation_alias="SCENARIO_PIPELINER_CORE_VERSION",
    )
    db_schema: str = Field(
        default=DEFAULT_DB_SCHEMA,
        validation_alias="SCENARIO_PIPELINER_DB_SCHEMA",
    )
    runner_poll_interval_seconds: int = Field(
        default=2,
        ge=1,
        le=3600,
        validation_alias="RUNNER_POLL_INTERVAL_SECONDS",
    )
    runner_shutdown_timeout_seconds: int = Field(
        default=30,
        ge=1,
        le=600,
        validation_alias=AliasChoices(
            "RUNNER_SHUTDOWN_TIMEOUT_SECONDS",
            "SHUTDOWN_TIMEOUT_SECONDS",
        ),
    )
    runner_tasks_limit: int = Field(
        default=10,
        ge=1,
        le=1000,
        validation_alias="RUNNER_TASKS_LIMIT",
    )
    runner_zombie_tasks_timeout_minutes: int = Field(
        default=30,
        ge=1,
        le=1440,
        validation_alias="RUNNER_ZOMBIE_TASKS_TIMEOUT_MINUTES",
    )
    runner_max_concurrent_tasks: int = Field(
        default=5,
        ge=1,
        le=1000,
        validation_alias="RUNNER_MAX_CONCURRENT_TASKS",
    )
    runner_heartbeat_interval_seconds: int = Field(
        default=30,
        ge=1,
        le=3600,
        validation_alias="RUNNER_HEARTBEAT_INTERVAL_SECONDS",
    )
    runner_reconcile_limit: int = Field(
        default=50,
        ge=1,
        le=1000,
        validation_alias="RUNNER_RECONCILE_LIMIT",
    )
    runner_outbox_limit: int = Field(
        default=20,
        ge=1,
        le=1000,
        validation_alias="RUNNER_OUTBOX_LIMIT",
    )
    run_seconds: Annotated[int | None, BeforeValidator(_empty_str_as_none)] = Field(
        default=None,
        ge=1,
        validation_alias="RUNNER_RUN_SECONDS",
    )
    apply_migrations: bool = Field(
        default=True,
        validation_alias="RUNNER_APPLY_MIGRATIONS",
    )
    runner_mode: str = Field(
        default="db",
        validation_alias="SCENARIO_PIPELINER_RUNNER_MODE",
    )

    @field_validator("plugins_root", mode="before")
    @classmethod
    def normalize_plugins_root(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()

    @field_validator("db_schema")
    @classmethod
    def normalize_db_schema(cls, value: str) -> str:
        return validate_schema_name(value)

    @field_validator("runner_mode", mode="before")
    @classmethod
    def normalize_runner_mode(cls, value: str) -> str:
        normalized = str(value or "db").strip().lower()
        if normalized not in {"db", "broker", "all"}:
            raise ValueError(
                "SCENARIO_PIPELINER_RUNNER_MODE must be one of: db, broker, all"
            )
        return normalized

    @model_validator(mode="after")
    def validate_backend(self) -> "RuntimeEnvSettings":
        if self.db_backend != DbBackend.POSTGRESQL:
            raise ValueError(
                "runtime entrypoint currently supports only RUNNER_DB_BACKEND=postgresql"
            )
        return self

    @classmethod
    def from_env(cls) -> "RuntimeEnvSettings":
        return cls()

    def scenario_config(self) -> ScenarioPipelinerConfig:
        return ScenarioPipelinerConfig(
            mode=self.mode,
            db_backend=self.db_backend,
            plugins_root=self.plugins_root,
            core_version=self.core_version,
            db_schema=self.db_schema,
        )

    def runner_settings(self) -> RunnerDBSettings:
        return RunnerDBSettings(
            POLL_INTERVAL_SECONDS=self.runner_poll_interval_seconds,
            SHUTDOWN_TIMEOUT_SECONDS=self.runner_shutdown_timeout_seconds,
            TASKS_LIMIT=self.runner_tasks_limit,
            ZOMBIE_TASKS_TIMEOUT_MINUTES=self.runner_zombie_tasks_timeout_minutes,
            MAX_CONCURRENT_TASKS=self.runner_max_concurrent_tasks,
            HEARTBEAT_INTERVAL_SECONDS=self.runner_heartbeat_interval_seconds,
        )

    def broker_runner_settings(self) -> RunnerBrokerSettings:
        return RunnerBrokerSettings(
            POLL_INTERVAL_SECONDS=self.runner_poll_interval_seconds,
            SHUTDOWN_TIMEOUT_SECONDS=self.runner_shutdown_timeout_seconds,
            RECONCILE_LIMIT=self.runner_reconcile_limit,
            OUTBOX_LIMIT=self.runner_outbox_limit,
        )


class PostgresRuntimeSettings(PostgresPoolSettings, BaseSettings):
    """Postgres connection settings for the runner entrypoint."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    POSTGRES_URL: str | None = None
    POSTGRES_HOST: str = ""
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = ""
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""

    @field_validator("POSTGRES_URL", mode="before")
    @classmethod
    def normalize_postgres_url(cls, value: Any) -> str | None:
        return normalize_postgres_url_env(value)

    @classmethod
    def from_env(cls) -> "PostgresRuntimeSettings":
        return cls()

    @property
    def urls(self) -> PostgresUrls:
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
        return self.urls.async_url

    @property
    def sync_url(self) -> str:
        return self.urls.dsn
