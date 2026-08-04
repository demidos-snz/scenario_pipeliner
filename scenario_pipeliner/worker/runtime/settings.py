from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote_plus

from pydantic import BeforeValidator, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from scenario_pipeliner.api.config import ScenarioPipelinerConfig
from scenario_pipeliner.api.enums import DbBackend, Mode
from scenario_pipeliner.worker.core.settings import RunnerDBSettings


def _empty_str_as_none(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() in {"", "none", "null"}:
        return None
    return value


class RuntimeEnvSettings(BaseSettings):
    """Runner process settings loaded from environment via pydantic-settings."""

    model_config = SettingsConfigDict(
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
        default="0.1.0",
        validation_alias="SCENARIO_PIPELINER_CORE_VERSION",
    )
    runner_poll_interval_seconds: int = Field(
        default=2,
        ge=1,
        le=3600,
        validation_alias="RUNNER_POLL_INTERVAL_SECONDS",
    )
    runner_tasks_limit: int = Field(
        default=10,
        ge=1,
        le=1000,
        validation_alias="RUNNER_TASKS_LIMIT",
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

    @field_validator("plugins_root", mode="before")
    @classmethod
    def normalize_plugins_root(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()

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
        )

    def runner_settings(self) -> RunnerDBSettings:
        return RunnerDBSettings(
            POLL_INTERVAL_SECONDS=self.runner_poll_interval_seconds,
            TASKS_LIMIT=self.runner_tasks_limit,
        )


class PostgresRuntimeSettings(BaseSettings):
    """Postgres connection settings for the runner entrypoint."""

    model_config = SettingsConfigDict(
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    POSTGRES_HOST: str = ""
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = ""
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    DB_POOL_MIN_SIZE: int = Field(default=1, ge=1, le=10)
    DB_POOL_MAX_SIZE: int = Field(default=5, ge=1, le=10)

    @model_validator(mode="after")
    def validate_pool_bounds(self) -> "PostgresRuntimeSettings":
        if self.DB_POOL_MIN_SIZE > self.DB_POOL_MAX_SIZE:
            raise ValueError(
                f"DB_POOL_MIN_SIZE ({self.DB_POOL_MIN_SIZE}) must be "
                f"<= DB_POOL_MAX_SIZE ({self.DB_POOL_MAX_SIZE})"
            )
        return self

    @classmethod
    def from_env(cls) -> "PostgresRuntimeSettings":
        return cls()

    @property
    def sync_url(self) -> str:
        user = quote_plus(self.POSTGRES_USER)
        password = quote_plus(self.POSTGRES_PASSWORD)
        return (
            f"postgresql://{user}:{password}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )
