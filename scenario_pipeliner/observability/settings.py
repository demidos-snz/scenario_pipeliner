from __future__ import annotations

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingSettings(BaseSettings):
    """Process-wide logging / Sentry settings (not runner-specific)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    level: str = Field(
        default="INFO",
        validation_alias=AliasChoices(
            "LEVEL_LOGGING",
            "SCENARIO_PIPELINER_LOG_LEVEL",
        ),
    )
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        validation_alias="SCENARIO_PIPELINER_LOG_FORMAT",
    )
    sentry_dsn: str | None = Field(default=None, validation_alias="SENTRY_DSN")
    environment: str = Field(default="production", validation_alias="ENV")
    sentry_traces_sample_rate: float = Field(
        default=0.1,
        ge=0.0,
        le=1.0,
        validation_alias="SENTRY_TRACES_SAMPLE_RATE",
    )
    sentry_profiles_sample_rate: float = Field(
        default=0.1,
        ge=0.0,
        le=1.0,
        validation_alias="SENTRY_PROFILES_SAMPLE_RATE",
    )

    @field_validator("level")
    @classmethod
    def normalize_level(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("sentry_dsn", mode="before")
    @classmethod
    def empty_dsn_as_none(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @classmethod
    def from_env(cls) -> "LoggingSettings":
        return cls()
