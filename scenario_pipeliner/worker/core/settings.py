from dataclasses import dataclass
from typing import TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Settings(BaseModel):
    """Base settings model for refactor track abstractions."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")


class ClientSettings(Settings):
    pass


class StepSettings(Settings):
    pass


TSettings = TypeVar("TSettings", bound=StepSettings)


class PipelineSettings(Settings):
    AUTORUN_CONNECT_CLIENTS: bool = True


class RunnerSettings(Settings):
    POLL_INTERVAL_SECONDS: int = Field(default=60, ge=1, le=3600)
    SHUTDOWN_TIMEOUT_SECONDS: int = Field(default=30, ge=1, le=600)


class RunnerDBSettings(RunnerSettings):
    TASKS_LIMIT: int = Field(default=10, ge=1, le=1000)
    ZOMBIE_TASKS_TIMEOUT_MINUTES: int = Field(default=30, ge=1, le=1440)
    ERROR_BACKOFF_INITIAL_SECONDS: int = Field(default=1, ge=1, le=60)
    ERROR_BACKOFF_MAX_SECONDS: int = Field(default=300, ge=1, le=3600)

    @model_validator(mode="after")
    def validate_backoff(self) -> "RunnerDBSettings":
        if self.ERROR_BACKOFF_INITIAL_SECONDS > self.ERROR_BACKOFF_MAX_SECONDS:
            raise ValueError(
                "ERROR_BACKOFF_INITIAL_SECONDS must be <= ERROR_BACKOFF_MAX_SECONDS"
            )
        return self


@dataclass(frozen=True, slots=True)
class RepositoryPollSettings:
    tasks_limit: int = 10
    zombie_timeout_minutes: int = 30


@dataclass(frozen=True, slots=True)
class TableSettings:
    tasks_table: str = "tasks"
    results_table: str = "results"
    settings_table: str = "settings"


TStepSettings = TypeVar("TStepSettings", bound=StepSettings)
TClientSettings = TypeVar("TClientSettings", bound=ClientSettings)
