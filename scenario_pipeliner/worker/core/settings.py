from pydantic import Field, model_validator

from scenario_pipeliner.base_settings import FrozenSettings, Settings

__all__ = [
    "ClientSettings",
    "FrozenSettings",
    "PipelineSettings",
    "RunnerBrokerSettings",
    "RunnerDBSettings",
    "RunnerSettings",
    "Settings",
    "StepSettings",
]


class ClientSettings(Settings):
    pass


class StepSettings(Settings):
    pass


class PipelineSettings(Settings):
    AUTORUN_CONNECT_CLIENTS: bool = True


class RunnerSettings(Settings):
    POLL_INTERVAL_SECONDS: int = Field(default=60, ge=1, le=3600)
    SHUTDOWN_TIMEOUT_SECONDS: int = Field(default=30, ge=1, le=600)


class RunnerDBSettings(RunnerSettings):
    TASKS_LIMIT: int = Field(default=10, ge=1, le=1000)
    ZOMBIE_TASKS_TIMEOUT_MINUTES: int = Field(default=30, ge=1, le=1440)
    MAX_CONCURRENT_TASKS: int = Field(default=5, ge=1, le=1000)
    HEARTBEAT_INTERVAL_SECONDS: int = Field(default=30, ge=1, le=3600)
    ERROR_BACKOFF_INITIAL_SECONDS: int = Field(default=1, ge=1, le=60)
    ERROR_BACKOFF_MAX_SECONDS: int = Field(default=300, ge=1, le=3600)

    @model_validator(mode="after")
    def validate_backoff(self) -> "RunnerDBSettings":
        if self.ERROR_BACKOFF_INITIAL_SECONDS > self.ERROR_BACKOFF_MAX_SECONDS:
            raise ValueError(
                "ERROR_BACKOFF_INITIAL_SECONDS must be <= ERROR_BACKOFF_MAX_SECONDS"
            )
        return self


class RunnerBrokerSettings(RunnerSettings):
    """Outbox reconcile/publish cadence for ``run --mode broker``."""

    RECONCILE_LIMIT: int = Field(default=50, ge=1, le=1000)
    OUTBOX_LIMIT: int = Field(default=20, ge=1, le=1000)
