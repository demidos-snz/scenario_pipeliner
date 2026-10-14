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


DEFAULT_POLL_INTERVAL_SECONDS = 2
DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30
DEFAULT_TASKS_LIMIT = 10
DEFAULT_ZOMBIE_TASKS_TIMEOUT_MINUTES = 30
DEFAULT_MAX_CONCURRENT_TASKS = 5
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30
DEFAULT_ERROR_BACKOFF_INITIAL_SECONDS = 1
DEFAULT_ERROR_BACKOFF_MAX_SECONDS = 300
DEFAULT_RECONCILE_LIMIT = 50
DEFAULT_OUTBOX_LIMIT = 20
DEFAULT_INGRESS_HANDLER_TIMEOUT_SECONDS = 120
DEFAULT_PING_FAILURE_LIMIT = 3


class ClientSettings(Settings):
    pass


class StepSettings(Settings):
    pass


class PipelineSettings(Settings):
    AUTORUN_CONNECT_CLIENTS: bool = True


class RunnerSettings(Settings):
    POLL_INTERVAL_SECONDS: int = Field(
        default=DEFAULT_POLL_INTERVAL_SECONDS, ge=1, le=3600
    )
    SHUTDOWN_TIMEOUT_SECONDS: int = Field(
        default=DEFAULT_SHUTDOWN_TIMEOUT_SECONDS, ge=1, le=600
    )


class RunnerDBSettings(RunnerSettings):
    TASKS_LIMIT: int = Field(default=DEFAULT_TASKS_LIMIT, ge=1, le=1000)
    ZOMBIE_TASKS_TIMEOUT_MINUTES: int = Field(
        default=DEFAULT_ZOMBIE_TASKS_TIMEOUT_MINUTES, ge=1, le=1440
    )
    MAX_CONCURRENT_TASKS: int = Field(
        default=DEFAULT_MAX_CONCURRENT_TASKS, ge=1, le=1000
    )
    HEARTBEAT_INTERVAL_SECONDS: int = Field(
        default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS, ge=1, le=3600
    )
    ERROR_BACKOFF_INITIAL_SECONDS: int = Field(
        default=DEFAULT_ERROR_BACKOFF_INITIAL_SECONDS, ge=1, le=60
    )
    ERROR_BACKOFF_MAX_SECONDS: int = Field(
        default=DEFAULT_ERROR_BACKOFF_MAX_SECONDS, ge=1, le=3600
    )

    @model_validator(mode="after")
    def validate_backoff(self) -> "RunnerDBSettings":
        if self.ERROR_BACKOFF_INITIAL_SECONDS > self.ERROR_BACKOFF_MAX_SECONDS:
            raise ValueError(
                "ERROR_BACKOFF_INITIAL_SECONDS must be <= ERROR_BACKOFF_MAX_SECONDS"
            )
        return self


class RunnerBrokerSettings(RunnerSettings):
    """Outbox reconcile/publish cadence for ``run --mode broker``."""

    RECONCILE_LIMIT: int = Field(default=DEFAULT_RECONCILE_LIMIT, ge=1, le=1000)
    OUTBOX_LIMIT: int = Field(default=DEFAULT_OUTBOX_LIMIT, ge=1, le=1000)
    INGRESS_HANDLER_TIMEOUT_SECONDS: int = Field(
        default=DEFAULT_INGRESS_HANDLER_TIMEOUT_SECONDS, gt=0, le=3600
    )
    PING_FAILURE_LIMIT: int = Field(default=DEFAULT_PING_FAILURE_LIMIT, ge=1, le=100)
