import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeVar, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator

from scenario_pipeliner.worker.core.enums import (
    EnumDoc,
    TaskSource,
    TaskStatus,
    TaskType,
)


class TaskResultError(BaseModel):
    error_type: str
    message: str


class TaskResult(BaseModel):
    ok: bool = False
    message: dict[str, Any] = Field(default_factory=dict)
    error: TaskResultError | None = None


class TaskPayloadParams(BaseModel):
    """Opaque plugin metadata bag.

    The library stores and round-trips JSON object keys as-is and never reads
    them. Declare plugin-specific fields by subclassing, or pass a dict.
    """

    model_config = ConfigDict(extra="allow")


class TaskPayload(BaseModel):
    """JSON in ``tasks.payload``, ``TaskDraft.payload``, and ``TaskState.payload``.

    The library validates and persists these fields. It does not branch on
    ``type_doc`` or ``params``. Plugin-specific values go in ``data`` / ``params``;
    sibling JSON keys on ``TaskPayload`` itself are ignored (``extra=ignore``).
    """

    type_doc: EnumDoc = EnumDoc.XML
    data: list[str] = Field(default_factory=list)
    params: TaskPayloadParams | None = None

    @field_validator("data", mode="before")
    @classmethod
    def coerce_legacy_str_data(cls, value: Any) -> list[str]:
        if isinstance(value, str):
            return [value] if value else []
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value]
        return cast(list[str], value)

    @property
    def primary(self) -> str:
        return self.data[0] if self.data else ""


@dataclass
class BaseState:
    cancel_event: asyncio.Event = field(
        default_factory=asyncio.Event, repr=False, init=False
    )

    @property
    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def cancel(self) -> None:
        self.cancel_event.set()


@dataclass
class TaskState(BaseState):
    task_id: int
    scenario: str = ""
    alias: str | None = None
    status: TaskStatus = TaskStatus.NEW
    source: TaskSource = TaskSource.INNER
    type_task: TaskType = TaskType.LINEAR
    interval_seconds: int = 60
    max_executions: int | None = 3
    current_executions: int = 0
    next_run_at: datetime | None = None
    is_block: bool = False
    parent_id: int | None = None
    payload: TaskPayload | None = field(default_factory=TaskPayload)
    result: TaskResult = field(default_factory=TaskResult)
    steps_names: list[str] = field(default_factory=list)


@dataclass
class ExecutionBatchState(BaseState):
    tasks: list[TaskState] = field(default_factory=list)


TState = TypeVar("TState", bound=BaseState)
TTaskState = TypeVar("TTaskState", bound=TaskState)
