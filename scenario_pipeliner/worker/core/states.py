import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeVar, cast

from pydantic import BaseModel, Field, field_validator

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
    scenario: str
    request_ids: list[str] = Field(default_factory=list)


class TaskPayload(BaseModel):
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
    status: str = TaskStatus.NEW.value
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
