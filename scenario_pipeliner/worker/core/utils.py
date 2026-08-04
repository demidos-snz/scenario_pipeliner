from __future__ import annotations

import asyncio
import datetime
from contextvars import ContextVar

from scenario_pipeliner.worker.core.enums import TaskStatus, TaskType
from scenario_pipeliner.worker.core.states import TaskState


def get_params_for_cyclical_task(
    status: str,
    state: TaskState,
) -> tuple[str, int, datetime.datetime | None]:
    current_executions: int = state.current_executions
    next_run_at: datetime.datetime | None = None

    if state.type_task == TaskType.CYCLICAL:
        current_executions += 1
        if state.max_executions is None or current_executions < state.max_executions:
            status = TaskStatus.NEW.value
            if state.interval_seconds is not None:
                next_run_at = datetime.datetime.now() + datetime.timedelta(
                    seconds=state.interval_seconds
                )
            else:
                next_run_at = datetime.datetime.now()

    return status, current_executions, next_run_at


_CANCEL_EVENT_CTX: ContextVar[asyncio.Event | None] = ContextVar(
    "pipeline_cancel_event",
    default=None,
)


def get_cancel_event() -> asyncio.Event | None:
    return _CANCEL_EVENT_CTX.get()
