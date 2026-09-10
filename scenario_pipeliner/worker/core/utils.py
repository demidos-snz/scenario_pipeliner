from __future__ import annotations

import asyncio
import datetime
from contextvars import ContextVar

from scenario_pipeliner.worker.core.enums import TaskStatus, TaskType
from scenario_pipeliner.worker.core.states import TaskState


def utc_now() -> datetime.datetime:
    """Timezone-aware UTC timestamp for ``timestamptz`` columns."""
    return datetime.datetime.now(datetime.UTC)


def get_params_for_cyclical_task(
    status: TaskStatus,
    state: TaskState,
) -> tuple[TaskStatus, int, datetime.datetime | None]:
    """Adjust status/executions for cyclical tasks.

    Incomplete iterations (not ``FINISHED``) are requeued as ``NEW`` until
    ``max_executions`` is reached. Successful ``FINISHED`` stays terminal so
    blocking subtasks can complete their parents.
    """
    current_executions: int = state.current_executions
    next_run_at: datetime.datetime | None = None

    if state.type_task == TaskType.CYCLICAL:
        current_executions += 1
        if status != TaskStatus.FINISHED and (
            state.max_executions is None or current_executions < state.max_executions
        ):
            status = TaskStatus.NEW
            delay = state.interval_seconds if state.interval_seconds is not None else 0
            next_run_at = utc_now() + datetime.timedelta(seconds=delay)

    return status, current_executions, next_run_at


_CANCEL_EVENT_CTX: ContextVar[asyncio.Event | None] = ContextVar(
    "pipeline_cancel_event",
    default=None,
)


def get_cancel_event() -> asyncio.Event | None:
    return _CANCEL_EVENT_CTX.get()
