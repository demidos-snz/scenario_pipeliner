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

    Successful iterations (``FINISHED``) are requeued as ``NEW`` until
    ``max_executions`` is reached. The iteration that hits the cap stays
    ``FINISHED``. ``FAILED`` and ``CANCELLED`` stay terminal and do not requeue.
    """
    current_executions: int = state.current_executions
    next_run_at: datetime.datetime | None = None

    if state.type_task == TaskType.CYCLICAL:
        current_executions += 1
        under_cap = (
            state.max_executions is None or current_executions < state.max_executions
        )
        if status == TaskStatus.FINISHED and under_cap:
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
