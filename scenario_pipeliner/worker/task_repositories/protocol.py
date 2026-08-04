from __future__ import annotations

from typing import Protocol

from scenario_pipeliner.worker.core.settings import RepositoryPollSettings
from scenario_pipeliner.worker.core.states import ExecutionBatchState, TaskState


class TaskRepository(Protocol):
    """Persistence boundary for DB runner polling and task transitions."""

    async def fetch_execution_batch(
        self,
        *,
        settings: RepositoryPollSettings,
    ) -> ExecutionBatchState:
        """Fetch and lock next runnable tasks batch."""

    async def mark_task_running(self, task: TaskState) -> None:
        """Persist RUNNING transition before pipeline execution."""

    async def persist_task_result(self, task: TaskState) -> None:
        """Persist successful/failed execution result and final status."""

    async def persist_task_error(self, task: TaskState, error: Exception) -> None:
        """Persist unhandled execution error for a task."""

    async def persist_timeout(self, tasks: list[TaskState]) -> None:
        """Persist timeout/cancel transitions for timed out batch tasks."""
