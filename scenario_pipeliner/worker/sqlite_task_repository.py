from __future__ import annotations

from scenario_pipeliner.worker.core.settings import RepositoryPollSettings
from scenario_pipeliner.worker.core.states import ExecutionBatchState, TaskState
from scenario_pipeliner.worker.core.task_repository import TaskRepository


class SQLiteTaskRepositoryStub(TaskRepository):
    """TODO(techdebt): native SQLite repository implementation."""

    _MESSAGE = (
        "SQLiteTaskRepository is not implemented yet; "
        "use PostgresTaskRepository or legacy adapter as temporary fallback."
    )

    async def fetch_execution_batch(
        self,
        *,
        settings: RepositoryPollSettings,
    ) -> ExecutionBatchState:
        raise NotImplementedError(self._MESSAGE)

    async def mark_task_running(self, task: TaskState) -> None:
        raise NotImplementedError(self._MESSAGE)

    async def persist_task_result(self, task: TaskState) -> None:
        raise NotImplementedError(self._MESSAGE)

    async def persist_task_error(self, task: TaskState, error: Exception) -> None:
        raise NotImplementedError(self._MESSAGE)

    async def persist_timeout(self, tasks: list[TaskState]) -> None:
        raise NotImplementedError(self._MESSAGE)
