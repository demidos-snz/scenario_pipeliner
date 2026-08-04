from __future__ import annotations

import logging
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import Any, Protocol

from scenario_pipeliner.worker.core.enums import TaskSource, TaskStatus, TaskType
from scenario_pipeliner.worker.core.native_db_protocol import NativeTaskStorage
from scenario_pipeliner.worker.core.settings import (
    RepositoryPollSettings,
    TableSettings,
)
from scenario_pipeliner.worker.core.states import (
    ExecutionBatchState,
    TaskPayload,
    TaskResultError,
    TaskState,
)
from scenario_pipeliner.worker.core.utils import get_params_for_cyclical_task
from scenario_pipeliner.worker.task_repositories.protocol import TaskRepository

logger = logging.getLogger(__name__)

_INCOMPLETE_TASK_STATUSES = frozenset(
    {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
)
_HEARTBEAT_STATUSES = frozenset({TaskStatus.QUEUED.value, TaskStatus.RUNNING.value})
_TERMINAL_SUBTASK_STATUSES = frozenset(
    {
        TaskStatus.FINISHED.value,
        TaskStatus.FAILED.value,
        TaskStatus.CANCELLED.value,
        TaskStatus.FINISHED_WITH_ERROR.value,
    }
)
_FAILED_SUBTASK_STATUSES = frozenset(
    {
        TaskStatus.FAILED.value,
        TaskStatus.FINISHED_WITH_ERROR.value,
    }
)


class PostgresTransactionProtocol(Protocol):
    async def __aenter__(self) -> object: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None: ...


class PostgresConnectionProtocol(Protocol):
    async def fetch(self, query: str, *args: Any) -> Sequence[dict[str, Any]]: ...
    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None: ...
    async def execute(self, query: str, *args: Any) -> str: ...
    def transaction(self) -> PostgresTransactionProtocol: ...


class PostgresPoolProtocol(Protocol):
    def acquire(
        self,
    ) -> AbstractAsyncContextManager[PostgresConnectionProtocol]: ...


class PostgresTaskStorage(NativeTaskStorage):
    def __init__(
        self,
        *,
        pool: PostgresPoolProtocol,
        table_settings: TableSettings | None = None,
    ) -> None:
        table_settings = table_settings or TableSettings()
        self._pool = pool
        self._tasks_table = table_settings.tasks_table
        self._results_table = table_settings.results_table
        self._settings_table = table_settings.settings_table

    async def lock_runnable_tasks(
        self,
        *,
        limit: int,
        zombie_timeout_minutes: int,
    ) -> list[dict[str, Any]]:
        query = f"""
            UPDATE {self._tasks_table}
            SET status = $1, updated_at = CURRENT_TIMESTAMP, last_heartbeat_at = CURRENT_TIMESTAMP
            WHERE id IN (
                SELECT t.id FROM {self._tasks_table} AS t
                WHERE (
                        t.status = $2
                        AND (t.next_run_at IS NULL OR t.next_run_at <= CURRENT_TIMESTAMP)
                        AND t.scenario IS NOT NULL
                        AND BTRIM(t.scenario) != ''
                        AND EXISTS (
                            SELECT 1 FROM {self._settings_table} AS s
                            WHERE s.key = 'pipeline_active_' || t.scenario
                                AND BTRIM(s.value) = '1'
                        )
                    )
                    OR (
                        t.status IN ($3, $4)
                        AND COALESCE(t.last_heartbeat_at, t.updated_at)
                            < NOW() - make_interval(mins => $5)
                    )
                ORDER BY t.id
                LIMIT $6
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, scenario, status, source, type_task, interval_seconds, max_executions,
                    current_executions, next_run_at, is_block, parent_id, payload, alias, steps_names
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                query,
                TaskStatus.QUEUED.value,
                TaskStatus.NEW.value,
                TaskStatus.QUEUED.value,
                TaskStatus.RUNNING.value,
                zombie_timeout_minutes,
                limit,
            )
        return [dict(row) for row in rows]

    async def set_task_status(self, task_id: int, status: str) -> None:
        heartbeat_sql = (
            ", last_heartbeat_at = CURRENT_TIMESTAMP"
            if status in _HEARTBEAT_STATUSES
            else ""
        )
        query = f"""
            UPDATE {self._tasks_table}
            SET status = $1, updated_at = CURRENT_TIMESTAMP{heartbeat_sql}
            WHERE id = $2
        """
        async with self._pool.acquire() as conn:
            await conn.execute(query, status, task_id)

    async def set_task_alias(self, task_id: int, alias: str) -> None:
        query = f"""
            UPDATE {self._tasks_table}
            SET alias = $1, updated_at = CURRENT_TIMESTAMP
            WHERE id = $2
        """
        async with self._pool.acquire() as conn:
            await conn.execute(query, alias, task_id)

    async def insert_result_and_update_task(
        self,
        *,
        task_id: int,
        result_json: str,
        status: str,
        current_executions: int,
        next_run_at: datetime | None,
        payload_json: str | None,
    ) -> None:
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                f"INSERT INTO {self._results_table} (task_id, result) VALUES ($1, $2)",
                task_id,
                result_json,
            )

            updates = ["status = $1", "updated_at = CURRENT_TIMESTAMP"]
            params: list[Any] = [status]
            idx = 2

            updates.append(f"current_executions = ${idx}")
            params.append(current_executions)
            idx += 1

            if next_run_at is not None:
                updates.append(f"next_run_at = ${idx}")
                params.append(next_run_at)
                idx += 1
            else:
                updates.append("next_run_at = NULL")

            if payload_json is not None:
                updates.append(f"payload = ${idx}::jsonb")
                params.append(payload_json)
                idx += 1

            params.append(task_id)
            update_query = (
                f"UPDATE {self._tasks_table} SET {', '.join(updates)} WHERE id = ${idx}"
            )
            await conn.execute(update_query, *params)

    async def count_parent_subtasks_state(self, parent_id: int) -> tuple[int, int]:
        terminal_statuses = tuple(_TERMINAL_SUBTASK_STATUSES)
        failed_statuses = tuple(_FAILED_SUBTASK_STATUSES)
        terminal_placeholders = ", ".join(
            f"${index + 1}" for index in range(len(terminal_statuses))
        )
        failed_offset = len(terminal_statuses) + 1
        failed_placeholders = ", ".join(
            f"${index + failed_offset}" for index in range(len(failed_statuses))
        )
        task_id_idx = len(terminal_statuses) + len(failed_statuses) + 1
        query = f"""
            SELECT
                SUM(CASE WHEN is_block = TRUE AND status NOT IN ({terminal_placeholders}) THEN 1 ELSE 0 END) as pending_blocking,
                SUM(CASE WHEN is_block = FALSE AND status IN ({failed_placeholders}) THEN 1 ELSE 0 END) as failed_non_blocking
            FROM {self._tasks_table}
            WHERE parent_id = ${task_id_idx}
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                *terminal_statuses,
                *failed_statuses,
                parent_id,
            )
        if row:
            pending = int(row.get("pending_blocking") or 0)
            failed = int(row.get("failed_non_blocking") or 0)
            return pending, failed
        return 0, 0

    async def cancel_open_subtasks(self, parent_id: int) -> None:
        query = f"""
            UPDATE {self._tasks_table}
            SET status = $1, updated_at = CURRENT_TIMESTAMP
            WHERE parent_id = $2 AND status IN ($3, $4)
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                query,
                TaskStatus.CANCELLED.value,
                parent_id,
                TaskStatus.NEW.value,
                TaskStatus.QUEUED.value,
            )


class PostgresTaskRepository(TaskRepository):
    """Native Postgres implementation of task runtime repository."""

    def __init__(self, storage: NativeTaskStorage) -> None:
        self._storage = storage

    async def fetch_execution_batch(
        self,
        *,
        settings: RepositoryPollSettings,
    ) -> ExecutionBatchState:
        rows = await self._storage.lock_runnable_tasks(
            limit=settings.tasks_limit,
            zombie_timeout_minutes=settings.zombie_timeout_minutes,
        )
        tasks = [self._row_to_task_state(row) for row in rows]
        return ExecutionBatchState(tasks=tasks)

    async def mark_task_running(self, task: TaskState) -> None:
        await self._storage.set_task_status(task.task_id, TaskStatus.RUNNING.value)
        if task.alias:
            await self._storage.set_task_alias(task.task_id, task.alias)

    async def persist_task_result(self, task: TaskState) -> None:
        status = (
            TaskStatus.FINISHED.value if task.result.ok else TaskStatus.FAILED.value
        )
        await self._persist_task_with_status(task, status=status)

    async def persist_task_error(self, task: TaskState, error: Exception) -> None:
        task.result.ok = False
        task.result.error = TaskResultError(
            error_type=type(error).__name__,
            message=str(error),
        )
        status = (
            TaskStatus.CANCELLED.value if task.is_cancelled else TaskStatus.FAILED.value
        )
        await self._persist_task_with_status(task, status=status)

    async def persist_timeout(self, tasks: list[TaskState]) -> None:
        for task in tasks:
            logger.warning("Task %s cancelled after shutdown timeout", task.task_id)
            task.cancel()
            task.result.ok = False
            task.result.error = TaskResultError(
                error_type="TimeoutError",
                message="Task execution timed out",
            )
            await self._persist_task_with_status(
                task, status=TaskStatus.CANCELLED.value
            )

    async def _persist_task_with_status(self, task: TaskState, *, status: str) -> None:
        original_status = status
        status, current_executions, next_run_at = get_params_for_cyclical_task(
            status, task
        )
        if task.parent_id is None and status == TaskStatus.FINISHED.value:
            (
                pending_blocking,
                failed_non_blocking,
            ) = await self._storage.count_parent_subtasks_state(task.task_id)
            if pending_blocking > 0:
                status = TaskStatus.WAITING.value
            elif failed_non_blocking > 0:
                status = TaskStatus.FINISHED_WITH_ERROR.value

        if (
            task.type_task == TaskType.CYCLICAL
            and status == TaskStatus.NEW.value
            and original_status != TaskStatus.NEW.value
        ):
            max_label = (
                str(task.max_executions) if task.max_executions is not None else "inf"
            )
            logger.info(
                "Requeued cyclical task %s as NEW (execution %s/%s, next_run_at=%s)",
                task.task_id,
                current_executions,
                max_label,
                next_run_at,
            )

        payload_json = self._payload_for_retry(task, status=status)
        await self._storage.insert_result_and_update_task(
            task_id=task.task_id,
            result_json=task.result.model_dump_json(),
            status=status,
            current_executions=current_executions,
            next_run_at=next_run_at,
            payload_json=payload_json,
        )

        if task.parent_id is not None:
            if status == TaskStatus.FAILED.value:
                await self._handle_subtask_failure(task)
            elif status == TaskStatus.FINISHED.value and task.is_block:
                await self._handle_blocking_subtask_finished(task)

    async def _handle_subtask_failure(self, subtask: TaskState) -> None:
        if subtask.parent_id is None:
            return
        if subtask.is_block:
            await self._storage.set_task_status(
                subtask.parent_id, TaskStatus.FAILED.value
            )
            await self._storage.cancel_open_subtasks(subtask.parent_id)
            return
        pending, failed = await self._storage.count_parent_subtasks_state(
            subtask.parent_id
        )
        if pending == 0 and failed > 0:
            await self._storage.set_task_status(
                subtask.parent_id,
                TaskStatus.FINISHED_WITH_ERROR.value,
            )

    async def _handle_blocking_subtask_finished(self, subtask: TaskState) -> None:
        if subtask.parent_id is None:
            return
        (
            pending_blocking,
            _failed_non_blocking,
        ) = await self._storage.count_parent_subtasks_state(subtask.parent_id)
        if pending_blocking == 0:
            await self._storage.set_task_status(
                subtask.parent_id, TaskStatus.QUEUED.value
            )

    @staticmethod
    def _payload_for_retry(task: TaskState, *, status: str) -> str | None:
        if (
            task.type_task == TaskType.CYCLICAL
            and status == TaskStatus.NEW.value
            and task.payload is not None
        ):
            return task.payload.model_dump_json(exclude_none=True)
        return None

    @staticmethod
    def _row_to_task_state(row: dict[str, Any]) -> TaskState:
        payload = PostgresTaskRepository._parse_payload(row.get("payload"))
        return TaskState(
            task_id=int(row["id"]),
            scenario=str(row.get("scenario") or ""),
            alias=row.get("alias"),
            status=str(row.get("status") or TaskStatus.NEW.value),
            source=PostgresTaskRepository._parse_source(row.get("source")),
            type_task=PostgresTaskRepository._parse_type_task(row.get("type_task")),
            interval_seconds=int(row.get("interval_seconds") or 60),
            max_executions=row.get("max_executions"),
            current_executions=int(row.get("current_executions") or 0),
            next_run_at=row.get("next_run_at"),
            is_block=bool(row.get("is_block") or False),
            parent_id=row.get("parent_id"),
            payload=payload,
            steps_names=list(row.get("steps_names") or []),
        )

    @staticmethod
    def _parse_source(value: object) -> TaskSource:
        if isinstance(value, TaskSource):
            return value
        if isinstance(value, str):
            try:
                return TaskSource(value)
            except ValueError:
                return TaskSource.INNER
        return TaskSource.INNER

    @staticmethod
    def _parse_type_task(value: object) -> TaskType:
        if isinstance(value, TaskType):
            return value
        if isinstance(value, str):
            try:
                return TaskType(value)
            except ValueError:
                return TaskType.LINEAR
        return TaskType.LINEAR

    @staticmethod
    def _parse_payload(value: object) -> TaskPayload | None:
        if value is None:
            return None
        if isinstance(value, TaskPayload):
            return value
        if isinstance(value, str):
            return TaskPayload.model_validate_json(value)
        if isinstance(value, dict):
            return TaskPayload.model_validate(value)
        return None
