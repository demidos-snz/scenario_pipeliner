from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import and_, case, func, literal, or_, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql.elements import ColumnElement

from scenario_pipeliner.db.schema_utils import run_with_identity_sequence_retry
from scenario_pipeliner.db.settings import CoreDb, CoreSchema
from scenario_pipeliner.db.tables import build_core_schema
from scenario_pipeliner.worker.core.enums import (
    TaskSource,
    TaskStatus,
    TaskType,
    WaitingParentCloseStatus,
    as_waiting_parent_close_status,
)
from scenario_pipeliner.worker.core.exceptions import DatabaseError
from scenario_pipeliner.worker.core.native_db_protocol import NativeTaskStorage
from scenario_pipeliner.worker.core.settings import RunnerDBSettings
from scenario_pipeliner.worker.core.states import (
    ExecutionBatchState,
    TaskPayload,
    TaskResultError,
    TaskState,
)
from scenario_pipeliner.worker.core.utils import get_params_for_cyclical_task
from scenario_pipeliner.worker.task_repositories.protocol import TaskRepository

logger = logging.getLogger(__name__)

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
_OPEN_SUBTASK_STATUSES = frozenset(
    {
        TaskStatus.NEW.value,
        TaskStatus.QUEUED.value,
        TaskStatus.RUNNING.value,
    }
)


class PostgresTaskStorage(NativeTaskStorage):
    def __init__(
        self,
        *,
        engine: AsyncEngine,
        core: CoreSchema | None = None,
        db_schema: str = "sp",
    ) -> None:
        self._engine = engine
        self._core = core or build_core_schema(db_schema)

    async def lock_runnable_tasks(
        self,
        *,
        limit: int,
        zombie_timeout_minutes: int,
    ) -> list[dict[str, Any]]:
        tasks = self._core.tasks
        settings = self._core.settings
        active_exists = (
            select(literal(1))
            .select_from(settings)
            .where(
                and_(
                    settings.c.key == func.concat("pipeline_active_", tasks.c.scenario),
                    func.btrim(settings.c.value) == "1",
                )
            )
            .exists()
        )
        worker_enabled = (
            select(literal(1))
            .select_from(settings)
            .where(
                and_(
                    settings.c.key == "worker_enabled",
                    func.btrim(settings.c.value) == "1",
                )
            )
            .exists()
        )
        new_runnable: ColumnElement[bool] = and_(
            tasks.c.status == TaskStatus.NEW.value,
            or_(tasks.c.next_run_at.is_(None), tasks.c.next_run_at <= func.now()),
            tasks.c.scenario.is_not(None),
            func.btrim(tasks.c.scenario) != "",
            active_exists,
        )
        zombie: ColumnElement[bool] = and_(
            tasks.c.status.in_((TaskStatus.QUEUED.value, TaskStatus.RUNNING.value)),
            func.coalesce(tasks.c.last_heartbeat_at, tasks.c.updated_at)
            < func.now() - func.make_interval(0, 0, 0, 0, 0, zombie_timeout_minutes),
            active_exists,
        )
        inner = (
            select(tasks.c.id)
            .where(
                and_(
                    or_(new_runnable, zombie),
                    tasks.c.deleted_at.is_(None),
                    worker_enabled,
                )
            )
            .order_by(tasks.c.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        stmt = (
            update(tasks)
            .where(tasks.c.id.in_(inner))
            .values(
                status=TaskStatus.QUEUED.value,
                updated_at=func.now(),
                last_heartbeat_at=func.now(),
            )
            .returning(
                tasks.c.id,
                tasks.c.scenario,
                tasks.c.status,
                tasks.c.source,
                tasks.c.type_task,
                tasks.c.interval_seconds,
                tasks.c.max_executions,
                tasks.c.current_executions,
                tasks.c.next_run_at,
                tasks.c.is_block,
                tasks.c.parent_id,
                tasks.c.payload,
                tasks.c.alias,
                tasks.c.steps_names,
            )
        )
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt)
            return [dict(row._mapping) for row in result]

    async def set_task_running(
        self,
        task_id: int,
        *,
        alias: str | None,
        steps_names: list[str] | None,
    ) -> None:
        tasks = self._core.tasks
        values: dict[str, Any] = {
            "status": TaskStatus.RUNNING.value,
            "updated_at": func.now(),
            "last_heartbeat_at": func.now(),
        }
        if alias:
            values["alias"] = alias
        if steps_names is not None:
            values["steps_names"] = steps_names
        async with self._engine.begin() as conn:
            await conn.execute(
                update(tasks).where(tasks.c.id == task_id).values(**values)
            )

    async def setting_is_on(self, key: str) -> bool:
        settings = self._core.settings
        try:
            async with self._engine.connect() as conn:
                value = (
                    await conn.execute(
                        select(settings.c.value).where(settings.c.key == key)
                    )
                ).scalar_one_or_none()
        except SQLAlchemyError as exc:
            raise DatabaseError("Database setting_is_on failed") from exc
        return value is not None and str(value).strip() == "1"

    async def touch_heartbeats(self, task_ids: Sequence[int]) -> None:
        if not task_ids:
            return
        tasks = self._core.tasks
        try:
            async with self._engine.begin() as conn:
                await conn.execute(
                    update(tasks)
                    .where(
                        and_(
                            tasks.c.id.in_(list(task_ids)),
                            tasks.c.status.in_(tuple(_HEARTBEAT_STATUSES)),
                        )
                    )
                    .values(last_heartbeat_at=func.now(), updated_at=func.now())
                )
        except SQLAlchemyError as exc:
            raise DatabaseError("Database touch_heartbeats failed") from exc

    async def fail_waiting_parent(self, parent_id: int) -> bool:
        tasks = self._core.tasks
        async with self._engine.begin() as conn:
            updated = (
                await conn.execute(
                    update(tasks)
                    .where(
                        and_(
                            tasks.c.id == parent_id,
                            tasks.c.status == TaskStatus.WAITING.value,
                        )
                    )
                    .values(
                        status=TaskStatus.FAILED.value,
                        updated_at=func.now(),
                    )
                    .returning(tasks.c.id)
                )
            ).first()
        return updated is not None

    async def requeue_incomplete_tasks(self, task_ids: Sequence[int]) -> int:
        if not task_ids:
            return 0
        tasks = self._core.tasks
        async with self._engine.begin() as conn:
            result = await conn.execute(
                update(tasks)
                .where(
                    and_(
                        tasks.c.id.in_(list(task_ids)),
                        tasks.c.status.not_in(tuple(_TERMINAL_SUBTASK_STATUSES)),
                    )
                )
                .values(
                    status=TaskStatus.NEW.value,
                    updated_at=func.now(),
                    last_heartbeat_at=None,
                )
                .returning(tasks.c.id)
            )
            return len(result.all())

    async def insert_result_and_update_task(
        self,
        *,
        task_id: int,
        result_json: str,
        status: TaskStatus,
        current_executions: int,
        next_run_at: datetime | None,
        payload_json: str | None,
    ) -> bool:
        from sqlalchemy import insert

        tasks = self._core.tasks
        results = self._core.results
        payload_value: Any = None
        if payload_json is not None:
            payload_value = json.loads(payload_json)
        result_value = json.loads(result_json)
        values: dict[str, Any] = {
            "status": status,
            "updated_at": func.now(),
            "current_executions": current_executions,
            "next_run_at": next_run_at,
        }
        if payload_json is not None:
            values["payload"] = payload_value

        async def _insert() -> bool:
            async with self._engine.begin() as conn:
                updated = (
                    await conn.execute(
                        update(tasks)
                        .where(
                            and_(
                                tasks.c.id == task_id,
                                tasks.c.status.not_in(
                                    tuple(_TERMINAL_SUBTASK_STATUSES)
                                ),
                            )
                        )
                        .values(**values)
                        .returning(tasks.c.id)
                    )
                ).first()
                if updated is None:
                    return False
                await conn.execute(
                    insert(results).values(task_id=task_id, result=result_value)
                )
            return True

        return await run_with_identity_sequence_retry(
            CoreDb(engine=self._engine, core=self._core),
            _insert,
        )

    async def count_parent_subtasks_state(self, parent_id: int) -> tuple[int, int]:
        tasks = self._core.tasks
        pending = func.coalesce(
            func.sum(
                case(
                    (
                        tasks.c.status.not_in(tuple(_TERMINAL_SUBTASK_STATUSES)),
                        1,
                    ),
                    else_=0,
                )
            ),
            0,
        )
        failed = func.coalesce(
            func.sum(
                case(
                    (
                        and_(
                            tasks.c.is_block.is_(False),
                            tasks.c.status.in_(tuple(_FAILED_SUBTASK_STATUSES)),
                        ),
                        1,
                    ),
                    else_=0,
                )
            ),
            0,
        )
        stmt = select(pending, failed).where(tasks.c.parent_id == parent_id)
        async with self._engine.begin() as conn:
            row = (await conn.execute(stmt)).one()
        return int(row[0] or 0), int(row[1] or 0)

    async def close_waiting_parent(
        self, parent_id: int, status: WaitingParentCloseStatus
    ) -> bool:
        status = as_waiting_parent_close_status(status)
        tasks = self._core.tasks
        values: dict[str, Any] = {
            "status": status,
            "updated_at": func.now(),
        }
        async with self._engine.begin() as conn:
            updated = (
                await conn.execute(
                    update(tasks)
                    .where(
                        and_(
                            tasks.c.id == parent_id,
                            tasks.c.status == TaskStatus.WAITING.value,
                        )
                    )
                    .values(**values)
                    .returning(tasks.c.id)
                )
            ).first()
        return updated is not None

    async def cancel_open_subtasks(self, parent_id: int) -> None:
        tasks = self._core.tasks
        async with self._engine.begin() as conn:
            await conn.execute(
                update(tasks)
                .where(
                    and_(
                        tasks.c.parent_id == parent_id,
                        tasks.c.status.in_(tuple(_OPEN_SUBTASK_STATUSES)),
                    )
                )
                .values(
                    status=TaskStatus.CANCELLED.value,
                    updated_at=func.now(),
                )
            )


class PostgresTaskRepository(TaskRepository):
    """Native Postgres implementation of task runtime repository."""

    def __init__(self, storage: NativeTaskStorage) -> None:
        self._storage = storage

    async def fetch_execution_batch(
        self,
        *,
        settings: RunnerDBSettings,
    ) -> ExecutionBatchState:
        rows = await self._storage.lock_runnable_tasks(
            limit=settings.TASKS_LIMIT,
            zombie_timeout_minutes=settings.ZOMBIE_TASKS_TIMEOUT_MINUTES,
        )
        tasks = [self._row_to_task_state(row) for row in rows]
        return ExecutionBatchState(tasks=tasks)

    async def mark_task_running(self, task: TaskState) -> None:
        steps_names: list[str] | None = None
        if task.parent_id is None and task.steps_names and task.steps_names != [""]:
            steps_names = task.steps_names
        await self._storage.set_task_running(
            task.task_id,
            alias=task.alias,
            steps_names=steps_names,
        )

    async def persist_task_result(self, task: TaskState) -> None:
        status = TaskStatus.FINISHED if task.result.ok else TaskStatus.FAILED
        await self._persist_task_with_status(task, status=status)

    async def persist_task_error(self, task: TaskState, error: Exception) -> None:
        task.result.ok = False
        task.result.error = TaskResultError(
            error_type=type(error).__name__,
            message=str(error),
        )
        status = TaskStatus.CANCELLED if task.is_cancelled else TaskStatus.FAILED
        await self._persist_task_with_status(task, status=status)

    async def persist_timeout(self, tasks: list[TaskState]) -> None:
        ids = [task.task_id for task in tasks]
        logger.warning(
            "Requeueing %s in-flight task(s) as NEW after shutdown timeout",
            len(ids),
        )
        updated = await self._storage.requeue_incomplete_tasks(ids)
        logger.info("Requeued %s task(s) as NEW (heartbeat cleared)", updated)

    async def is_worker_enabled(self) -> bool:
        return await self._storage.setting_is_on("worker_enabled")

    async def is_scenario_active(self, scenario: str) -> bool:
        return await self._storage.setting_is_on(f"pipeline_active_{scenario}")

    async def touch_heartbeats(self, task_ids: Sequence[int]) -> None:
        await self._storage.touch_heartbeats(task_ids)

    async def _persist_task_with_status(
        self, task: TaskState, *, status: TaskStatus
    ) -> None:
        original_status = status
        status, current_executions, next_run_at = get_params_for_cyclical_task(
            status, task
        )
        if task.parent_id is None and status == TaskStatus.FINISHED:
            (
                pending_open,
                failed_non_blocking,
            ) = await self._storage.count_parent_subtasks_state(task.task_id)
            if pending_open > 0:
                status = TaskStatus.WAITING
            elif failed_non_blocking > 0:
                status = TaskStatus.FINISHED_WITH_ERROR

        if (
            task.type_task == TaskType.CYCLICAL
            and status == TaskStatus.NEW
            and original_status != TaskStatus.NEW
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
        updated = await self._storage.insert_result_and_update_task(
            task_id=task.task_id,
            result_json=task.result.model_dump_json(),
            status=status,
            current_executions=current_executions,
            next_run_at=next_run_at,
            payload_json=payload_json,
        )
        if not updated:
            logger.warning(
                "Skipped result persist for task %s: status update hit 0 rows "
                "(already terminal or missing)",
                task.task_id,
            )
            return

        if task.parent_id is not None:
            if status == TaskStatus.FAILED:
                await self._handle_subtask_failure(task)
            elif status == TaskStatus.FINISHED:
                await self._handle_subtask_finished(task)

    async def _handle_subtask_failure(self, subtask: TaskState) -> None:
        if subtask.parent_id is None:
            return
        if subtask.is_block:
            await self._storage.fail_waiting_parent(subtask.parent_id)
            await self._storage.cancel_open_subtasks(subtask.parent_id)
            return
        pending, failed = await self._storage.count_parent_subtasks_state(
            subtask.parent_id
        )
        if pending == 0 and failed > 0:
            await self._storage.close_waiting_parent(
                subtask.parent_id,
                TaskStatus.FINISHED_WITH_ERROR,
            )

    async def _handle_subtask_finished(self, subtask: TaskState) -> None:
        if subtask.parent_id is None:
            return
        (
            pending_open,
            failed_non_blocking,
        ) = await self._storage.count_parent_subtasks_state(subtask.parent_id)
        if pending_open > 0:
            return
        parent_status: WaitingParentCloseStatus = (
            TaskStatus.FINISHED_WITH_ERROR
            if failed_non_blocking > 0
            else TaskStatus.FINISHED
        )
        await self._storage.close_waiting_parent(subtask.parent_id, parent_status)

    @staticmethod
    def _payload_for_retry(task: TaskState, *, status: TaskStatus) -> str | None:
        if (
            task.type_task == TaskType.CYCLICAL
            and status == TaskStatus.NEW
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
            status=PostgresTaskRepository._parse_status(row.get("status")),
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
    def _parse_status(value: TaskStatus | str | None) -> TaskStatus:
        if isinstance(value, TaskStatus):
            return value
        if isinstance(value, str):
            try:
                return TaskStatus(value)
            except ValueError:
                return TaskStatus.NEW
        return TaskStatus.NEW

    @staticmethod
    def _parse_source(value: TaskSource | str | None) -> TaskSource:
        if isinstance(value, TaskSource):
            return value
        if isinstance(value, str):
            try:
                return TaskSource(value)
            except ValueError:
                return TaskSource.INNER
        return TaskSource.INNER

    @staticmethod
    def _parse_type_task(value: TaskType | str | None) -> TaskType:
        if isinstance(value, TaskType):
            return value
        if isinstance(value, str):
            try:
                return TaskType(value)
            except ValueError:
                return TaskType.LINEAR
        return TaskType.LINEAR

    @staticmethod
    def _parse_payload(value: Any) -> TaskPayload | None:
        if value is None:
            return None
        if isinstance(value, TaskPayload):
            return value
        if isinstance(value, dict):
            return TaskPayload.model_validate(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped or stripped == "null":
                return None
            parsed: Any = json.loads(stripped)
            if parsed is None:
                return None
            if isinstance(parsed, dict):
                return TaskPayload.model_validate(parsed)
            return None
        return None
