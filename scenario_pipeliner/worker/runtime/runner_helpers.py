from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from scenario_pipeliner.api.settings import ScenarioPipelinerConfig
from scenario_pipeliner.db.schema_utils import run_with_identity_sequence_retry
from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.core.enums import TaskSource, TaskStatus, TaskType
from scenario_pipeliner.worker.core.settings import RunnerDBSettings
from scenario_pipeliner.worker.core.states import TaskPayload
from scenario_pipeliner.worker.execution.runner_db import RunnerDB
from scenario_pipeliner.worker.plugin_registry import MainPipelinePluginRegistry
from scenario_pipeliner.worker.runtime.factories import (
    create_runner_db_from_config_with_native_repository,
)


@dataclass(frozen=True, slots=True)
class TaskSeed:
    scenario: str
    type_task: TaskType = TaskType.LINEAR
    interval_seconds: int = 1
    max_executions: int | None = None
    alias: str | None = None
    steps_names: tuple[str, ...] = ()
    payload: dict[str, Any] | None = None
    source: TaskSource = TaskSource.INNER


@dataclass(frozen=True, slots=True)
class CyclicalTaskSeed:
    scenario: str
    interval_seconds: int = 1
    max_executions: int | None = 3
    alias: str | None = None
    steps_names: tuple[str, ...] = ()
    payload: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class TaskRuntimeSnapshot:
    task_id: int
    status: TaskStatus
    current_executions: int
    max_executions: int | None
    next_run_at: datetime | None
    results_count: int


async def ensure_worker_settings(
    *,
    db: CoreDb,
    scenario: str,
) -> None:
    """Insert default active flags when missing; never overwrite existing values."""
    settings = db.core.settings
    async with db.engine.begin() as conn:
        for key, value in (
            (f"pipeline_active_{scenario}", "1"),
            ("worker_enabled", "1"),
        ):
            await conn.execute(
                pg_insert(settings)
                .values(key=key, value=value)
                .on_conflict_do_nothing(index_elements=[settings.c.key])
            )


async def create_task(
    *,
    db: CoreDb,
    seed: TaskSeed,
) -> int:
    tasks = db.core.tasks
    alias = seed.alias or f"task-{seed.scenario}"
    payload = TaskPayload.model_validate(seed.payload or {}).model_dump(mode="json")

    async def _insert() -> int:
        async with db.engine.begin() as conn:
            task_id = (
                await conn.execute(
                    insert(tasks)
                    .values(
                        scenario=seed.scenario,
                        status=TaskStatus.NEW.value,
                        source=seed.source.value,
                        type_task=seed.type_task.value,
                        interval_seconds=seed.interval_seconds,
                        max_executions=seed.max_executions,
                        current_executions=0,
                        payload=payload,
                        alias=alias,
                        steps_names=list(seed.steps_names),
                    )
                    .returning(tasks.c.id)
                )
            ).scalar_one()
        return int(task_id)

    await ensure_worker_settings(db=db, scenario=seed.scenario)
    return await run_with_identity_sequence_retry(db, _insert)


async def create_cyclical_task(
    *,
    db: CoreDb,
    seed: CyclicalTaskSeed,
) -> int:
    return await create_task(
        db=db,
        seed=TaskSeed(
            scenario=seed.scenario,
            type_task=TaskType.CYCLICAL,
            interval_seconds=seed.interval_seconds,
            max_executions=seed.max_executions,
            alias=seed.alias,
            steps_names=seed.steps_names,
            payload=seed.payload,
        ),
    )


def build_postgres_runner(
    *,
    config: ScenarioPipelinerConfig,
    db: CoreDb,
    runner_settings: RunnerDBSettings | None = None,
    plugin_services: dict[str, Any] | None = None,
    plugin_registry: MainPipelinePluginRegistry | None = None,
) -> RunnerDB:
    return create_runner_db_from_config_with_native_repository(
        config=config,
        db=db,
        plugin_services=plugin_services,
        plugin_registry=plugin_registry,
        runner_settings=runner_settings,
    )


async def fetch_task_snapshot(
    *,
    db: CoreDb,
    task_id: int,
) -> TaskRuntimeSnapshot | None:
    tasks = db.core.tasks
    results = db.core.results
    async with db.engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    select(
                        tasks.c.id,
                        tasks.c.status,
                        tasks.c.current_executions,
                        tasks.c.max_executions,
                        tasks.c.next_run_at,
                    ).where(tasks.c.id == task_id)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        results_count = (
            await conn.execute(
                select(func.count())
                .select_from(results)
                .where(results.c.task_id == task_id)
            )
        ).scalar_one()
    status_raw = str(row["status"] or TaskStatus.NEW.value)
    try:
        status = TaskStatus(status_raw)
    except ValueError:
        status = TaskStatus.NEW
    return TaskRuntimeSnapshot(
        task_id=int(row["id"]),
        status=status,
        current_executions=int(row["current_executions"] or 0),
        max_executions=row["max_executions"],
        next_run_at=row["next_run_at"],
        results_count=int(results_count or 0),
    )
