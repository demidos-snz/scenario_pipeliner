from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import OperationalError

from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.core.enums import (
    EnumDoc,
    TaskSource,
    TaskStatus,
    TaskType,
    as_waiting_parent_close_status,
)
from scenario_pipeliner.worker.core.exceptions import DatabaseError
from scenario_pipeliner.worker.core.settings import RunnerDBSettings
from scenario_pipeliner.worker.core.states import TaskPayload, TaskState
from scenario_pipeliner.worker.runtime.runner_helpers import ensure_worker_settings
from scenario_pipeliner.worker.task_repositories import (
    PostgresTaskRepository,
    PostgresTaskStorage,
)


def _repository(core_db: CoreDb) -> PostgresTaskRepository:
    return PostgresTaskRepository(
        storage=PostgresTaskStorage(engine=core_db.engine, core=core_db.core)
    )


def _insert_task(core_db: CoreDb, **values: Any) -> int:
    payload = {
        "scenario": "scenario9",
        "status": TaskStatus.NEW.value,
        "source": TaskSource.INNER.value,
        "type_task": TaskType.LINEAR.value,
        "interval_seconds": 30,
        "current_executions": 0,
        "is_block": False,
        "steps_names": [],
    }
    payload.update(values)

    async def _run() -> int:
        async with core_db.engine.begin() as conn:
            task_id = (
                await conn.execute(
                    insert(core_db.core.tasks)
                    .values(**payload)
                    .returning(core_db.core.tasks.c.id)
                )
            ).scalar_one()
        return int(task_id)

    return asyncio.run(_run())


def _fetch_task(core_db: CoreDb, task_id: int) -> dict[str, Any]:
    async def _run() -> dict[str, Any]:
        async with core_db.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        select(core_db.core.tasks).where(
                            core_db.core.tasks.c.id == task_id
                        )
                    )
                )
                .mappings()
                .one()
            )
            return dict(row)

    return asyncio.run(_run())


def _fetch_setting(core_db: CoreDb, key: str) -> str | None:
    async def _run() -> str | None:
        async with core_db.engine.connect() as conn:
            value = (
                await conn.execute(
                    select(core_db.core.settings.c.value).where(
                        core_db.core.settings.c.key == key
                    )
                )
            ).scalar_one_or_none()
        return str(value) if value is not None else None

    return asyncio.run(_run())


def _count_results(core_db: CoreDb, task_id: int) -> int:
    async def _run() -> int:
        async with core_db.engine.connect() as conn:
            count = (
                await conn.execute(
                    select(func.count())
                    .select_from(core_db.core.results)
                    .where(core_db.core.results.c.task_id == task_id)
                )
            ).scalar_one()
        return int(count or 0)

    return asyncio.run(_run())


def _set_setting(core_db: CoreDb, key: str, value: str) -> None:
    settings = core_db.core.settings

    async def _run() -> None:
        async with core_db.engine.begin() as conn:
            await conn.execute(
                pg_insert(settings)
                .values(key=key, value=value)
                .on_conflict_do_update(
                    index_elements=[settings.c.key],
                    set_={"value": value},
                )
            )

    asyncio.run(_run())


def test_ensure_worker_settings_inserts_missing_flags(core_db: CoreDb) -> None:
    scenario = "scenario9"
    key = f"pipeline_active_{scenario}"
    assert _fetch_setting(core_db, key) is None
    asyncio.run(ensure_worker_settings(db=core_db, scenario=scenario))
    assert _fetch_setting(core_db, key) == "1"
    assert _fetch_setting(core_db, "worker_enabled") == "1"


def test_ensure_worker_settings_keeps_kill_switch(core_db: CoreDb) -> None:
    scenario = "scenario9"
    key = f"pipeline_active_{scenario}"
    _set_setting(core_db, key, "0")
    _set_setting(core_db, "worker_enabled", "0")
    asyncio.run(ensure_worker_settings(db=core_db, scenario=scenario))
    assert _fetch_setting(core_db, key) == "0"
    assert _fetch_setting(core_db, "worker_enabled") == "0"


def test_as_waiting_parent_close_status_rejects_invalid() -> None:
    assert as_waiting_parent_close_status(TaskStatus.FINISHED) is TaskStatus.FINISHED
    assert (
        as_waiting_parent_close_status(TaskStatus.FINISHED_WITH_ERROR)
        is TaskStatus.FINISHED_WITH_ERROR
    )
    with pytest.raises(ValueError, match="FINISHED_WITH_ERROR"):
        as_waiting_parent_close_status(TaskStatus.FAILED)


def test_parse_status_coerces_and_falls_back() -> None:
    assert (
        PostgresTaskRepository._parse_status(TaskStatus.WAITING) is TaskStatus.WAITING
    )
    assert PostgresTaskRepository._parse_status("RUNNING") is TaskStatus.RUNNING
    assert PostgresTaskRepository._parse_status("nope") is TaskStatus.NEW
    assert PostgresTaskRepository._parse_status(None) is TaskStatus.NEW


def test_parse_payload_accepts_json_null_and_empty() -> None:
    assert PostgresTaskRepository._parse_payload(None) is None
    assert PostgresTaskRepository._parse_payload("null") is None
    assert PostgresTaskRepository._parse_payload("  null  ") is None
    assert PostgresTaskRepository._parse_payload("") is None
    assert PostgresTaskRepository._parse_payload("   ") is None
    payload = PostgresTaskRepository._parse_payload('{"type_doc":"JSON","data":["{}"]}')
    assert payload is not None
    assert payload.primary == "{}"
    stripped = PostgresTaskRepository._parse_payload(
        {"type_doc": "JSON", "data": ["x"], "order_id": "42"}
    )
    assert stripped is not None
    assert stripped.data == ["x"]
    assert "order_id" not in stripped.model_dump()


def test_postgres_repository_fetch_batch_maps_rows(core_db: CoreDb) -> None:
    asyncio.run(ensure_worker_settings(db=core_db, scenario="scenario9"))
    task_id = _insert_task(
        core_db,
        payload={"type_doc": "JSON", "data": ["{}"]},
        alias="demo",
        steps_names=["step_a"],
    )
    repository = _repository(core_db)

    state = asyncio.run(
        repository.fetch_execution_batch(
            settings=RunnerDBSettings(TASKS_LIMIT=5, ZOMBIE_TASKS_TIMEOUT_MINUTES=9)
        )
    )

    assert len(state.tasks) == 1
    task = state.tasks[0]
    assert task.task_id == task_id
    assert task.status is TaskStatus.QUEUED
    assert task.steps_names == ["step_a"]
    assert task.payload is not None
    assert task.payload.primary == "{}"
    assert _fetch_task(core_db, task_id)["status"] == TaskStatus.QUEUED.value


def test_postgres_repository_marks_running_and_alias(core_db: CoreDb) -> None:
    task_id = _insert_task(core_db)
    repository = _repository(core_db)
    task = TaskState(task_id=task_id, scenario="scenario9", alias="worker")

    asyncio.run(repository.mark_task_running(task))

    row = _fetch_task(core_db, task_id)
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["alias"] == "worker"


def test_postgres_repository_marks_running_and_steps_names(core_db: CoreDb) -> None:
    task_id = _insert_task(core_db)
    repository = _repository(core_db)
    task = TaskState(
        task_id=task_id,
        scenario="scenario9",
        steps_names=["updatetasktorunningstep", "ezzcreatedraftmessagestep"],
    )

    asyncio.run(repository.mark_task_running(task))

    row = _fetch_task(core_db, task_id)
    assert row["status"] == TaskStatus.RUNNING.value
    assert list(row["steps_names"]) == [
        "updatetasktorunningstep",
        "ezzcreatedraftmessagestep",
    ]


def test_postgres_repository_skips_steps_names_for_subtasks(core_db: CoreDb) -> None:
    parent_id = _insert_task(core_db)
    task_id = _insert_task(core_db, parent_id=parent_id, steps_names=["kept"])
    repository = _repository(core_db)
    task = TaskState(
        task_id=task_id,
        scenario="scenario9",
        parent_id=parent_id,
        steps_names=["trackdocumentstatusstep"],
    )

    asyncio.run(repository.mark_task_running(task))

    row = _fetch_task(core_db, task_id)
    assert row["status"] == TaskStatus.RUNNING.value
    assert list(row["steps_names"]) == ["kept"]


def test_postgres_repository_persists_cyclical_retry(core_db: CoreDb) -> None:
    task_id = _insert_task(
        core_db,
        type_task=TaskType.CYCLICAL.value,
        interval_seconds=60,
        max_executions=3,
        current_executions=0,
        payload={"type_doc": "JSON", "data": ["{}"]},
    )
    repository = _repository(core_db)
    task = TaskState(
        task_id=task_id,
        scenario="scenario9",
        type_task=TaskType.CYCLICAL,
        interval_seconds=60,
        max_executions=3,
        current_executions=0,
        payload=TaskPayload(type_doc=EnumDoc.JSON, data=["{}"]),
    )
    task.result.ok = False

    asyncio.run(repository.persist_task_result(task))

    row = _fetch_task(core_db, task_id)
    assert row["status"] == TaskStatus.NEW.value
    assert row["current_executions"] == 1
    assert row["next_run_at"] is not None


def test_postgres_repository_persists_cyclical_success_as_finished(
    core_db: CoreDb,
) -> None:
    task_id = _insert_task(
        core_db,
        type_task=TaskType.CYCLICAL.value,
        interval_seconds=30,
        max_executions=None,
        current_executions=2,
        payload={"type_doc": "JSON", "data": ['{"cursor":1}']},
    )
    repository = _repository(core_db)
    task = TaskState(
        task_id=task_id,
        scenario="scenario9",
        type_task=TaskType.CYCLICAL,
        interval_seconds=30,
        max_executions=None,
        current_executions=2,
        payload=TaskPayload(type_doc=EnumDoc.JSON, data=['{"cursor":1}']),
    )
    task.result.ok = True

    asyncio.run(repository.persist_task_result(task))

    row = _fetch_task(core_db, task_id)
    assert row["status"] == TaskStatus.FINISHED.value
    assert row["current_executions"] == 3


def test_count_parent_subtasks_state(core_db: CoreDb) -> None:
    parent_id = _insert_task(core_db)
    _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=True,
        status=TaskStatus.RUNNING.value,
    )
    _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=False,
        status=TaskStatus.FAILED.value,
    )
    storage = PostgresTaskStorage(engine=core_db.engine, core=core_db.core)

    pending, failed = asyncio.run(storage.count_parent_subtasks_state(parent_id))

    assert pending == 1
    assert failed == 1


def test_postgres_repository_persists_cyclical_success_at_max_executions(
    core_db: CoreDb,
) -> None:
    task_id = _insert_task(
        core_db,
        type_task=TaskType.CYCLICAL.value,
        interval_seconds=30,
        max_executions=3,
        current_executions=2,
    )
    repository = _repository(core_db)
    task = TaskState(
        task_id=task_id,
        scenario="scenario9",
        type_task=TaskType.CYCLICAL,
        interval_seconds=30,
        max_executions=3,
        current_executions=2,
    )
    task.result.ok = True

    asyncio.run(repository.persist_task_result(task))

    row = _fetch_task(core_db, task_id)
    assert row["status"] == TaskStatus.FINISHED.value
    assert row["current_executions"] == 3


def test_zombie_queued_task_is_relocked(core_db: CoreDb) -> None:
    asyncio.run(ensure_worker_settings(db=core_db, scenario="scenario9"))
    task_id = _insert_task(core_db, status=TaskStatus.QUEUED.value)

    async def _age() -> None:
        stale = func.now() - text("INTERVAL '20 minutes'")
        async with core_db.engine.begin() as conn:
            await conn.execute(
                update(core_db.core.tasks)
                .where(core_db.core.tasks.c.id == task_id)
                .values(last_heartbeat_at=stale, updated_at=stale)
            )

    asyncio.run(_age())
    repository = _repository(core_db)
    state = asyncio.run(
        repository.fetch_execution_batch(
            settings=RunnerDBSettings(TASKS_LIMIT=5, ZOMBIE_TASKS_TIMEOUT_MINUTES=9)
        )
    )
    assert [task.task_id for task in state.tasks] == [task_id]


def test_deleted_task_is_not_locked(core_db: CoreDb) -> None:
    asyncio.run(ensure_worker_settings(db=core_db, scenario="scenario9"))
    live_id = _insert_task(core_db)
    deleted_id = _insert_task(core_db)

    async def _soft_delete() -> None:
        async with core_db.engine.begin() as conn:
            await conn.execute(
                update(core_db.core.tasks)
                .where(core_db.core.tasks.c.id == deleted_id)
                .values(deleted_at=func.now())
            )

    asyncio.run(_soft_delete())
    repository = _repository(core_db)
    state = asyncio.run(
        repository.fetch_execution_batch(
            settings=RunnerDBSettings(TASKS_LIMIT=5, ZOMBIE_TASKS_TIMEOUT_MINUTES=9)
        )
    )
    assert [task.task_id for task in state.tasks] == [live_id]


def test_deleted_zombie_task_is_not_relocked(core_db: CoreDb) -> None:
    asyncio.run(ensure_worker_settings(db=core_db, scenario="scenario9"))
    task_id = _insert_task(core_db, status=TaskStatus.QUEUED.value)

    async def _age_and_delete() -> None:
        stale = func.now() - text("INTERVAL '20 minutes'")
        async with core_db.engine.begin() as conn:
            await conn.execute(
                update(core_db.core.tasks)
                .where(core_db.core.tasks.c.id == task_id)
                .values(
                    last_heartbeat_at=stale,
                    updated_at=stale,
                    deleted_at=func.now(),
                )
            )

    asyncio.run(_age_and_delete())
    repository = _repository(core_db)
    state = asyncio.run(
        repository.fetch_execution_batch(
            settings=RunnerDBSettings(TASKS_LIMIT=5, ZOMBIE_TASKS_TIMEOUT_MINUTES=9)
        )
    )
    assert state.tasks == []


def _persist_subtask(
    repository: PostgresTaskRepository,
    *,
    task_id: int,
    parent_id: int,
    is_block: bool,
    ok: bool,
) -> None:
    task = TaskState(
        task_id=task_id,
        scenario="scenario9",
        parent_id=parent_id,
        is_block=is_block,
        status=TaskStatus.RUNNING,
    )
    task.result.ok = ok
    asyncio.run(repository.persist_task_result(task))


def test_blocking_failure_does_not_finish_failed_parent(core_db: CoreDb) -> None:
    parent_id = _insert_task(core_db, status=TaskStatus.WAITING.value)
    failed_id = _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=True,
        status=TaskStatus.RUNNING.value,
    )
    sibling_id = _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=True,
        status=TaskStatus.RUNNING.value,
    )
    repository = _repository(core_db)

    _persist_subtask(
        repository, task_id=failed_id, parent_id=parent_id, is_block=True, ok=False
    )
    assert _fetch_task(core_db, parent_id)["status"] == TaskStatus.FAILED.value
    assert _fetch_task(core_db, sibling_id)["status"] == TaskStatus.CANCELLED.value

    _persist_subtask(
        repository, task_id=sibling_id, parent_id=parent_id, is_block=True, ok=True
    )
    assert _fetch_task(core_db, parent_id)["status"] == TaskStatus.FAILED.value
    assert _fetch_task(core_db, sibling_id)["status"] == TaskStatus.CANCELLED.value


def test_last_blocking_success_closes_waiting_parent(core_db: CoreDb) -> None:
    parent_id = _insert_task(core_db, status=TaskStatus.WAITING.value)
    child_id = _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=True,
        status=TaskStatus.RUNNING.value,
    )
    repository = _repository(core_db)

    _persist_subtask(
        repository, task_id=child_id, parent_id=parent_id, is_block=True, ok=True
    )

    assert _fetch_task(core_db, parent_id)["status"] == TaskStatus.FINISHED.value
    assert _fetch_task(core_db, child_id)["status"] == TaskStatus.FINISHED.value


def test_last_blocking_success_keeps_failed_parent(core_db: CoreDb) -> None:
    parent_id = _insert_task(core_db, status=TaskStatus.FAILED.value)
    child_id = _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=True,
        status=TaskStatus.RUNNING.value,
    )
    repository = _repository(core_db)

    _persist_subtask(
        repository, task_id=child_id, parent_id=parent_id, is_block=True, ok=True
    )

    assert _fetch_task(core_db, parent_id)["status"] == TaskStatus.FAILED.value
    assert _fetch_task(core_db, child_id)["status"] == TaskStatus.FINISHED.value


def test_last_blocking_success_closes_waiting_parent_with_error(
    core_db: CoreDb,
) -> None:
    parent_id = _insert_task(core_db, status=TaskStatus.WAITING.value)
    child_id = _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=True,
        status=TaskStatus.RUNNING.value,
    )
    _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=False,
        status=TaskStatus.FAILED.value,
    )
    repository = _repository(core_db)

    _persist_subtask(
        repository, task_id=child_id, parent_id=parent_id, is_block=True, ok=True
    )

    assert (
        _fetch_task(core_db, parent_id)["status"]
        == TaskStatus.FINISHED_WITH_ERROR.value
    )


def test_cancel_open_subtasks_includes_running(core_db: CoreDb) -> None:
    parent_id = _insert_task(core_db, status=TaskStatus.FAILED.value)
    new_id = _insert_task(core_db, parent_id=parent_id, status=TaskStatus.NEW.value)
    queued_id = _insert_task(
        core_db, parent_id=parent_id, status=TaskStatus.QUEUED.value
    )
    running_id = _insert_task(
        core_db, parent_id=parent_id, status=TaskStatus.RUNNING.value
    )
    finished_id = _insert_task(
        core_db, parent_id=parent_id, status=TaskStatus.FINISHED.value
    )
    storage = PostgresTaskStorage(engine=core_db.engine, core=core_db.core)

    asyncio.run(storage.cancel_open_subtasks(parent_id))

    assert _fetch_task(core_db, new_id)["status"] == TaskStatus.CANCELLED.value
    assert _fetch_task(core_db, queued_id)["status"] == TaskStatus.CANCELLED.value
    assert _fetch_task(core_db, running_id)["status"] == TaskStatus.CANCELLED.value
    assert _fetch_task(core_db, finished_id)["status"] == TaskStatus.FINISHED.value


def test_worker_disabled_skips_lock(core_db: CoreDb) -> None:
    asyncio.run(ensure_worker_settings(db=core_db, scenario="scenario9"))
    _set_setting(core_db, "worker_enabled", "0")
    _insert_task(core_db)
    repository = _repository(core_db)
    state = asyncio.run(
        repository.fetch_execution_batch(
            settings=RunnerDBSettings(TASKS_LIMIT=5, ZOMBIE_TASKS_TIMEOUT_MINUTES=9)
        )
    )
    assert state.tasks == []


def test_heartbeat_prevents_zombie_relock(core_db: CoreDb) -> None:
    asyncio.run(ensure_worker_settings(db=core_db, scenario="scenario9"))
    task_id = _insert_task(core_db, status=TaskStatus.RUNNING.value)
    storage = PostgresTaskStorage(engine=core_db.engine, core=core_db.core)

    async def _stale_then_touch() -> None:
        stale = func.now() - text("INTERVAL '20 minutes'")
        async with core_db.engine.begin() as conn:
            await conn.execute(
                update(core_db.core.tasks)
                .where(core_db.core.tasks.c.id == task_id)
                .values(last_heartbeat_at=stale, updated_at=stale)
            )
        await storage.touch_heartbeats([task_id])

    asyncio.run(_stale_then_touch())
    repository = _repository(core_db)
    state = asyncio.run(
        repository.fetch_execution_batch(
            settings=RunnerDBSettings(TASKS_LIMIT=5, ZOMBIE_TASKS_TIMEOUT_MINUTES=9)
        )
    )
    assert state.tasks == []
    row = _fetch_task(core_db, task_id)
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["last_heartbeat_at"] is not None


def test_result_not_inserted_when_status_already_terminal(
    core_db: CoreDb, caplog: pytest.LogCaptureFixture
) -> None:
    task_id = _insert_task(core_db, status=TaskStatus.FINISHED.value)
    repository = _repository(core_db)
    task = TaskState(task_id=task_id, scenario="scenario9")
    task.result.ok = False
    with caplog.at_level(logging.WARNING):
        asyncio.run(repository.persist_task_result(task))
    assert _count_results(core_db, task_id) == 0
    assert _fetch_task(core_db, task_id)["status"] == TaskStatus.FINISHED.value
    assert f"Skipped result persist for task {task_id}" in caplog.text


def test_persist_timeout_requeues_as_new(core_db: CoreDb) -> None:
    task_id = _insert_task(core_db, status=TaskStatus.RUNNING.value)
    repository = _repository(core_db)
    task = TaskState(task_id=task_id, scenario="scenario9", status=TaskStatus.RUNNING)
    asyncio.run(repository.persist_timeout([task]))
    row = _fetch_task(core_db, task_id)
    assert row["status"] == TaskStatus.NEW.value
    assert row["last_heartbeat_at"] is None
    assert _count_results(core_db, task_id) == 0


def test_blocking_failure_does_not_fail_running_parent(core_db: CoreDb) -> None:
    parent_id = _insert_task(core_db, status=TaskStatus.RUNNING.value)
    failed_id = _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=True,
        status=TaskStatus.RUNNING.value,
    )
    repository = _repository(core_db)
    _persist_subtask(
        repository, task_id=failed_id, parent_id=parent_id, is_block=True, ok=False
    )
    assert _fetch_task(core_db, parent_id)["status"] == TaskStatus.RUNNING.value
    assert _fetch_task(core_db, failed_id)["status"] == TaskStatus.FAILED.value


def test_parent_waits_for_non_blocking_children(core_db: CoreDb) -> None:
    parent_id = _insert_task(core_db, status=TaskStatus.WAITING.value)
    blocking_id = _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=True,
        status=TaskStatus.RUNNING.value,
    )
    tracker_id = _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=False,
        status=TaskStatus.RUNNING.value,
    )
    repository = _repository(core_db)
    _persist_subtask(
        repository, task_id=blocking_id, parent_id=parent_id, is_block=True, ok=True
    )
    assert _fetch_task(core_db, parent_id)["status"] == TaskStatus.WAITING.value
    _persist_subtask(
        repository, task_id=tracker_id, parent_id=parent_id, is_block=False, ok=True
    )
    assert _fetch_task(core_db, parent_id)["status"] == TaskStatus.FINISHED.value


def test_root_finished_waits_for_open_non_blocking_child(core_db: CoreDb) -> None:
    parent_id = _insert_task(core_db, status=TaskStatus.RUNNING.value)
    _insert_task(
        core_db,
        parent_id=parent_id,
        is_block=False,
        status=TaskStatus.RUNNING.value,
    )
    repository = _repository(core_db)
    parent = TaskState(task_id=parent_id, scenario="scenario9")
    parent.result.ok = True
    asyncio.run(repository.persist_task_result(parent))
    assert _fetch_task(core_db, parent_id)["status"] == TaskStatus.WAITING.value
    assert _count_results(core_db, parent_id) == 1


class _FailingConnect:
    async def __aenter__(self) -> None:
        raise OperationalError("SELECT 1", {}, Exception("connection refused"))

    async def __aexit__(self, *args: object) -> bool:
        return False


class _FailingEngine:
    def connect(self) -> _FailingConnect:
        return _FailingConnect()

    def begin(self) -> _FailingConnect:
        return _FailingConnect()


def test_setting_is_on_wraps_sqlalchemy_error() -> None:
    storage = PostgresTaskStorage(engine=_FailingEngine(), db_schema="sp")  # type: ignore[arg-type]
    with pytest.raises(DatabaseError, match="setting_is_on"):
        asyncio.run(storage.setting_is_on("worker_enabled"))


def test_touch_heartbeats_wraps_sqlalchemy_error() -> None:
    storage = PostgresTaskStorage(engine=_FailingEngine(), db_schema="sp")  # type: ignore[arg-type]
    with pytest.raises(DatabaseError, match="touch_heartbeats"):
        asyncio.run(storage.touch_heartbeats([1]))


def test_pipeline_active_default_is_not_seeded(core_db: CoreDb) -> None:
    assert _fetch_setting(core_db, "pipeline_active_default") is None
