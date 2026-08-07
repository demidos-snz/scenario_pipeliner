from __future__ import annotations

import asyncio
from typing import Any

from scenario_pipeliner.worker.core.enums import EnumDoc, TaskType
from scenario_pipeliner.worker.core.settings import RepositoryPollSettings
from scenario_pipeliner.worker.core.states import TaskPayload, TaskState
from scenario_pipeliner.worker.task_repositories import (
    PostgresTaskRepository,
    PostgresTaskStorage,
)


class _FakeTransaction:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        return


class _FakeConnection:
    def __init__(self) -> None:
        self.fetch_rows: list[dict[str, Any]] = []
        self.fetchrow_result: dict[str, Any] | None = None
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.executed.append((query, args))
        return self.fetch_rows

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.executed.append((query, args))
        return self.fetchrow_result

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append((query, args))
        return "OK"

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()


class _FakeAcquireCtx:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConnection:
        return self._conn

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        return


class _FakePool:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquireCtx:
        return _FakeAcquireCtx(self._conn)


def test_parse_payload_accepts_json_null_and_empty() -> None:
    assert PostgresTaskRepository._parse_payload(None) is None
    assert PostgresTaskRepository._parse_payload("null") is None
    assert PostgresTaskRepository._parse_payload("  null  ") is None
    assert PostgresTaskRepository._parse_payload("") is None
    assert PostgresTaskRepository._parse_payload("   ") is None
    payload = PostgresTaskRepository._parse_payload('{"type_doc":"JSON","data":["{}"]}')
    assert payload is not None
    assert payload.primary == "{}"


def test_postgres_repository_fetch_batch_maps_rows() -> None:
    conn = _FakeConnection()
    conn.fetch_rows = [
        {
            "id": 42,
            "scenario": "scenario9",
            "status": "QUEUED",
            "source": "INNER",
            "type_task": "LINEAR",
            "interval_seconds": 30,
            "max_executions": 3,
            "current_executions": 1,
            "next_run_at": None,
            "is_block": False,
            "parent_id": None,
            "payload": {"type_doc": "JSON", "data": ["{}"]},
            "alias": "demo",
            "steps_names": ["step_a"],
        }
    ]
    repository = PostgresTaskRepository(
        storage=PostgresTaskStorage(pool=_FakePool(conn))
    )

    state = asyncio.run(
        repository.fetch_execution_batch(
            settings=RepositoryPollSettings(tasks_limit=5, zombie_timeout_minutes=9)
        )
    )

    assert len(state.tasks) == 1
    task = state.tasks[0]
    assert task.task_id == 42
    assert task.steps_names == ["step_a"]
    assert task.payload is not None
    assert task.payload.primary == "{}"


def test_postgres_repository_marks_running_and_alias() -> None:
    conn = _FakeConnection()
    repository = PostgresTaskRepository(
        storage=PostgresTaskStorage(pool=_FakePool(conn))
    )
    task = TaskState(task_id=7, scenario="scenario9", alias="worker")

    asyncio.run(repository.mark_task_running(task))

    assert len(conn.executed) == 2
    assert conn.executed[0][1] == ("RUNNING", 7)
    assert conn.executed[1][1] == ("worker", 7)


def test_postgres_repository_persists_cyclical_retry() -> None:
    conn = _FakeConnection()
    repository = PostgresTaskRepository(
        storage=PostgresTaskStorage(pool=_FakePool(conn))
    )
    task = TaskState(
        task_id=11,
        scenario="scenario9",
        type_task=TaskType.CYCLICAL,
        interval_seconds=60,
        max_executions=3,
        current_executions=0,
        payload=TaskPayload(type_doc=EnumDoc.JSON, data=["{}"]),
    )
    task.result.ok = False

    asyncio.run(repository.persist_task_result(task))

    assert len(conn.executed) >= 2
    _, result_insert_args = conn.executed[0]
    _, update_args = conn.executed[1]
    assert result_insert_args[0] == 11
    assert update_args[0] == "NEW"
    assert update_args[1] == 1
    assert update_args[-1] == 11
    assert update_args[-2] is not None


def test_postgres_repository_persists_cyclical_success_as_finished() -> None:
    conn = _FakeConnection()
    conn.fetchrow_result = {"pending_blocking": 0, "failed_non_blocking": 0}
    repository = PostgresTaskRepository(
        storage=PostgresTaskStorage(pool=_FakePool(conn))
    )
    task = TaskState(
        task_id=12,
        scenario="scenario9",
        type_task=TaskType.CYCLICAL,
        interval_seconds=30,
        max_executions=None,
        current_executions=2,
        payload=TaskPayload(type_doc=EnumDoc.JSON, data=['{"cursor":1}']),
    )
    task.result.ok = True

    asyncio.run(repository.persist_task_result(task))

    update_queries = [
        args for query, args in conn.executed if query.startswith("UPDATE")
    ]
    assert update_queries
    assert update_queries[0][0] == "FINISHED"
    assert update_queries[0][1] == 3
    assert update_queries[0][-1] == 12


def test_count_parent_subtasks_state_uses_dense_placeholders() -> None:
    conn = _FakeConnection()
    conn.fetchrow_result = {"pending_blocking": 0, "failed_non_blocking": 0}
    storage = PostgresTaskStorage(pool=_FakePool(conn))

    pending, failed = asyncio.run(storage.count_parent_subtasks_state(99))

    assert pending == 0
    assert failed == 0
    query, args = conn.executed[0]
    assert "NOT IN ($1, $2, $3, $4)" in query
    assert "IN ($5, $6)" in query
    assert "WHERE parent_id = $7" in query
    assert "$8" not in query
    assert args[-1] == 99


def test_postgres_repository_persists_cyclical_success_at_max_executions() -> None:
    conn = _FakeConnection()
    conn.fetchrow_result = {"pending_blocking": 0, "failed_non_blocking": 0}
    repository = PostgresTaskRepository(
        storage=PostgresTaskStorage(pool=_FakePool(conn))
    )
    task = TaskState(
        task_id=13,
        scenario="scenario9",
        type_task=TaskType.CYCLICAL,
        interval_seconds=30,
        max_executions=3,
        current_executions=2,
    )
    task.result.ok = True

    asyncio.run(repository.persist_task_result(task))

    # insert result + count parent subtasks + update task
    assert any("NOT IN ($1, $2, $3, $4)" in query for query, _ in conn.executed)
    update_queries = [
        args for query, args in conn.executed if query.startswith("UPDATE")
    ]
    assert update_queries
    assert update_queries[0][0] == "FINISHED"
