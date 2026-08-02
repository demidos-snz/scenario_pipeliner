import asyncio

import pytest

from scenario_pipeliner.worker.core.exceptions import (
    DatabaseError,
    PipelineCancelledError,
)
from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.settings import RunnerDBSettings
from scenario_pipeliner.worker.core.states import TaskState
from scenario_pipeliner.worker.execution.batch_executor import (
    ExecutionBatchRunner,
)
from scenario_pipeliner.worker.execution.db_orchestrator import (
    DBExecutionOrchestrator,
    ExecutionBatchState,
)
from scenario_pipeliner.worker.execution.runner_db import (
    RunnerDB,
)
from scenario_pipeliner.worker.execution.task_dispatch import (
    TaskExecutionRouter,
)


class _SpyPipeline(AsyncPipeline):
    def __init__(self) -> None:
        super().__init__(steps=[])
        self.calls = 0
        self.enter_calls = 0
        self.exit_calls = 0

    async def __aenter__(self) -> AsyncPipeline:
        self.enter_calls += 1
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.exit_calls += 1
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def execute(self, state=None) -> None:  # type: ignore[override]
        self.calls += 1


def _build_orchestrator() -> tuple[DBExecutionOrchestrator, list[_SpyPipeline]]:
    produced: list[_SpyPipeline] = []

    def factory() -> AsyncPipeline:
        pipeline = _SpyPipeline()
        produced.append(pipeline)
        return pipeline

    router = TaskExecutionRouter(
        pipeline_factories={"scenario9": factory},
        state_classes={"scenario9": TaskState},
    )
    return (
        DBExecutionOrchestrator(
            router=router,
            batch_runner=ExecutionBatchRunner(shutdown_timeout_seconds=1),
        ),
        produced,
    )


def test_runner_db_processes_polled_batches() -> None:
    orchestrator, scenario_pipelines = _build_orchestrator()
    settings = RunnerDBSettings(POLL_INTERVAL_SECONDS=1)
    calls = {"count": 0}

    async def fetch_batch_state() -> ExecutionBatchState:
        calls["count"] += 1
        if calls["count"] >= 2:
            runner.stop()
        return ExecutionBatchState(
            tasks=[TaskState(task_id=calls["count"], scenario="scenario9")]
        )

    runner = RunnerDB(
        pipeline=AsyncPipeline(steps=[]),
        settings=settings,
        fetch_batch_state=fetch_batch_state,
        orchestrator=orchestrator,
    )

    asyncio.run(runner.execute())

    assert calls["count"] == 2
    assert len(scenario_pipelines) == 2
    assert all(pipeline.enter_calls == 1 for pipeline in scenario_pipelines)
    assert all(pipeline.exit_calls == 1 for pipeline in scenario_pipelines)


def test_runner_db_applies_exponential_backoff_on_errors() -> None:
    orchestrator, _ = _build_orchestrator()
    settings = RunnerDBSettings(
        POLL_INTERVAL_SECONDS=60,
        ERROR_BACKOFF_INITIAL_SECONDS=1,
        ERROR_BACKOFF_MAX_SECONDS=8,
    )
    waits: list[int] = []
    calls = {"count": 0}

    async def fetch_batch_state() -> ExecutionBatchState:
        calls["count"] += 1
        raise DatabaseError("db error")

    runner = RunnerDB(
        pipeline=AsyncPipeline(steps=[]),
        settings=settings,
        fetch_batch_state=fetch_batch_state,
        orchestrator=orchestrator,
    )

    async def record_wait(seconds: int) -> None:
        waits.append(seconds)
        if len(waits) >= 3:
            runner.stop()

    runner._wait_before_next_poll = record_wait  # type: ignore[method-assign]

    asyncio.run(runner.execute())

    assert waits == [1, 2, 4]


def test_runner_db_resets_backoff_after_success() -> None:
    orchestrator, _ = _build_orchestrator()
    settings = RunnerDBSettings(
        POLL_INTERVAL_SECONDS=5,
        ERROR_BACKOFF_INITIAL_SECONDS=1,
        ERROR_BACKOFF_MAX_SECONDS=8,
    )
    waits: list[int] = []
    calls = {"count": 0}

    async def fetch_batch_state() -> ExecutionBatchState:
        calls["count"] += 1
        if calls["count"] == 1:
            raise DatabaseError("db error")
        return ExecutionBatchState(tasks=[])

    runner = RunnerDB(
        pipeline=AsyncPipeline(steps=[]),
        settings=settings,
        fetch_batch_state=fetch_batch_state,
        orchestrator=orchestrator,
    )

    async def record_wait(seconds: int) -> None:
        waits.append(seconds)
        if len(waits) >= 2:
            runner.stop()

    runner._wait_before_next_poll = record_wait  # type: ignore[method-assign]

    asyncio.run(runner.execute())

    assert waits == [1, 5]
    assert runner._consecutive_errors == 0


def test_runner_db_handles_pipeline_cancelled_without_backoff() -> None:
    settings = RunnerDBSettings(POLL_INTERVAL_SECONDS=7)
    waits: list[int] = []

    class _CancellingOrchestrator(DBExecutionOrchestrator):
        async def execute(  # type: ignore[override]
            self,
            state,
            *,
            on_task_start=None,
            on_task_success=None,
            on_task_error=None,
            on_timeout=None,
        ) -> None:
            raise PipelineCancelledError("cancelled")

    async def fetch_batch_state() -> ExecutionBatchState:
        return ExecutionBatchState(tasks=[TaskState(task_id=1, scenario="scenario9")])

    runner = RunnerDB(
        pipeline=AsyncPipeline(steps=[]),
        settings=settings,
        fetch_batch_state=fetch_batch_state,
        orchestrator=_CancellingOrchestrator(
            router=TaskExecutionRouter(pipeline_factories={}, state_classes={}),
            batch_runner=ExecutionBatchRunner(shutdown_timeout_seconds=1),
        ),
    )

    async def record_wait(seconds: int) -> None:
        waits.append(seconds)
        runner.stop()

    runner._wait_before_next_poll = record_wait  # type: ignore[method-assign]

    asyncio.run(runner.execute())

    assert waits == [7]


def test_runner_db_settings_validate_backoff_bounds() -> None:
    with pytest.raises(ValueError, match="ERROR_BACKOFF_INITIAL_SECONDS"):
        RunnerDBSettings(
            ERROR_BACKOFF_INITIAL_SECONDS=10,
            ERROR_BACKOFF_MAX_SECONDS=3,
        )
