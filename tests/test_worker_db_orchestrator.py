import asyncio
from collections.abc import Callable
from dataclasses import dataclass

import pytest

from scenario_pipeliner.worker.core.exceptions import (
    DatabaseError,
    PipelineCancelledError,
)
from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.states import BaseState, TaskState
from scenario_pipeliner.worker.execution.active_flag_poller import (
    ActiveFlagPoller,
)
from scenario_pipeliner.worker.execution.batch_executor import (
    ExecutionBatchRunner,
)
from scenario_pipeliner.worker.execution.db_orchestrator import (
    DBExecutionOrchestrator,
    ExecutionBatchState,
)
from scenario_pipeliner.worker.execution.task_dispatch import (
    TaskExecutionRouter,
)


@dataclass
class _ScenarioState(TaskState):
    promoted: bool = True


class _SpyPipeline(AsyncPipeline):
    def __init__(self, *, delay: float = 0.0) -> None:
        super().__init__(steps=[])
        self.delay = delay
        self.seen_states: list[TaskState] = []
        self.enter_calls = 0
        self.exit_calls = 0

    async def __aenter__(self) -> AsyncPipeline:
        self.enter_calls += 1
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.exit_calls += 1
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def execute(self, state: BaseState | None = None) -> None:
        assert state is not None
        assert isinstance(state, TaskState)
        if self.delay:
            await asyncio.sleep(self.delay)
        self.seen_states.append(state)


def test_db_orchestrator_executes_tasks_and_promotes_states() -> None:
    produced: list[_SpyPipeline] = []

    def factory() -> AsyncPipeline:
        pipeline = _SpyPipeline()
        produced.append(pipeline)
        return pipeline

    router = TaskExecutionRouter(
        pipeline_factories={"scenario9": factory},
        state_classes={"scenario9": _ScenarioState},
    )
    orchestrator = DBExecutionOrchestrator(
        router=router,
        batch_runner=ExecutionBatchRunner(shutdown_timeout_seconds=1),
        max_concurrent_tasks=2,
    )
    state = ExecutionBatchState(
        tasks=[
            TaskState(task_id=1, scenario="scenario9"),
            TaskState(task_id=2, scenario="scenario9"),
        ]
    )

    asyncio.run(orchestrator.execute(state))

    assert len(produced) == 2
    assert all(len(item.seen_states) == 1 for item in produced)
    assert all(isinstance(item.seen_states[0], _ScenarioState) for item in produced)
    assert all(item.enter_calls == 1 for item in produced)
    assert all(item.exit_calls == 1 for item in produced)


def test_db_orchestrator_reports_per_task_errors_without_crashing_batch() -> None:
    router = TaskExecutionRouter(pipeline_factories={}, state_classes={})
    orchestrator = DBExecutionOrchestrator(
        router=router,
        batch_runner=ExecutionBatchRunner(shutdown_timeout_seconds=1),
    )
    state = ExecutionBatchState(
        tasks=[
            TaskState(task_id=1, scenario="unknown-a"),
            TaskState(task_id=2, scenario="unknown-b"),
        ]
    )
    errors: list[tuple[int, str]] = []

    async def on_task_error(task: TaskState, error: Exception) -> None:
        errors.append((task.task_id, str(error)))

    asyncio.run(orchestrator.execute(state, on_task_error=on_task_error))

    assert [task_id for task_id, _ in errors] == [1, 2]
    assert all("Pipeline for scenario" in msg for _, msg in errors)


def test_db_orchestrator_on_task_error_receives_promoted_state_with_mutations() -> None:
    class _FailingPipeline(AsyncPipeline):
        async def execute(self, state: BaseState | None = None) -> None:
            assert isinstance(state, _ScenarioState)
            state.promoted = False
            raise RuntimeError("boom")

    router = TaskExecutionRouter(
        pipeline_factories={"scenario9": lambda: _FailingPipeline(steps=[])},
        state_classes={"scenario9": _ScenarioState},
    )
    orchestrator = DBExecutionOrchestrator(
        router=router,
        batch_runner=ExecutionBatchRunner(shutdown_timeout_seconds=1),
    )
    state = ExecutionBatchState(tasks=[TaskState(task_id=7, scenario="scenario9")])
    captured: list[tuple[TaskState, str]] = []

    async def on_task_error(task: TaskState, error: Exception) -> None:
        captured.append((task, str(error)))

    asyncio.run(orchestrator.execute(state, on_task_error=on_task_error))

    assert len(captured) == 1
    errored_state, message = captured[0]
    assert isinstance(errored_state, _ScenarioState)
    assert errored_state.promoted is False
    assert message == "boom"


def test_db_orchestrator_timeout_calls_timeout_hook() -> None:
    def factory() -> AsyncPipeline:
        return _SpyPipeline(delay=0.2)

    router = TaskExecutionRouter(
        pipeline_factories={"scenario9": factory},
        state_classes={"scenario9": _ScenarioState},
    )
    orchestrator = DBExecutionOrchestrator(
        router=router,
        batch_runner=ExecutionBatchRunner(shutdown_timeout_seconds=0.01),
    )
    state = ExecutionBatchState(tasks=[TaskState(task_id=1, scenario="scenario9")])
    timeout_calls = {"count": 0}

    async def on_timeout(tasks: list[TaskState]) -> None:
        timeout_calls["count"] += 1
        assert [task.task_id for task in tasks] == [1]

    with pytest.raises(PipelineCancelledError, match="Shutdown timeout"):
        asyncio.run(orchestrator.execute(state, on_timeout=on_timeout))

    assert timeout_calls["count"] == 1


def test_db_orchestrator_poller_db_failure_cancels_tasks() -> None:
    def factory() -> AsyncPipeline:
        return _SpyPipeline(delay=0.2)

    router = TaskExecutionRouter(
        pipeline_factories={"scenario9": factory},
        state_classes={"scenario9": _ScenarioState},
    )

    def poller_factory(
        should_continue: Callable[[], bool],
        on_db_failure: Callable[[], None],
    ) -> ActiveFlagPoller:
        async def check_main() -> bool:
            raise DatabaseError("db fail")

        return ActiveFlagPoller(
            interval_seconds=0,
            should_continue=should_continue,
            check_main=check_main,
            on_db_failure=on_db_failure,
        )

    orchestrator = DBExecutionOrchestrator(
        router=router,
        batch_runner=ExecutionBatchRunner(shutdown_timeout_seconds=1),
        poller_factory=poller_factory,
    )
    state = ExecutionBatchState(tasks=[TaskState(task_id=1, scenario="scenario9")])

    asyncio.run(orchestrator.execute(state, on_task_error=_ignore_task_error))

    assert state.is_cancelled is True


async def _ignore_task_error(task: TaskState, error: Exception) -> None:
    return
