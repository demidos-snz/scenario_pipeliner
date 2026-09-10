import asyncio
from dataclasses import dataclass
from datetime import UTC

import pytest

from scenario_pipeliner.worker.core.clients import AsyncClient
from scenario_pipeliner.worker.core.enums import TaskStatus, TaskType
from scenario_pipeliner.worker.core.exceptions import PipelineCancelledError
from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.settings import (
    ClientSettings,
    StepSettings,
)
from scenario_pipeliner.worker.core.states import (
    BaseState,
    TaskPayload,
    TaskPayloadParams,
    TaskState,
)
from scenario_pipeliner.worker.core.step import AsyncStep, BaseSubtaskStep
from scenario_pipeliner.worker.core.utils import (
    get_cancel_event,
    get_params_for_cyclical_task,
    utc_now,
)


class _DummyClient(AsyncClient):
    def __init__(self) -> None:
        super().__init__(settings=ClientSettings())
        self.connect_calls = 0
        self.disconnect_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1
        self.initialized = True

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.initialized = False

    async def receive(self, *args, **kwargs) -> dict[str, str] | None:
        return None


@dataclass
class _RecorderState(BaseState):
    seen_cancel_event: asyncio.Event | None = None
    executed: bool = False


class _RecorderStep(AsyncStep):
    def __init__(self, client: _DummyClient | None = None) -> None:
        super().__init__(settings=StepSettings())
        self._client = client or _DummyClient()

    @property
    def clients(self) -> list[AsyncClient]:
        return [self._client]

    async def _run(self, state: _RecorderState) -> None:
        state.seen_cancel_event = get_cancel_event()
        state.executed = True


class _SubtaskStep(BaseSubtaskStep):
    def __init__(self) -> None:
        super().__init__(settings=StepSettings())
        self.created_calls = 0
        self.run_calls = 0

    @property
    def clients(self) -> list[AsyncClient]:
        return []

    async def _create_subtask(self, state: TaskState) -> None:
        self.created_calls += 1

    async def _run_subtask(self, state: TaskState) -> None:
        self.run_calls += 1


def test_task_payload_coerces_legacy_string_data() -> None:
    payload = TaskPayload.model_validate({"data": "hello"})
    assert payload.data == ["hello"]
    assert payload.primary == "hello"


def test_task_payload_params_are_opaque() -> None:
    payload = TaskPayload.model_validate(
        {
            "data": ["doc"],
            "params": {"request_ids": ["req-1"], "cursor": 10},
        }
    )
    assert payload.params is not None
    dumped = payload.params.model_dump()
    assert dumped == {"request_ids": ["req-1"], "cursor": 10}
    via_ctor = TaskPayloadParams.model_validate(
        {"request_ids": ["req-1"], "cursor": 10}
    )
    assert via_ctor.model_dump() == dumped


def test_pipeline_requires_initialized_step_before_execute() -> None:
    step = _RecorderStep()
    pipeline = AsyncPipeline(steps=[step])
    with pytest.raises(RuntimeError, match="must be initialized before run"):
        asyncio.run(pipeline.execute(_RecorderState()))


def test_pipeline_binds_cancel_event_to_step_context() -> None:
    step = _RecorderStep()
    asyncio.run(step.initialize())
    state = _RecorderState()
    pipeline = AsyncPipeline(steps=[step])

    asyncio.run(pipeline.execute(state))

    assert state.executed is True
    assert state.seen_cancel_event is state.cancel_event


def test_step_raises_cancelled_error_when_state_already_cancelled() -> None:
    step = _RecorderStep()
    asyncio.run(step.initialize())
    state = _RecorderState()
    state.cancel()

    with pytest.raises(PipelineCancelledError):
        asyncio.run(step.run(state))


def test_base_subtask_step_dispatches_by_task_type_and_result() -> None:
    step = _SubtaskStep()
    asyncio.run(step.initialize())

    linear_task = TaskState(task_id=1)
    linear_task.result.ok = True
    asyncio.run(step.run(linear_task))

    cyclical_task = TaskState(task_id=2)
    cyclical_task.result.ok = True
    cyclical_task.type_task = TaskType.CYCLICAL
    asyncio.run(step.run(cyclical_task))

    # Cyclical polls must run even when result.ok is still the default False
    # (prior draft steps are skipped for CYCLICAL tasks).
    cyclical_default_ok = TaskState(task_id=4)
    cyclical_default_ok.type_task = TaskType.CYCLICAL
    assert cyclical_default_ok.result.ok is False
    asyncio.run(step.run(cyclical_default_ok))

    skipped_task = TaskState(task_id=3)
    skipped_task.result.ok = False
    asyncio.run(step.run(skipped_task))

    assert step.created_calls == 1
    assert step.run_calls == 2


def test_pipeline_context_manager_connects_and_disconnects_clients_once() -> None:
    shared_client = _DummyClient()
    step1 = _RecorderStep(client=shared_client)
    step2 = _RecorderStep(client=shared_client)
    pipeline = AsyncPipeline(steps=[step1, step2])

    async def _run() -> None:
        async with pipeline:
            pass

    asyncio.run(_run())

    assert shared_client.connect_calls == 1
    assert shared_client.disconnect_calls == 1


def test_utc_now_is_timezone_aware() -> None:
    assert utc_now().tzinfo is UTC


def test_cyclical_retry_next_run_at_is_timezone_aware() -> None:
    task = TaskState(
        task_id=1,
        type_task=TaskType.CYCLICAL,
        interval_seconds=30,
        max_executions=3,
        current_executions=0,
    )
    status, executions, next_run_at = get_params_for_cyclical_task(
        TaskStatus.FAILED, task
    )
    assert status is TaskStatus.NEW
    assert executions == 1
    assert next_run_at is not None
    assert next_run_at.tzinfo is UTC
