import asyncio
from dataclasses import dataclass

from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.runner import AsyncRunner
from scenario_pipeliner.worker.core.settings import Settings
from scenario_pipeliner.worker.core.states import BaseState


class _SpyPipeline(AsyncPipeline):
    def __init__(self) -> None:
        super().__init__(steps=[])
        self.enter_calls = 0
        self.exit_calls = 0

    async def __aenter__(self) -> AsyncPipeline:
        self.enter_calls += 1
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.exit_calls += 1
        await super().__aexit__(exc_type, exc_val, exc_tb)


class _TestRunner(AsyncRunner):
    def __init__(self, pipeline: AsyncPipeline) -> None:
        super().__init__(pipeline=pipeline, settings=Settings())
        self.execute_calls = 0
        self.extra_connect_calls = 0
        self.extra_disconnect_calls = 0

    async def _connect_extra_clients(self) -> None:
        self.extra_connect_calls += 1

    async def _disconnect_extra_clients(self) -> None:
        self.extra_disconnect_calls += 1

    async def execute(self) -> None:
        async with self._pipeline_lifecycle():
            self.execute_calls += 1


@dataclass
class _CancellableState(BaseState):
    pass


def test_runner_pipeline_lifecycle_manages_pipeline_when_not_in_context() -> None:
    pipeline = _SpyPipeline()
    runner = _TestRunner(pipeline)

    asyncio.run(runner.execute())

    assert runner.execute_calls == 1
    assert pipeline.enter_calls == 1
    assert pipeline.exit_calls == 1


def test_runner_pipeline_lifecycle_reuses_context_managed_pipeline() -> None:
    pipeline = _SpyPipeline()
    runner = _TestRunner(pipeline)

    async def _run() -> None:
        async with runner:
            await runner.execute()

    asyncio.run(_run())

    assert runner.execute_calls == 1
    assert pipeline.enter_calls == 1
    assert pipeline.exit_calls == 1
    assert runner.extra_connect_calls == 1
    assert runner.extra_disconnect_calls == 1


def test_runner_stop_sets_event_and_cancels_current_state() -> None:
    runner = _TestRunner(_SpyPipeline())
    runner.current_state = _CancellableState()  # type: ignore[attr-defined]

    runner.stop()

    assert runner._stop_event.is_set() is True
    assert runner.current_state.is_cancelled is True  # type: ignore[attr-defined]
