import asyncio

import pytest

from scenario_pipeliner.worker.core.exceptions import PipelineCancelledError
from scenario_pipeliner.worker.execution.batch_executor import (
    ExecutionBatchRunner,
)


def test_batch_executor_completes_successfully() -> None:
    results: list[int] = []

    async def worker(value: int) -> None:
        await asyncio.sleep(0)
        results.append(value)

    async def _run() -> None:
        runner = ExecutionBatchRunner(shutdown_timeout_seconds=1)
        await runner.run_with_shutdown_timeout(
            (worker(i) for i in [1, 2, 3]),
            on_timeout=_on_timeout_noop,
            names=("w1", "w2", "w3"),
        )

    asyncio.run(_run())

    assert sorted(results) == [1, 2, 3]


def test_batch_executor_raises_cancelled_on_timeout_and_calls_hook() -> None:
    hooks = {"timeout": 0}

    async def slow_worker() -> None:
        await asyncio.sleep(0.2)

    async def on_timeout() -> None:
        hooks["timeout"] += 1

    async def _run() -> None:
        runner = ExecutionBatchRunner(shutdown_timeout_seconds=0.05)
        with pytest.raises(PipelineCancelledError, match="Shutdown timeout"):
            await runner.run_with_shutdown_timeout(
                [slow_worker()],
                on_timeout=on_timeout,
            )

    asyncio.run(_run())

    assert hooks["timeout"] == 1


def test_batch_executor_assigns_task_names() -> None:
    observed_names: list[str] = []

    async def worker() -> None:
        task = asyncio.current_task()
        assert task is not None
        observed_names.append(task.get_name())

    async def _run() -> None:
        runner = ExecutionBatchRunner(shutdown_timeout_seconds=1)
        await runner.run_with_shutdown_timeout(
            [worker(), worker()],
            on_timeout=_on_timeout_noop,
            names=["task-A", "task-B"],
        )

    asyncio.run(_run())

    assert sorted(observed_names) == ["task-A", "task-B"]


def test_batch_executor_cancels_hanging_tasks_on_timeout() -> None:
    cancelled = {"value": 0}

    async def hanging_worker() -> None:
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            cancelled["value"] += 1
            raise

    async def _run() -> None:
        runner = ExecutionBatchRunner(shutdown_timeout_seconds=0.01)
        with pytest.raises(PipelineCancelledError):
            await runner.run_with_shutdown_timeout(
                [hanging_worker()],
                on_timeout=_on_timeout_noop,
            )

    asyncio.run(_run())

    assert cancelled["value"] == 1


async def _on_timeout_noop() -> None:
    return
