from __future__ import annotations

import asyncio

import pytest

from scenario_pipeliner.worker.core.exceptions import PipelineCancelledError
from scenario_pipeliner.worker.execution.batch_executor import ExecutionBatchRunner


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def test_batch_runner_completes_without_stop() -> None:
    runner = ExecutionBatchRunner(shutdown_timeout_seconds=0.05)
    finished: list[int] = []
    timeout_calls = {"count": 0}

    async def one() -> None:
        await asyncio.sleep(0.01)
        finished.append(1)

    async def two() -> None:
        await asyncio.sleep(0.01)
        finished.append(2)

    async def on_timeout() -> None:
        timeout_calls["count"] += 1

    asyncio.run(
        runner.run_with_shutdown_timeout(
            (one(), two()),
            on_timeout=on_timeout,
        )
    )

    assert sorted(finished) == [1, 2]
    assert timeout_calls["count"] == 0


def test_batch_runner_ignores_timeout_until_stop() -> None:
    runner = ExecutionBatchRunner(shutdown_timeout_seconds=0.02)
    timeout_calls = {"count": 0}

    async def on_timeout() -> None:
        timeout_calls["count"] += 1

    asyncio.run(
        runner.run_with_shutdown_timeout(
            (_sleep(0.08),),
            on_timeout=on_timeout,
        )
    )

    assert timeout_calls["count"] == 0


def test_batch_runner_stop_then_timeout_cancels() -> None:
    runner = ExecutionBatchRunner(shutdown_timeout_seconds=0.02)
    timeout_calls = {"count": 0}
    stop = asyncio.Event()

    async def on_timeout() -> None:
        timeout_calls["count"] += 1

    async def _run() -> None:
        async def fire_stop() -> None:
            await asyncio.sleep(0.01)
            stop.set()

        stopper = asyncio.create_task(fire_stop())
        try:
            await runner.run_with_shutdown_timeout(
                (_sleep(1.0),),
                on_timeout=on_timeout,
                stop_event=stop,
            )
        finally:
            await stopper

    with pytest.raises(PipelineCancelledError, match="Shutdown timeout"):
        asyncio.run(_run())

    assert timeout_calls["count"] == 1


def test_batch_runner_stop_allows_tasks_to_finish() -> None:
    runner = ExecutionBatchRunner(shutdown_timeout_seconds=0.5)
    timeout_calls = {"count": 0}
    finished = {"ok": False}
    stop = asyncio.Event()

    async def on_timeout() -> None:
        timeout_calls["count"] += 1

    async def work() -> None:
        await asyncio.sleep(0.03)
        finished["ok"] = True

    async def _run() -> None:
        async def fire_stop() -> None:
            await asyncio.sleep(0.005)
            stop.set()

        stopper = asyncio.create_task(fire_stop())
        try:
            await runner.run_with_shutdown_timeout(
                (work(),),
                on_timeout=on_timeout,
                stop_event=stop,
            )
        finally:
            await stopper

    asyncio.run(_run())

    assert finished["ok"] is True
    assert timeout_calls["count"] == 0
