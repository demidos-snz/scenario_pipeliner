import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress

from scenario_pipeliner.worker.core.exceptions import PipelineCancelledError
from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.states import (
    ExecutionBatchState,
    TaskState,
)
from scenario_pipeliner.worker.execution.active_flag_poller import (
    ActiveFlagPoller,
)
from scenario_pipeliner.worker.execution.batch_executor import (
    ExecutionBatchRunner,
)
from scenario_pipeliner.worker.execution.task_dispatch import (
    TaskExecutionRouter,
)

PollerFactory = Callable[
    [Callable[[], bool], Callable[[], None]],
    ActiveFlagPoller,
]
TaskErrorHandler = Callable[[TaskState, Exception], Awaitable[None]]
TaskStateHandler = Callable[[TaskState], Awaitable[None]]
TimeoutHandler = Callable[[list[TaskState]], Awaitable[None]]


class DBExecutionOrchestrator:
    """Minimal DB execution orchestrator for the refactor track."""

    def __init__(
        self,
        *,
        router: TaskExecutionRouter,
        batch_runner: ExecutionBatchRunner,
        max_concurrent_tasks: int = 5,
        poller_factory: PollerFactory | None = None,
    ) -> None:
        self._router = router
        self._batch_runner = batch_runner
        self._max_concurrent_tasks = max_concurrent_tasks
        self._poller_factory = poller_factory

    async def execute(
        self,
        state: ExecutionBatchState,
        *,
        on_task_start: TaskStateHandler | None = None,
        on_task_success: TaskStateHandler | None = None,
        on_task_error: TaskErrorHandler | None = None,
        on_timeout: TimeoutHandler | None = None,
    ) -> None:
        if not state.tasks:
            return

        semaphore = asyncio.Semaphore(self._max_concurrent_tasks)
        task_cancel_events = {task.task_id: asyncio.Event() for task in state.tasks}
        active_task_ids: set[int] = {task.task_id for task in state.tasks}
        poll_task: asyncio.Task[None] | None = None

        def _cancel_execution_batch() -> None:
            state.cancel()
            for event in task_cancel_events.values():
                event.set()

        if self._poller_factory is not None:
            poller = self._poller_factory(
                lambda: not state.is_cancelled and bool(active_task_ids),
                _cancel_execution_batch,
            )
            poll_task = poller.start()

        async def process_task(task_state: TaskState) -> None:
            async with semaphore:
                task_state.cancel_event = task_cancel_events[task_state.task_id]
                error_state: TaskState = task_state
                try:
                    promoted = self._router.promote_task_state(task_state)
                    error_state = promoted
                    pipeline_factory, _is_default = (
                        self._router.resolve_pipeline_factory(promoted.scenario)
                    )
                    if pipeline_factory is None:
                        raise LookupError(
                            f"Pipeline for scenario {promoted.scenario!r} not found"
                        )
                    pipeline = pipeline_factory()
                    if not isinstance(pipeline, AsyncPipeline):
                        raise TypeError(
                            "Pipeline factory for scenario "
                            f"{promoted.scenario!r} returned "
                            f"{type(pipeline).__name__}, expected AsyncPipeline"
                        )
                    if promoted.is_cancelled:
                        raise PipelineCancelledError(
                            f"Task {promoted.task_id} cancelled before execute"
                        )
                    if on_task_start is not None:
                        await on_task_start(promoted)
                    async with pipeline:
                        await pipeline.execute(state=promoted)
                    if on_task_success is not None:
                        await on_task_success(promoted)
                except Exception as e:
                    if on_task_error is not None:
                        await on_task_error(error_state, e)
                    else:
                        raise
                finally:
                    active_task_ids.discard(task_state.task_id)
                    task_cancel_events.pop(task_state.task_id, None)

        async def _on_timeout() -> None:
            if on_timeout is not None:
                await on_timeout(state.tasks)

        try:
            await self._batch_runner.run_with_shutdown_timeout(
                (process_task(task) for task in state.tasks),
                on_timeout=_on_timeout,
                names=(f"task-{task.task_id}" for task in state.tasks),
            )
        finally:
            if poll_task is not None:
                poll_task.cancel()
                with suppress(asyncio.CancelledError):
                    await poll_task
