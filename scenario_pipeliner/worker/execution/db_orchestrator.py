import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from typing import Protocol

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
    DEFAULT_PIPELINE_KEY,
    TaskExecutionRouter,
)

logger = logging.getLogger(__name__)

PollerFactory = Callable[
    [Callable[[], bool], Callable[[], None]],
    ActiveFlagPoller,
]
TaskErrorHandler = Callable[[TaskState, Exception], Awaitable[None]]
TaskStateHandler = Callable[[TaskState], Awaitable[None]]
TimeoutHandler = Callable[[list[TaskState]], Awaitable[None]]


class BatchRuntimeControl(Protocol):
    """Flags + heartbeat used by the stock DB runner poller."""

    async def is_worker_enabled(self) -> bool: ...

    async def is_scenario_active(self, scenario: str) -> bool: ...

    async def touch_heartbeats(self, task_ids: Sequence[int]) -> None: ...


class DBExecutionOrchestrator:
    """Minimal DB execution orchestrator for the refactor track."""

    def __init__(
        self,
        *,
        router: TaskExecutionRouter,
        batch_runner: ExecutionBatchRunner,
        max_concurrent_tasks: int = 5,
        poller_factory: PollerFactory | None = None,
        runtime_control: BatchRuntimeControl | None = None,
        heartbeat_interval_seconds: int = 30,
    ) -> None:
        self._router = router
        self._batch_runner = batch_runner
        self._max_concurrent_tasks = max_concurrent_tasks
        self._poller_factory = poller_factory
        self._runtime_control = runtime_control
        self._heartbeat_interval_seconds = heartbeat_interval_seconds

    async def execute(
        self,
        state: ExecutionBatchState,
        *,
        on_task_start: TaskStateHandler | None = None,
        on_task_success: TaskStateHandler | None = None,
        on_task_error: TaskErrorHandler | None = None,
        on_timeout: TimeoutHandler | None = None,
        stop_event: asyncio.Event | None = None,
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
        elif self._runtime_control is not None:
            poller = self._default_runtime_poller(
                state=state,
                active_task_ids=active_task_ids,
                on_cancel=_cancel_execution_batch,
            )
            poll_task = poller.start()

        async def process_task(task_state: TaskState) -> None:
            async with semaphore:
                logger.info(
                    "Processing task %s with scenario %s",
                    task_state.task_id,
                    task_state.scenario,
                )
                task_state.cancel_event = task_cancel_events[task_state.task_id]
                error_state: TaskState = task_state
                try:
                    promoted = self._router.promote_task_state(task_state)
                    error_state = promoted
                    pipeline_factory, is_default = (
                        self._router.resolve_pipeline_factory(promoted.scenario)
                    )
                    if pipeline_factory is None:
                        logger.error(
                            "Pipeline for scenario %s not found", promoted.scenario
                        )
                        raise LookupError(
                            f"Pipeline for scenario {promoted.scenario!r} not found"
                        )
                    if is_default:
                        logger.warning(
                            "Pipeline for scenario %s not found, using %s",
                            promoted.scenario,
                            DEFAULT_PIPELINE_KEY,
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
                    # Snapshot for API GET /tasks/{id}/steps_names (root tasks only).
                    if promoted.parent_id is None:
                        promoted.steps_names = [str(step) for step in pipeline.steps]
                    logger.info("Executing pipeline for scenario %s", promoted.scenario)
                    if on_task_start is not None:
                        await on_task_start(promoted)
                    async with pipeline:
                        await pipeline.execute(state=promoted)
                    if on_task_success is not None:
                        await on_task_success(promoted)
                except PipelineCancelledError as e:
                    logger.warning(
                        "Task %s interrupted (pause), updating status to CANCELLED",
                        error_state.task_id,
                    )
                    if on_task_error is not None:
                        await on_task_error(error_state, e)
                    else:
                        raise
                except Exception as e:
                    logger.exception("Task %s failed: %s", error_state.task_id, e)
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
                stop_event=stop_event,
            )
        finally:
            if poll_task is not None:
                poll_task.cancel()
                with suppress(asyncio.CancelledError):
                    await poll_task

    def _default_runtime_poller(
        self,
        *,
        state: ExecutionBatchState,
        active_task_ids: set[int],
        on_cancel: Callable[[], None],
    ) -> ActiveFlagPoller:
        control = self._runtime_control
        assert control is not None

        async def check_main() -> bool:
            if await control.is_worker_enabled():
                return True
            on_cancel()
            return False

        async def check_scenarios() -> None:
            seen: set[str] = set()
            for task in state.tasks:
                if task.task_id not in active_task_ids:
                    continue
                if task.scenario in seen:
                    continue
                seen.add(task.scenario)
                if not await control.is_scenario_active(task.scenario):
                    on_cancel()
                    return

        async def check_tasks() -> None:
            await control.touch_heartbeats(tuple(active_task_ids))

        return ActiveFlagPoller(
            interval_seconds=self._heartbeat_interval_seconds,
            should_continue=lambda: not state.is_cancelled and bool(active_task_ids),
            check_main=check_main,
            check_scenarios=check_scenarios,
            check_tasks=check_tasks,
            on_db_failure=on_cancel,
        )
