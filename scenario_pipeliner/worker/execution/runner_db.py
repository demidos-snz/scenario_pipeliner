import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress

from scenario_pipeliner.worker.core.exceptions import PipelineCancelledError
from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.runner import AsyncRunner
from scenario_pipeliner.worker.core.settings import RunnerDBSettings
from scenario_pipeliner.worker.execution.db_orchestrator import (
    DBExecutionOrchestrator,
    ExecutionBatchState,
    TaskErrorHandler,
    TaskStateHandler,
    TimeoutHandler,
)

FetchBatchState = Callable[[], Awaitable[ExecutionBatchState]]


class RunnerDB(AsyncRunner):
    """Prototype DB runner loop for refactor track."""

    def __init__(
        self,
        *,
        pipeline: AsyncPipeline,
        settings: RunnerDBSettings,
        fetch_batch_state: FetchBatchState,
        orchestrator: DBExecutionOrchestrator,
        on_task_start: TaskStateHandler | None = None,
        on_task_success: TaskStateHandler | None = None,
        on_task_error: TaskErrorHandler | None = None,
        on_timeout: TimeoutHandler | None = None,
    ) -> None:
        super().__init__(pipeline=pipeline, settings=settings)
        self._settings = settings
        self._fetch_batch_state = fetch_batch_state
        self._orchestrator = orchestrator
        self._on_task_start = on_task_start
        self._on_task_success = on_task_success
        self._on_task_error = on_task_error
        self._on_timeout = on_timeout
        self.current_state: ExecutionBatchState | None = None
        self._consecutive_errors: int = 0

    def _error_backoff_seconds(self) -> int:
        return min(
            self._settings.ERROR_BACKOFF_INITIAL_SECONDS
            * (2 ** (self._consecutive_errors - 1)),
            self._settings.ERROR_BACKOFF_MAX_SECONDS,
        )

    async def _wait_before_next_poll(self, seconds: int) -> None:
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)

    async def _run_single_cycle(self) -> None:
        state = await self._fetch_batch_state()
        self.current_state = state
        try:
            await self._orchestrator.execute(
                state,
                on_task_start=self._on_task_start,
                on_task_success=self._on_task_success,
                on_task_error=self._on_task_error,
                on_timeout=self._on_timeout,
            )
        finally:
            self.current_state = None

    async def execute(self) -> None:
        async with self._pipeline_lifecycle():
            while not self._stop_event.is_set():
                wait_seconds = self._settings.POLL_INTERVAL_SECONDS
                try:
                    await self._run_single_cycle()
                    self._consecutive_errors = 0
                except PipelineCancelledError:
                    # Cancel signal during shutdown/timeout is expected in runner loop.
                    pass
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._consecutive_errors += 1
                    wait_seconds = self._error_backoff_seconds()

                if self._stop_event.is_set():
                    break
                await self._wait_before_next_poll(wait_seconds)
