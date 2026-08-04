import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine, Iterable
from contextlib import suppress
from typing import Any

from scenario_pipeliner.worker.core.exceptions import PipelineCancelledError

logger = logging.getLogger(__name__)


class ExecutionBatchRunner:
    """Run a coroutine batch with shutdown-timeout semantics."""

    def __init__(self, *, shutdown_timeout_seconds: float) -> None:
        self._shutdown_timeout_seconds = shutdown_timeout_seconds

    async def run_with_shutdown_timeout(
        self,
        coroutines: Iterable[Coroutine[Any, Any, None]],
        *,
        on_timeout: Callable[[], Awaitable[None]],
        names: Iterable[str] | None = None,
    ) -> None:
        name_iter = iter(names) if names is not None else None
        running_tasks: list[asyncio.Task[None]] = []
        for coro in coroutines:
            task_name = next(name_iter, None) if name_iter is not None else None
            running_tasks.append(
                asyncio.create_task(coro, name=task_name or "pipeline-task")
            )

        try:
            await asyncio.wait_for(
                asyncio.gather(*running_tasks),
                timeout=self._shutdown_timeout_seconds,
            )
        except TimeoutError:
            logger.warning(
                "Shutdown timeout (%ss) waiting for tasks in batch, forcing cancellation",
                self._shutdown_timeout_seconds,
            )
            for task in running_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*running_tasks, return_exceptions=True)
            await on_timeout()
            raise PipelineCancelledError("Shutdown timeout waiting for tasks") from None
        finally:
            for task in running_tasks:
                if not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
