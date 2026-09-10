import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine, Iterable
from contextlib import suppress
from typing import Any, cast

from scenario_pipeliner.worker.core.exceptions import PipelineCancelledError

logger = logging.getLogger(__name__)


class ExecutionBatchRunner:
    """Run a coroutine batch; apply shutdown timeout only after ``stop_event``."""

    def __init__(self, *, shutdown_timeout_seconds: float) -> None:
        self._shutdown_timeout_seconds = shutdown_timeout_seconds

    async def run_with_shutdown_timeout(
        self,
        coroutines: Iterable[Coroutine[Any, Any, None]],
        *,
        on_timeout: Callable[[], Awaitable[None]],
        names: Iterable[str] | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        name_iter = iter(names) if names is not None else None
        running_tasks: list[asyncio.Task[None]] = []
        for coro in coroutines:
            task_name = next(name_iter, None) if name_iter is not None else None
            running_tasks.append(
                asyncio.create_task(coro, name=task_name or "pipeline-task")
            )
        if not running_tasks:
            return

        gather_task = asyncio.ensure_future(asyncio.gather(*running_tasks))
        stop = stop_event if stop_event is not None else asyncio.Event()
        stop_waiter = asyncio.create_task(stop.wait())
        try:
            done, _pending = await asyncio.wait(
                cast(set[asyncio.Future[Any]], {gather_task, stop_waiter}),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if gather_task in done:
                await gather_task
                return

            logger.info(
                "Stop requested, waiting up to %ss for in-flight tasks",
                self._shutdown_timeout_seconds,
            )
            try:
                await asyncio.wait_for(
                    asyncio.shield(gather_task),
                    timeout=self._shutdown_timeout_seconds,
                )
            except TimeoutError:
                logger.warning(
                    "Shutdown timeout (%ss) waiting for tasks in batch, "
                    "forcing cancellation",
                    self._shutdown_timeout_seconds,
                )
                for task in running_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*running_tasks, return_exceptions=True)
                with suppress(asyncio.CancelledError):
                    await gather_task
                await on_timeout()
                raise PipelineCancelledError(
                    "Shutdown timeout waiting for tasks"
                ) from None
        finally:
            if not stop_waiter.done():
                stop_waiter.cancel()
                with suppress(asyncio.CancelledError):
                    await stop_waiter
            if not gather_task.done():
                gather_task.cancel()
            with suppress(asyncio.CancelledError):
                await gather_task
            for task in running_tasks:
                if not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
