import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.settings import Settings

logger = logging.getLogger(__name__)


class AsyncRunner(ABC):
    """Base runner that repeatedly executes a pipeline."""

    def __init__(self, pipeline: AsyncPipeline, settings: Settings):
        self.pipeline = pipeline
        self.settings = settings
        self._stop_event = asyncio.Event()
        self._pipeline_managed_by_runner = False

    @abstractmethod
    async def execute(self) -> None:
        pass

    def stop(self) -> None:
        self._stop_event.set()
        current_state = getattr(self, "current_state", None)
        if current_state is not None:
            current_state.cancel()

    async def _connect_extra_clients(self) -> None:
        return

    async def _disconnect_extra_clients(self) -> None:
        return

    @asynccontextmanager
    async def _pipeline_lifecycle(self) -> AsyncIterator[None]:
        if self._pipeline_managed_by_runner:
            yield
            return

        async with self.pipeline:
            yield

    async def __aenter__(self) -> "AsyncRunner":
        logger.info("Entering runner")
        await self._connect_extra_clients()
        await self.pipeline.__aenter__()
        self._pipeline_managed_by_runner = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        logger.info("Exiting runner")
        self._pipeline_managed_by_runner = False
        await self.pipeline.__aexit__(exc_type, exc_val, exc_tb)
        await self._disconnect_extra_clients()
