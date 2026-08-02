import asyncio
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import Token

from scenario_pipeliner.worker.core.exceptions import PipelineCancelledError
from scenario_pipeliner.worker.core.settings import PipelineSettings
from scenario_pipeliner.worker.core.states import BaseState
from scenario_pipeliner.worker.core.step import AsyncStep
from scenario_pipeliner.worker.core.utils import _CANCEL_EVENT_CTX

logger = logging.getLogger(__name__)


class AsyncPipeline:
    def __init__(
        self,
        steps: list[AsyncStep],
        settings: PipelineSettings | None = None,
    ):
        self.steps: list[AsyncStep] = [s for s in steps if isinstance(s, AsyncStep)]
        self.settings: PipelineSettings = settings or PipelineSettings()

    async def execute(self, state: BaseState | None = None) -> None:
        state = self.__validate_state(state=state)
        with self.bind_cancel_event(state.cancel_event):
            for step in self.steps:
                if state.is_cancelled:
                    raise PipelineCancelledError("Pipeline was cancelled by token")

                if not step.initialized:
                    raise RuntimeError(
                        f"Step {step.__class__.__name__} must be initialized before run"
                    )

                await step.run(state=state)

    @contextmanager
    def bind_cancel_event(self, event: asyncio.Event | None) -> Iterator[None]:
        if event is None:
            yield
            return

        token: Token[asyncio.Event | None] = _CANCEL_EVENT_CTX.set(event)
        try:
            yield
        finally:
            _CANCEL_EVENT_CTX.reset(token)

    @staticmethod
    def __validate_state(state: BaseState | None = None) -> BaseState:
        if state is None:
            state = BaseState()
        if not isinstance(state, BaseState):
            raise ValueError("State must be an instance of BaseState")
        return state

    async def connect_clients(self) -> None:
        seen: set[int] = set()
        for step in self.steps:
            for client in step.clients:
                key = id(client)
                if key in seen:
                    continue
                seen.add(key)
                if not client.initialized:
                    await client.connect()

    async def disconnect_clients(self) -> None:
        seen: set[int] = set()
        for step in self.steps:
            for client in step.clients:
                key = id(client)
                if key in seen:
                    continue
                seen.add(key)
                if client.initialized:
                    await client.disconnect()

    async def finalize(self) -> None:
        for step in self.steps:
            if step.initialized:
                await step.finalize()
            else:
                logger.warning(
                    "Step %s is not initialized, skipping finalize",
                    step.__class__.__name__,
                )

    async def __aenter__(self) -> "AsyncPipeline":
        if self.settings.AUTORUN_CONNECT_CLIENTS:
            await self.connect_clients()
        for step in self.steps:
            if not step.initialized:
                await step.initialize()
        return self

    async def __aexit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> None:
        await self.finalize()
        if self.settings.AUTORUN_CONNECT_CLIENTS:
            await self.disconnect_clients()
