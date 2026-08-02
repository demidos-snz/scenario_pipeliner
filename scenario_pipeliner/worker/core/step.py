from abc import ABC, abstractmethod
from typing import final

from scenario_pipeliner.worker.core.clients import AsyncClient
from scenario_pipeliner.worker.core.enums import TaskType
from scenario_pipeliner.worker.core.exceptions import PipelineCancelledError
from scenario_pipeliner.worker.core.settings import StepSettings
from scenario_pipeliner.worker.core.states import BaseState, TaskState


class AsyncStep[TStepState: BaseState, TStepSettings: StepSettings](ABC):
    def __init__(self, settings: TStepSettings):
        self.settings = settings
        self.initialized: bool = False
        self.is_custom: bool = False

    async def initialize(self) -> None:
        self.initialized = True
        await self._on_initialize()

    async def _on_initialize(self) -> None:
        return

    @property
    @abstractmethod
    def clients(self) -> list[AsyncClient]:
        return []

    @final
    async def run(self, state: TStepState) -> None:
        if not await self.should_run(state=state):
            return

        if state.is_cancelled:
            raise PipelineCancelledError(
                f"Step {self.__class__.__name__} cancelled before run"
            )

        await self._run(state=state)

        if state.is_cancelled:
            raise PipelineCancelledError(
                f"Step {self.__class__.__name__} cancelled after run"
            )

    @abstractmethod
    async def _run(self, state: TStepState) -> None:
        pass

    async def finalize(self) -> None:
        await self._on_finalize()
        self.initialized = False

    async def _on_finalize(self) -> None:
        return

    async def should_run(self, state: TStepState) -> bool:
        if state.is_cancelled:
            raise PipelineCancelledError(
                f"Step {self.__class__.__name__} cancelled before start"
            )
        return True


class BaseSubtaskStep[TSubtaskState: TaskState, TStepSettings: StepSettings](
    AsyncStep[TSubtaskState, TStepSettings],
    ABC,
):
    @property
    @abstractmethod
    def clients(self) -> list[AsyncClient]:
        return []

    async def should_run(self, state: TSubtaskState) -> bool:
        if not await super().should_run(state=state):
            return False
        return state.result.ok

    async def _run(self, state: TSubtaskState) -> None:
        if state.type_task != TaskType.CYCLICAL:
            await self._create_subtask(state=state)
        else:
            await self._run_subtask(state=state)

    @abstractmethod
    async def _create_subtask(self, state: TSubtaskState) -> None:
        pass

    @abstractmethod
    async def _run_subtask(self, state: TSubtaskState) -> None:
        pass
