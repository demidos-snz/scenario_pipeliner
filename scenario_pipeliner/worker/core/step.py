from abc import ABC, abstractmethod
from typing import final

from scenario_pipeliner.worker.core.clients import AsyncClient
from scenario_pipeliner.worker.core.enums import TaskType
from scenario_pipeliner.worker.core.exceptions import PipelineCancelledError
from scenario_pipeliner.worker.core.settings import StepSettings
from scenario_pipeliner.worker.core.states import BaseState, TaskState


class AsyncStep[TStepState: BaseState, TStepSettings: StepSettings](ABC):
    """Async processing step in a pipeline."""

    def __init__(self, settings: TStepSettings):
        self.settings = settings
        self.initialized: bool = False
        self.is_custom: bool = False

    async def initialize(self) -> None:
        """Mark the step as ready and run subclass setup hooks."""
        self.initialized = True
        await self._on_initialize()

    async def _on_initialize(self) -> None:
        """Optional subclass hook invoked during initialize()."""
        return None

    @property
    @abstractmethod
    def clients(self) -> list[AsyncClient]:
        """Clients owned or used by this step."""
        return []

    @final
    async def run(self, state: TStepState) -> None:
        """Execute the step when should_run() allows it.

        Raises:
            PipelineCancelledError: If the task was cancelled before or after
                ``_run``.
        """
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
        """Core step logic implemented by subclasses."""

    async def finalize(self) -> None:
        """Tear down the step and clear the initialized flag."""
        await self._on_finalize()
        self.initialized = False

    async def _on_finalize(self) -> None:
        """Optional subclass hook invoked during finalize()."""
        return None

    async def should_run(self, state: TStepState) -> bool:
        """Return whether this step should execute for the given state.

        Raises:
            PipelineCancelledError: If the task is already cancelled.
        """
        if state.is_cancelled:
            raise PipelineCancelledError(
                f"Step {self.__class__.__name__} cancelled before start"
            )
        return True

    def __str__(self) -> str:
        return f"{self.__class__.__name__}".lower()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}".lower()


class BaseSubtaskStep[TSubtaskState: TaskState, TStepSettings: StepSettings](
    AsyncStep[TSubtaskState, TStepSettings],
    ABC,
):
    """Step that creates a subtask once, then runs cyclical subtask work."""

    @property
    @abstractmethod
    def clients(self) -> list[AsyncClient]:
        """Clients owned or used by this step."""
        return []

    async def should_run(self, state: TSubtaskState) -> bool:
        """Gate linear create-subtask on prior success; always run cyclical polls.

        ``TaskResult.ok`` defaults to ``False``. Linear parent steps must set
        ``ok=True`` before a tracking subtask is created. Cyclical iterations
        share the same pipeline but often skip those prior steps, so they must
        not be blocked by the default ``ok`` value.
        """
        if not await super().should_run(state=state):
            return False
        if state.type_task == TaskType.CYCLICAL:
            return True
        return state.result.ok

    async def _run(self, state: TSubtaskState) -> None:
        """Create a subtask for linear tasks; otherwise execute subtask logic."""
        if state.type_task != TaskType.CYCLICAL:
            await self._create_subtask(state=state)
        else:
            await self._run_subtask(state=state)

    @abstractmethod
    async def _create_subtask(self, state: TSubtaskState) -> None:
        """Create the tracking or child subtask for a parent task."""

    @abstractmethod
    async def _run_subtask(self, state: TSubtaskState) -> None:
        """Execute one cyclical iteration of the subtask."""
