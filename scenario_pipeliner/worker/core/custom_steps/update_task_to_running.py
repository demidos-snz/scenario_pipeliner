# fixme Any<-MainPipelineTasksTableDBClient?
from typing import Any

from scenario_pipeliner.worker.core.clients import AsyncClient
from scenario_pipeliner.worker.core.enums import TaskStatus
from scenario_pipeliner.worker.core.settings import TSettings
from scenario_pipeliner.worker.core.states import TaskState
from scenario_pipeliner.worker.core.step import AsyncStep


# if TYPE_CHECKING:
#     from main_pipeline.clients import MainPipelineTasksTableDBClient


class UpdateTaskToRunningStep(AsyncStep[TaskState, TSettings]):
    """Update task status to RUNNING."""

    def __init__(
        self,
        db_client: Any,
        settings: TSettings,
    ):
        super().__init__(settings=settings)
        self.db_client: Any = db_client

    @property
    def clients(self) -> list[AsyncClient]:
        return [self.db_client]

    async def _run(self, state: TaskState) -> None:
        await self.db_client.update_task_status(
            task_id=state.task_id,
            status=TaskStatus.RUNNING.value,
        )


def build_update_task_to_running_step(
    db_client: Any, settings: TSettings
) -> UpdateTaskToRunningStep:
    """Build UpdateTaskToRunningStep instance."""
    return UpdateTaskToRunningStep(db_client=db_client, settings=settings)
