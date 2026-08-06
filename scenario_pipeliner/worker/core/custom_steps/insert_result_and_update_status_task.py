# fixme Any<-MainPipelineTasksTableDBClient?
import datetime
from typing import Any

from scenario_pipeliner.worker.core.clients import AsyncClient
from scenario_pipeliner.worker.core.enums import TaskStatus, TaskType
from scenario_pipeliner.worker.core.settings import TSettings
from scenario_pipeliner.worker.core.states import TaskState
from scenario_pipeliner.worker.core.step import AsyncStep
from scenario_pipeliner.worker.core.utils import get_params_for_cyclical_task

# if TYPE_CHECKING:
#     from main_pipeline.clients import MainPipelineTasksTableDBClient


class InsertResultAndUpdateStatusTaskStep(AsyncStep[TaskState, TSettings]):
    """Insert result and update status task."""

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
        status = (
            TaskStatus.FINISHED.value if state.result.ok else TaskStatus.FAILED.value
        )
        status, current_executions, next_run_at = get_params_for_cyclical_task(
            status, state
        )

        # Циклическая сабтаска, которая не завершилась и ребьётся в NEW:
        # персистим обновлённый payload (с продвинутым курсором afterIndexKey),
        # иначе следующий lock прочитает старый payload и начнёт сканирование с начала.
        payload_json: str | None = None
        if (
            state.type_task == TaskType.CYCLICAL
            and status == TaskStatus.NEW.value
            and state.payload is not None
        ):
            payload_json = state.payload.model_dump_json(exclude_none=True)

        if state.parent_id is None:
            await self._handle_parent_task_completion(
                state, status, current_executions, next_run_at, payload_json
            )
        else:
            await self._handle_subtask_completion(
                state, status, current_executions, next_run_at, payload_json
            )

    async def _handle_subtask_completion(
        self,
        state: TaskState,
        status: str,
        current_executions: int,
        next_run_at: datetime.datetime | None,
        payload_json: str | None = None,
    ) -> None:
        await self.db_client.insert_result_and_update_status_task(
            task_id=state.task_id,
            result=state.result.model_dump_json(),
            status=status,
            current_executions=current_executions,
            next_run_at=next_run_at,
            payload=payload_json,
        )

        if status == TaskStatus.FAILED.value:
            await self.db_client.apply_subtask_failure_to_parent(subtask=state)
        elif status == TaskStatus.FINISHED.value and state.is_block:
            await self.db_client.apply_blocking_subtask_finished_to_parent(
                subtask=state
            )

    async def _handle_parent_task_completion(
        self,
        state: TaskState,
        status: str,
        current_executions: int,
        next_run_at: datetime.datetime | None,
        payload_json: str | None = None,
    ) -> None:
        if status == TaskStatus.FINISHED.value:
            (
                pending_blocking,
                failed_non_blocking,
            ) = await self.db_client.check_subtasks_status(state.task_id)
            if pending_blocking > 0:
                status = TaskStatus.WAITING.value
            elif failed_non_blocking > 0:
                status = TaskStatus.FINISHED_WITH_ERROR.value

        await self.db_client.insert_result_and_update_status_task(
            task_id=state.task_id,
            result=state.result.model_dump_json(),
            status=status,
            current_executions=current_executions,
            next_run_at=next_run_at,
            payload=payload_json,
        )


def build_insert_result_and_update_status_task_step(
    db_client: Any, settings: TSettings
) -> InsertResultAndUpdateStatusTaskStep:
    """Build InsertResultAndUpdateStatusTaskStep instance."""
    return InsertResultAndUpdateStatusTaskStep(db_client=db_client, settings=settings)
