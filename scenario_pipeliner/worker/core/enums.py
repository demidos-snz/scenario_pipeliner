from enum import StrEnum
from typing import Literal

from scenario_pipeliner.db.enums import TaskSource, TaskStatus, TaskType

__all__ = [
    "EnumDoc",
    "TaskSource",
    "TaskStatus",
    "TaskType",
    "WaitingParentCloseStatus",
    "as_waiting_parent_close_status",
]


WaitingParentCloseStatus = Literal[
    TaskStatus.FINISHED,
    TaskStatus.FINISHED_WITH_ERROR,
]

_WAITING_PARENT_CLOSE_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.FINISHED, TaskStatus.FINISHED_WITH_ERROR}
)


def as_waiting_parent_close_status(status: TaskStatus) -> WaitingParentCloseStatus:
    if status not in _WAITING_PARENT_CLOSE_STATUSES:
        raise ValueError(
            "close_waiting_parent status must be FINISHED or "
            f"FINISHED_WITH_ERROR, got {status!r}"
        )
    return status  # type: ignore[return-value]


class EnumDoc(StrEnum):
    XML = "XML"
    JSON = "JSON"
