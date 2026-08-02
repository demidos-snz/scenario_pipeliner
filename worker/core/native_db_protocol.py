from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any


class NativeTaskStorage(ABC):
    """Abstract native DB storage contract for task transitions."""

    @abstractmethod
    async def lock_runnable_tasks(
        self,
        *,
        limit: int,
        zombie_timeout_minutes: int,
    ) -> list[dict[str, Any]]:
        """Lock and return runnable task rows."""
        raise NotImplementedError

    @abstractmethod
    async def set_task_status(self, task_id: int, status: str) -> None:
        """Persist task status update."""
        raise NotImplementedError

    @abstractmethod
    async def set_task_alias(self, task_id: int, alias: str) -> None:
        """Persist task alias update."""
        raise NotImplementedError

    @abstractmethod
    async def insert_result_and_update_task(
        self,
        *,
        task_id: int,
        result_json: str,
        status: str,
        current_executions: int,
        next_run_at: datetime | None,
        payload_json: str | None,
    ) -> None:
        """Insert result row and update task row atomically."""
        raise NotImplementedError

    @abstractmethod
    async def count_parent_subtasks_state(self, parent_id: int) -> tuple[int, int]:
        """Return (pending_blocking, failed_non_blocking) for parent."""
        raise NotImplementedError

    @abstractmethod
    async def cancel_open_subtasks(self, parent_id: int) -> None:
        """Cancel NEW/QUEUED children for parent."""
        raise NotImplementedError
