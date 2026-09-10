from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from scenario_pipeliner.worker.core.enums import TaskStatus, WaitingParentCloseStatus


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
    async def insert_result_and_update_task(
        self,
        *,
        task_id: int,
        result_json: str,
        status: TaskStatus,
        current_executions: int,
        next_run_at: datetime | None,
        payload_json: str | None,
    ) -> bool:
        """Insert a result row only if the task status update affected a row.

        Returns whether the task row was updated.
        """
        raise NotImplementedError

    @abstractmethod
    async def count_parent_subtasks_state(self, parent_id: int) -> tuple[int, int]:
        """Return (pending_open_children, failed_non_blocking) for parent."""
        raise NotImplementedError

    @abstractmethod
    async def close_waiting_parent(
        self, parent_id: int, status: WaitingParentCloseStatus
    ) -> bool:
        """Set parent to ``FINISHED`` / ``FINISHED_WITH_ERROR`` only if still ``WAITING``."""
        raise NotImplementedError

    @abstractmethod
    async def cancel_open_subtasks(self, parent_id: int) -> None:
        """Cancel NEW/QUEUED/RUNNING children for parent. Terminal rows are left."""
        raise NotImplementedError

    @abstractmethod
    async def setting_is_on(self, key: str) -> bool:
        """True only when ``settings.value`` trims to ``1``."""
        raise NotImplementedError

    @abstractmethod
    async def touch_heartbeats(self, task_ids: Sequence[int]) -> None:
        """Refresh ``last_heartbeat_at`` for QUEUED/RUNNING ids (lease)."""
        raise NotImplementedError

    @abstractmethod
    async def set_task_running(
        self,
        task_id: int,
        *,
        alias: str | None,
        steps_names: list[str] | None,
    ) -> None:
        """Set RUNNING, heartbeat, and optional alias/steps_names in one UPDATE."""
        raise NotImplementedError

    @abstractmethod
    async def fail_waiting_parent(self, parent_id: int) -> bool:
        """Set parent FAILED only if it is still WAITING."""
        raise NotImplementedError

    @abstractmethod
    async def requeue_incomplete_tasks(self, task_ids: Sequence[int]) -> int:
        """Set non-terminal rows to NEW and clear heartbeat. Return updated count."""
        raise NotImplementedError
