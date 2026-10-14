from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime
from typing import Any, NamedTuple

from scenario_pipeliner.worker.core.enums import TaskStatus, WaitingParentCloseStatus


class ParentSubtaskCounts(NamedTuple):
    """Child tallies used when a root task leaves ``RUNNING``."""

    pending_open: int
    failed_non_blocking: int
    failed_blocking: int


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
        rollup_parent: bool = False,
    ) -> bool:
        """Insert a result row only if the task status update affected a row.

        When ``rollup_parent`` is set and ``status`` is ``FINISHED``, child rows
        are locked in the same transaction and the stored status becomes
        ``FAILED`` (blocking child already failed), ``WAITING``, or
        ``FINISHED_WITH_ERROR``.

        Returns whether the task row was updated.
        """
        raise NotImplementedError

    @abstractmethod
    async def count_parent_subtasks_state(self, parent_id: int) -> ParentSubtaskCounts:
        """Return open, failed non-blocking, and failed blocking child counts.

        Soft-deleted children (``deleted_at`` set) are omitted.
        """
        raise NotImplementedError

    @abstractmethod
    async def close_waiting_parent(
        self, parent_id: int, status: WaitingParentCloseStatus
    ) -> bool:
        """Set parent to ``FINISHED`` / ``FINISHED_WITH_ERROR`` only if still ``WAITING``."""
        raise NotImplementedError

    @abstractmethod
    async def cancel_open_subtasks(self, parent_id: int) -> None:
        """Cancel NEW/QUEUED/RUNNING children for parent.

        Terminal rows and soft-deleted rows are left unchanged.
        """
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
    ) -> bool:
        """Set RUNNING only while the row is still ``QUEUED``.

        Returns whether the row was updated. A cancel that landed after the
        lock must stay ``CANCELLED``.
        """
        raise NotImplementedError

    @abstractmethod
    async def fail_waiting_parent(self, parent_id: int) -> bool:
        """Set parent FAILED only if it is still WAITING."""
        raise NotImplementedError

    @abstractmethod
    async def requeue_incomplete_tasks(self, task_ids: Sequence[int]) -> int:
        """Set non-terminal rows to NEW and clear heartbeat. Return updated count."""
        raise NotImplementedError
