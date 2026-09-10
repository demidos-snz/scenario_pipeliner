"""Periodic polling for active flags and task statuses."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from scenario_pipeliner.worker.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class ActiveFlagPoller:
    """Poll active flags with fail-closed behavior on DB failures."""

    def __init__(
        self,
        *,
        interval_seconds: int,
        should_continue: Callable[[], bool],
        check_main: Callable[[], Awaitable[bool]],
        on_db_failure: Callable[[], None],
        check_scenarios: Callable[[], Awaitable[None]] | None = None,
        check_tasks: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._interval_seconds = interval_seconds
        self._should_continue = should_continue
        self._check_main = check_main
        self._on_db_failure = on_db_failure
        self._check_scenarios = check_scenarios
        self._check_tasks = check_tasks

    async def run(self) -> None:
        while self._should_continue():
            try:
                if not await self._check_main():
                    break
                if self._check_scenarios is not None:
                    await self._check_scenarios()
                if self._check_tasks is not None:
                    await self._check_tasks()
            except DatabaseError:
                logger.error(
                    "Database error in active-flag poller; cancelling execution batch"
                )
                self._on_db_failure()
                break
            except Exception:
                logger.exception("Unexpected error in active-flag poller")
            await asyncio.sleep(self._interval_seconds)

    def start(self) -> asyncio.Task[None]:
        return asyncio.create_task(self.run(), name="active-flag-poller")
