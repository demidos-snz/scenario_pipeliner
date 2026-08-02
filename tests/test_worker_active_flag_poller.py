import asyncio

from scenario_pipeliner.worker.core.exceptions import DatabaseError
from scenario_pipeliner.worker.execution.active_flag_poller import (
    ActiveFlagPoller,
)


def test_active_flag_poller_runs_all_checks_while_active() -> None:
    checks: list[str] = []
    loops = {"count": 0}

    def should_continue() -> bool:
        loops["count"] += 1
        return loops["count"] <= 2

    async def check_main() -> bool:
        checks.append("main")
        return True

    async def check_scenarios() -> None:
        checks.append("scenarios")

    async def check_tasks() -> None:
        checks.append("tasks")

    poller = ActiveFlagPoller(
        interval_seconds=0,
        should_continue=should_continue,
        check_main=check_main,
        check_scenarios=check_scenarios,
        check_tasks=check_tasks,
        on_db_failure=lambda: None,
    )

    asyncio.run(poller.run())

    assert checks == ["main", "scenarios", "tasks", "main", "scenarios", "tasks"]


def test_active_flag_poller_stops_when_main_flag_is_inactive() -> None:
    checks: list[str] = []

    def should_continue() -> bool:
        return True

    async def check_main() -> bool:
        checks.append("main")
        return False

    async def check_scenarios() -> None:
        checks.append("scenarios")

    poller = ActiveFlagPoller(
        interval_seconds=0,
        should_continue=should_continue,
        check_main=check_main,
        check_scenarios=check_scenarios,
        on_db_failure=lambda: None,
    )

    asyncio.run(poller.run())

    assert checks == ["main"]


def test_active_flag_poller_calls_failure_callback_on_database_error() -> None:
    callbacks = {"db_failure": 0}
    checks = {"main": 0}

    def should_continue() -> bool:
        return True

    async def check_main() -> bool:
        checks["main"] += 1
        raise DatabaseError("db unavailable")

    def on_db_failure() -> None:
        callbacks["db_failure"] += 1

    poller = ActiveFlagPoller(
        interval_seconds=0,
        should_continue=should_continue,
        check_main=check_main,
        on_db_failure=on_db_failure,
    )

    asyncio.run(poller.run())

    assert checks["main"] == 1
    assert callbacks["db_failure"] == 1


def test_active_flag_poller_ignores_unexpected_errors_and_retries() -> None:
    attempts = {"count": 0}

    def should_continue() -> bool:
        return attempts["count"] < 2

    async def check_main() -> bool:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("transient non-db error")
        return True

    poller = ActiveFlagPoller(
        interval_seconds=0,
        should_continue=should_continue,
        check_main=check_main,
        on_db_failure=lambda: None,
    )

    asyncio.run(poller.run())

    assert attempts["count"] == 2


def test_active_flag_poller_start_returns_named_task() -> None:
    loops = {"count": 0}

    def should_continue() -> bool:
        loops["count"] += 1
        return loops["count"] <= 1

    async def check_main() -> bool:
        return True

    poller = ActiveFlagPoller(
        interval_seconds=0,
        should_continue=should_continue,
        check_main=check_main,
        on_db_failure=lambda: None,
    )

    async def _run():
        task = poller.start()
        await task
        return task

    task = asyncio.run(_run())

    assert task.get_name() == "active-flag-poller"
    assert task.done() is True
