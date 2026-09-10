"""Worker package: execution, plugins, runtime, and task repositories."""

from __future__ import annotations

from typing import Any

__all__ = [
    "RunnerApp",
    "CyclicalTaskSeed",
    "TaskSeed",
    "ensure_worker_settings",
    "create_cyclical_task",
    "create_task",
    "build_postgres_runner",
    "fetch_task_snapshot",
    "TaskRuntimeSnapshot",
]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from scenario_pipeliner.worker import runtime as _runtime

        return getattr(_runtime, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
