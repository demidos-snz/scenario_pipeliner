"""Task repository protocol and backend implementations."""

from scenario_pipeliner.worker.task_repositories.postgres import (
    PostgresPoolProtocol,
    PostgresTaskRepository,
    PostgresTaskStorage,
)
from scenario_pipeliner.worker.task_repositories.protocol import TaskRepository
from scenario_pipeliner.worker.task_repositories.sqlite import SQLiteTaskRepositoryStub

__all__ = [
    "TaskRepository",
    "PostgresPoolProtocol",
    "PostgresTaskRepository",
    "PostgresTaskStorage",
    "SQLiteTaskRepositoryStub",
]
