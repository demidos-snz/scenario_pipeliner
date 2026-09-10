"""Enums stored on core Postgres tables.

Kept in ``db`` so schema/migrate consumers do not import ``worker``.
Worker modules re-export these names for plugin code.
"""

from __future__ import annotations

from enum import StrEnum


class TaskStatus(StrEnum):
    DRAFT = "DRAFT"
    NEW = "NEW"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    FINISHED = "FINISHED"
    FINISHED_WITH_ERROR = "FINISHED_WITH_ERROR"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class TaskSource(StrEnum):
    RABBITMQ = "RABBITMQ"
    INNER = "INNER"


class TaskType(StrEnum):
    LINEAR = "LINEAR"
    CYCLICAL = "CYCLICAL"


class BrokerOutboxStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SENT = "SENT"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
