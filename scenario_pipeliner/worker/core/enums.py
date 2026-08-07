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


class EnumDoc(StrEnum):
    XML = "XML"
    JSON = "JSON"


class ActiveFlag(StrEnum):
    """Статус рубильника pipeline_active / pipeline_active_{name}."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    MISSING = "missing"

    @property
    def is_active(self) -> bool:
        """Только явная запись value=1; MISSING трактуется как inactive."""
        return self is ActiveFlag.ACTIVE
