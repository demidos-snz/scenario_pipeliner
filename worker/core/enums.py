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
