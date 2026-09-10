from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, ENUM, JSONB

from scenario_pipeliner.db.enums import (
    BrokerOutboxStatus,
    TaskSource,
    TaskStatus,
    TaskType,
)
from scenario_pipeliner.db.schemes_names import validate_schema_name
from scenario_pipeliner.db.settings import CoreSchema


def _schema_enum(schema: str, name: str, values: tuple[str, ...]) -> ENUM:
    return ENUM(
        *values,
        name=name,
        schema=schema,
        inherit_schema=True,
        create_type=False,
    )


def build_core_schema(schema: str) -> CoreSchema:
    schema_name = validate_schema_name(schema)
    metadata = MetaData(schema=schema_name)
    task_status = _schema_enum(
        schema_name, "task_status", tuple(member.value for member in TaskStatus)
    )
    task_source = _schema_enum(
        schema_name, "task_source", tuple(member.value for member in TaskSource)
    )
    task_type = _schema_enum(
        schema_name, "task_type", tuple(member.value for member in TaskType)
    )
    outbox_status = _schema_enum(
        schema_name,
        "broker_outbox_status",
        tuple(member.value for member in BrokerOutboxStatus),
    )

    tasks = Table(
        "tasks",
        metadata,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("scenario", String, nullable=False),
        Column(
            "status",
            task_status,
            nullable=False,
            server_default=text(f"'{TaskStatus.NEW.value}'"),
        ),
        Column(
            "source",
            task_source,
            nullable=False,
            server_default=text(f"'{TaskSource.INNER.value}'"),
        ),
        Column(
            "type_task",
            task_type,
            nullable=False,
            server_default=text(f"'{TaskType.LINEAR.value}'"),
        ),
        Column("interval_seconds", Integer, nullable=False, server_default=text("1")),
        Column("max_executions", Integer, nullable=True),
        Column("current_executions", Integer, nullable=False, server_default=text("0")),
        Column("next_run_at", DateTime(timezone=True), nullable=True),
        Column("last_heartbeat_at", DateTime(timezone=True), nullable=True),
        Column("is_block", Boolean, nullable=False, server_default=text("FALSE")),
        Column(
            "parent_id",
            BigInteger,
            ForeignKey(f"{schema_name}.tasks.id"),
            nullable=True,
        ),
        Column("payload", JSONB, nullable=True),
        Column("alias", String, nullable=True),
        Column(
            "steps_names",
            ARRAY(String),
            nullable=False,
            server_default=text("'{}'::varchar[]"),
        ),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        ),
        Column("deleted_at", DateTime(timezone=True), nullable=True),
    )
    Index("ix_tasks_status", tasks.c.status)
    Index("ix_tasks_parent_id", tasks.c.parent_id)
    Index("ix_tasks_next_run_at", tasks.c.next_run_at)
    Index("ix_tasks_last_heartbeat_at", tasks.c.last_heartbeat_at)
    Index("ix_tasks_alias", tasks.c.alias)

    settings = Table(
        "settings",
        metadata,
        Column("key", String, primary_key=True),
        Column("value", String, nullable=False),
    )

    results = Table(
        "results",
        metadata,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column(
            "task_id",
            BigInteger,
            ForeignKey(f"{schema_name}.tasks.id"),
            nullable=False,
        ),
        Column("result", JSONB, nullable=False),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        ),
    )
    Index("ix_results_task_id", results.c.task_id)

    broker_ingress = Table(
        "broker_ingress",
        metadata,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column(
            "task_id",
            BigInteger,
            ForeignKey(f"{schema_name}.tasks.id"),
            nullable=False,
            unique=True,
        ),
        Column("plugin_name", String, nullable=False),
        Column("scenario", String, nullable=False),
        Column("ingress_queue", String, nullable=False),
        Column("message_id", String, nullable=False),
        Column("reply_to", String, nullable=True),
        Column("content_type", String, nullable=True),
        Column("headers", JSONB, nullable=True),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        ),
    )
    Index("ix_broker_ingress_task_id", broker_ingress.c.task_id)
    Index(
        "uq_broker_ingress_queue_message",
        broker_ingress.c.ingress_queue,
        broker_ingress.c.message_id,
        unique=True,
    )

    broker_outbox = Table(
        "broker_outbox",
        metadata,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column(
            "task_id",
            BigInteger,
            ForeignKey(f"{schema_name}.tasks.id"),
            nullable=False,
            unique=True,
        ),
        Column(
            "ingress_id",
            BigInteger,
            ForeignKey(f"{schema_name}.broker_ingress.id"),
            nullable=True,
        ),
        Column(
            "status",
            outbox_status,
            nullable=False,
            server_default=text("'PENDING'"),
        ),
        Column("body", LargeBinary, nullable=True),
        Column("content_type", String, nullable=True),
        Column(
            "routing_queues",
            ARRAY(String),
            nullable=False,
            server_default=text("'{}'::varchar[]"),
        ),
        Column("headers", JSONB, nullable=True),
        Column("attempt_count", Integer, nullable=False, server_default=text("0")),
        Column("last_error", String, nullable=True),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        ),
        Column("sent_at", DateTime(timezone=True), nullable=True),
    )
    Index(
        "ix_broker_outbox_status_created",
        broker_outbox.c.status,
        broker_outbox.c.created_at,
    )

    plugin_migrations = Table(
        "plugin_migrations",
        metadata,
        Column("plugin_name", String, primary_key=True),
        Column("migration_order", String, nullable=False),
        Column("sql_sha256", String, nullable=False),
        Column(
            "applied_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        ),
    )

    return CoreSchema(
        schema_name=schema_name,
        metadata=metadata,
        tasks=tasks,
        settings=settings,
        results=results,
        broker_ingress=broker_ingress,
        broker_outbox=broker_outbox,
        plugin_migrations=plugin_migrations,
        task_status=task_status,
        task_source=task_source,
        task_type=task_type,
        outbox_status=outbox_status,
    )
