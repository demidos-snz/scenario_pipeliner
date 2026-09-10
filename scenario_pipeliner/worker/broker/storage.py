"""Postgres storage for broker_ingress / broker_outbox (RFC-0004)."""

from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import and_, case, cast, func, insert, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from scenario_pipeliner.db.schema_utils import run_with_identity_sequence_retry
from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.broker.enums import BrokerOutboxStatus
from scenario_pipeliner.worker.broker.exceptions import DuplicateBrokerMessage
from scenario_pipeliner.worker.broker.settings import (
    BrokerIngressView,
    PluginBrokerSettings,
    ReplyDraft,
    ResultRowView,
    TaskDraft,
    TaskRowView,
)
from scenario_pipeliner.worker.core.enums import TaskSource, TaskStatus, TaskType
from scenario_pipeliner.worker.core.states import TaskPayload

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = (
    TaskStatus.FINISHED.value,
    TaskStatus.FINISHED_WITH_ERROR.value,
    TaskStatus.FAILED.value,
    TaskStatus.CANCELLED.value,
)


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row._mapping)


async def find_existing_ingress_task_id(
    db: CoreDb,
    *,
    ingress_queue: str,
    message_id: str,
) -> int | None:
    ingress = db.core.broker_ingress
    stmt = select(ingress.c.task_id).where(
        and_(
            ingress.c.ingress_queue == ingress_queue,
            ingress.c.message_id == message_id,
        )
    )
    async with db.engine.connect() as conn:
        value = (await conn.execute(stmt)).scalar_one_or_none()
    return int(value) if value is not None else None


async def insert_linear_task_with_ingress(
    db: CoreDb,
    *,
    plugin_name: str,
    draft: TaskDraft,
    ingress_queue: str,
    message_id: str,
    reply_to: str | None,
    content_type: str | None,
    headers: dict[str, Any] | None,
) -> int:
    """Insert LINEAR task + broker_ingress in one transaction."""
    tasks = db.core.tasks
    ingress = db.core.broker_ingress
    payload = draft.payload.model_dump(mode="json")
    alias = draft.alias or f"broker-{draft.scenario}"

    async def _insert() -> int:
        async with db.engine.begin() as conn:
            existing = (
                await conn.execute(
                    select(ingress.c.task_id).where(
                        and_(
                            ingress.c.ingress_queue == ingress_queue,
                            ingress.c.message_id == message_id,
                        )
                    )
                )
            ).scalar_one_or_none()
            if existing is not None:
                raise DuplicateBrokerMessage(task_id=int(existing))

            task_id = (
                await conn.execute(
                    insert(tasks)
                    .values(
                        scenario=draft.scenario,
                        status=TaskStatus.NEW.value,
                        source=TaskSource.RABBITMQ.value,
                        type_task=TaskType.LINEAR.value,
                        interval_seconds=1,
                        max_executions=draft.max_executions,
                        current_executions=0,
                        payload=payload,
                        alias=alias,
                        steps_names=[],
                    )
                    .returning(tasks.c.id)
                )
            ).scalar_one()
            task_id_int = int(task_id)
            try:
                async with conn.begin_nested():
                    await conn.execute(
                        insert(ingress).values(
                            task_id=task_id_int,
                            plugin_name=plugin_name,
                            scenario=draft.scenario,
                            ingress_queue=ingress_queue,
                            message_id=message_id,
                            reply_to=reply_to,
                            content_type=content_type,
                            headers=headers,
                        )
                    )
            except IntegrityError:
                raced = (
                    await conn.execute(
                        select(ingress.c.task_id).where(
                            and_(
                                ingress.c.ingress_queue == ingress_queue,
                                ingress.c.message_id == message_id,
                            )
                        )
                    )
                ).scalar_one_or_none()
                if raced is not None:
                    raise DuplicateBrokerMessage(task_id=int(raced)) from None
                raise
            return task_id_int

    return await run_with_identity_sequence_retry(db, _insert)


async def list_terminal_tasks_missing_outbox(
    db: CoreDb,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Return terminal broker tasks that still have no outbox row (unlocked)."""
    tasks = db.core.tasks
    ingress = db.core.broker_ingress
    outbox = db.core.broker_outbox
    stmt = (
        select(tasks.c.id.label("task_id"), ingress.c.id.label("ingress_id"))
        .join(ingress, ingress.c.task_id == tasks.c.id)
        .outerjoin(outbox, outbox.c.task_id == tasks.c.id)
        .where(
            and_(
                tasks.c.status.in_(_TERMINAL_STATUSES),
                tasks.c.deleted_at.is_(None),
                outbox.c.id.is_(None),
            )
        )
        .order_by(tasks.c.updated_at.asc().nulls_last(), tasks.c.id.asc())
        .limit(limit)
    )
    async with db.engine.connect() as conn:
        rows = (await conn.execute(stmt)).all()
    return [
        {"task_id": int(row.task_id), "ingress_id": int(row.ingress_id)} for row in rows
    ]


async def insert_outbox_for_terminal_tasks(
    db: CoreDb,
    *,
    limit: int = 50,
) -> int:
    """Insert PENDING outbox rows for terminal broker tasks.

    Locks candidate ``tasks`` rows with ``FOR UPDATE SKIP LOCKED`` and inserts
    in the same transaction so two reconcilers do not race the same work.
    Unique ``task_id`` is the second line of defence.
    """
    tasks = db.core.tasks
    ingress = db.core.broker_ingress
    outbox = db.core.broker_outbox
    candidates = (
        select(tasks.c.id.label("task_id"), ingress.c.id.label("ingress_id"))
        .join(ingress, ingress.c.task_id == tasks.c.id)
        .outerjoin(outbox, outbox.c.task_id == tasks.c.id)
        .where(
            and_(
                tasks.c.status.in_(_TERMINAL_STATUSES),
                tasks.c.deleted_at.is_(None),
                outbox.c.id.is_(None),
            )
        )
        .order_by(tasks.c.updated_at.asc().nulls_last(), tasks.c.id.asc())
        .limit(limit)
        .with_for_update(of=tasks, skip_locked=True)
    )
    created = 0
    async with db.engine.begin() as conn:
        rows = (await conn.execute(candidates)).all()
        for row in rows:
            result = await conn.execute(
                pg_insert(outbox)
                .values(
                    task_id=int(row.task_id),
                    ingress_id=int(row.ingress_id),
                    status=BrokerOutboxStatus.PENDING.value,
                )
                .on_conflict_do_nothing(index_elements=[outbox.c.task_id])
            )
            if int(result.rowcount or 0) == 1:
                created += 1
    return created


async def ensure_outbox_pending(
    db: CoreDb,
    *,
    task_id: int,
    ingress_id: int,
) -> bool:
    outbox = db.core.broker_outbox
    stmt = (
        pg_insert(outbox)
        .values(
            task_id=task_id,
            ingress_id=ingress_id,
            status=BrokerOutboxStatus.PENDING.value,
        )
        .on_conflict_do_nothing(index_elements=[outbox.c.task_id])
    )
    async with db.engine.begin() as conn:
        result = await conn.execute(stmt)
    return int(result.rowcount or 0) == 1


async def claim_pending_outbox(
    db: CoreDb,
    *,
    limit: int = 20,
    stale_processing_minutes: int | None = None,
) -> list[dict[str, Any]]:
    """Atomically claim PENDING (and stale PROCESSING) rows.

    Sets ``status=PROCESSING`` with ``FOR UPDATE SKIP LOCKED`` so two broker
    processes cannot pick the same row. ``PROCESSING`` older than
    ``stale_processing_minutes`` is treated as abandoned after a crash.
    Default lease matches ``PluginBrokerSettings.stale_processing_minutes``.
    """
    if stale_processing_minutes is None:
        stale_processing_minutes = PluginBrokerSettings.outbox_stale_processing_minutes(
            {}
        )
    outbox = db.core.broker_outbox
    stale = or_(
        outbox.c.status == BrokerOutboxStatus.PENDING.value,
        and_(
            outbox.c.status == BrokerOutboxStatus.PROCESSING.value,
            func.coalesce(outbox.c.updated_at, outbox.c.created_at)
            < func.now() - func.make_interval(0, 0, 0, 0, 0, stale_processing_minutes),
        ),
    )
    inner = (
        select(outbox.c.id)
        .where(stale)
        .order_by(outbox.c.created_at.asc(), outbox.c.id.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    stmt = (
        update(outbox)
        .where(outbox.c.id.in_(inner))
        .values(
            status=BrokerOutboxStatus.PROCESSING.value,
            updated_at=func.now(),
        )
        .returning(
            outbox.c.id.label("outbox_id"),
            outbox.c.task_id,
            outbox.c.ingress_id,
            outbox.c.status,
            outbox.c.body,
            outbox.c.content_type,
            outbox.c.routing_queues,
            outbox.c.headers,
            outbox.c.attempt_count,
        )
    )
    async with db.engine.begin() as conn:
        rows = (await conn.execute(stmt)).all()
    return [_mapping(row) for row in rows]


async def load_task_result_ingress(
    db: CoreDb,
    *,
    task_id: int,
    ingress_id: int,
) -> tuple[TaskRowView, ResultRowView | None, BrokerIngressView]:
    tasks = db.core.tasks
    results = db.core.results
    ingress = db.core.broker_ingress
    async with db.engine.connect() as conn:
        task_row = (
            (
                await conn.execute(
                    select(
                        tasks.c.id,
                        tasks.c.scenario,
                        tasks.c.status,
                        tasks.c.payload,
                        tasks.c.alias,
                    ).where(tasks.c.id == task_id)
                )
            )
            .mappings()
            .one_or_none()
        )
        if task_row is None:
            raise LookupError(f"task {task_id} not found")
        result_row = (
            (
                await conn.execute(
                    select(results.c.id, results.c.task_id, results.c.result)
                    .where(results.c.task_id == task_id)
                    .order_by(results.c.id.desc())
                    .limit(1)
                )
            )
            .mappings()
            .one_or_none()
        )
        ingress_row = (
            (
                await conn.execute(
                    select(
                        ingress.c.id,
                        ingress.c.task_id,
                        ingress.c.plugin_name,
                        ingress.c.scenario,
                        ingress.c.ingress_queue,
                        ingress.c.message_id,
                        ingress.c.reply_to,
                        ingress.c.content_type,
                        ingress.c.headers,
                    ).where(ingress.c.id == ingress_id)
                )
            )
            .mappings()
            .one_or_none()
        )
        if ingress_row is None:
            raise LookupError(f"broker_ingress {ingress_id} not found")

    payload = task_row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    payload_view: TaskPayload | None = None
    if isinstance(payload, dict):
        payload_view = TaskPayload.model_validate(payload)
    elif isinstance(payload, TaskPayload):
        payload_view = payload

    result_view: ResultRowView | None = None
    if result_row is not None:
        result_payload = result_row["result"]
        if isinstance(result_payload, str):
            result_payload = json.loads(result_payload)
        result_view = ResultRowView(
            result_id=int(result_row["id"]),
            task_id=int(result_row["task_id"]),
            result=result_payload,
        )

    headers = ingress_row["headers"]
    if isinstance(headers, str):
        headers = json.loads(headers)

    return (
        TaskRowView(
            task_id=int(task_row["id"]),
            scenario=str(task_row["scenario"]),
            status=str(task_row["status"]),
            payload=payload_view,
            alias=task_row["alias"],
        ),
        result_view,
        BrokerIngressView(
            ingress_id=int(ingress_row["id"]),
            task_id=int(ingress_row["task_id"]),
            plugin_name=str(ingress_row["plugin_name"]),
            scenario=str(ingress_row["scenario"]),
            ingress_queue=str(ingress_row["ingress_queue"]),
            message_id=str(ingress_row["message_id"]),
            reply_to=ingress_row["reply_to"],
            content_type=ingress_row["content_type"],
            headers=headers if isinstance(headers, dict) else None,
        ),
    )


async def mark_outbox_skipped(db: CoreDb, *, outbox_id: int) -> None:
    outbox = db.core.broker_outbox
    async with db.engine.begin() as conn:
        await conn.execute(
            update(outbox)
            .where(
                and_(
                    outbox.c.id == outbox_id,
                    outbox.c.status == BrokerOutboxStatus.PROCESSING.value,
                )
            )
            .values(status=BrokerOutboxStatus.SKIPPED.value, updated_at=func.now())
        )


async def store_outbox_reply(
    db: CoreDb,
    *,
    outbox_id: int,
    draft: ReplyDraft,
    routing_queues: list[str],
) -> None:
    outbox = db.core.broker_outbox
    content_type = draft.content_type or "application/json"
    async with db.engine.begin() as conn:
        await conn.execute(
            update(outbox)
            .where(
                and_(
                    outbox.c.id == outbox_id,
                    outbox.c.status == BrokerOutboxStatus.PROCESSING.value,
                )
            )
            .values(
                body=draft.body,
                content_type=content_type,
                routing_queues=routing_queues,
                headers=draft.headers,
                updated_at=func.now(),
            )
        )


async def mark_outbox_sent(db: CoreDb, *, outbox_id: int) -> None:
    outbox = db.core.broker_outbox
    async with db.engine.begin() as conn:
        await conn.execute(
            update(outbox)
            .where(
                and_(
                    outbox.c.id == outbox_id,
                    outbox.c.status == BrokerOutboxStatus.PROCESSING.value,
                )
            )
            .values(
                status=BrokerOutboxStatus.SENT.value,
                sent_at=func.now(),
                updated_at=func.now(),
            )
        )


async def mark_outbox_publish_failure(
    db: CoreDb,
    *,
    outbox_id: int,
    error: str,
    max_attempts: int = 10,
) -> None:
    outbox = db.core.broker_outbox
    async with db.engine.begin() as conn:
        await conn.execute(
            update(outbox)
            .where(
                and_(
                    outbox.c.id == outbox_id,
                    outbox.c.status == BrokerOutboxStatus.PROCESSING.value,
                )
            )
            .values(
                attempt_count=outbox.c.attempt_count + 1,
                last_error=error[:2000],
                status=case(
                    (
                        (outbox.c.attempt_count + 1) >= max_attempts,
                        cast(
                            BrokerOutboxStatus.FAILED.value,
                            outbox.c.status.type,
                        ),
                    ),
                    else_=cast(
                        BrokerOutboxStatus.PENDING.value,
                        outbox.c.status.type,
                    ),
                ),
                updated_at=func.now(),
            )
        )
