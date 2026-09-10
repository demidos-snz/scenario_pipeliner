"""Ingress message handling (RFC-0004)."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.broker.exceptions import (
    DuplicateBrokerMessage,
    IngressReject,
    IngressRetry,
)
from scenario_pipeliner.worker.broker.settings import BrokerMessageMeta, TaskDraft
from scenario_pipeliner.worker.broker.storage import insert_linear_task_with_ingress
from scenario_pipeliner.worker.broker.utils import resolve_message_id
from scenario_pipeliner.worker.broker.worker_plugin_broker_protocol import (
    WorkerPluginBrokerHooks,
)

logger = logging.getLogger(__name__)

AckCallback = Callable[[], Awaitable[None]]
NackCallback = Callable[[bool], Awaitable[None]]  # requeue: bool


def ingress_delivery_attempt(meta: BrokerMessageMeta) -> int:
    """1-based delivery attempt for the requeue cap.

    Prefers RabbitMQ ``x-delivery-count`` (quorum) or ``x-death`` counts.
    Classic queues only expose ``redelivered``: first delivery is 1, any
    redelivery is 2, so the cap is effectively 2 without those headers.
    """
    headers = meta.headers
    raw_count = headers.get("x-delivery-count")
    if raw_count is not None:
        try:
            return int(raw_count) + 1
        except (TypeError, ValueError):
            pass
    x_death = headers.get("x-death")
    if isinstance(x_death, (list, tuple)):
        total = 0
        for entry in x_death:
            if not isinstance(entry, dict):
                continue
            raw_entry = entry.get("count")
            if raw_entry is None:
                continue
            try:
                total += int(raw_entry)
            except (TypeError, ValueError):
                continue
        if total:
            return total
    return 2 if meta.redelivered else 1


async def _nack_with_retry_cap(
    *,
    nack: NackCallback,
    meta: BrokerMessageMeta,
    max_ingress_retries: int,
    plugin_name: str,
    message_id: str,
    reason: str,
) -> None:
    attempt = ingress_delivery_attempt(meta)
    requeue = attempt < max_ingress_retries
    if not requeue:
        logger.warning(
            "broker ingress retry cap plugin=%s queue=%s message_id=%s "
            "attempt=%s max=%s reason=%s",
            plugin_name,
            meta.queue,
            message_id,
            attempt,
            max_ingress_retries,
            reason,
        )
    await nack(requeue)


async def process_ingress_message(
    *,
    db: CoreDb,
    plugin_name: str,
    hooks: WorkerPluginBrokerHooks,
    raw: bytes,
    meta: BrokerMessageMeta,
    ack: AckCallback,
    nack: NackCallback,
    max_ingress_retries: int = 3,
) -> int | None:
    """Validate via plugin hooks and persist LINEAR task + broker_ingress.

    Returns created ``task_id``, or existing ``task_id`` on duplicate (after ack).
    """
    message_id = resolve_message_id(
        queue=meta.queue,
        body=raw,
        amqp_message_id=meta.message_id,
        headers=meta.headers,
    )
    enriched = BrokerMessageMeta(
        queue=meta.queue,
        delivery_tag=meta.delivery_tag,
        message_id=message_id,
        reply_to=meta.reply_to,
        content_type=meta.content_type,
        headers=dict(meta.headers),
        redelivered=meta.redelivered,
    )

    try:
        draft = await hooks.on_ingress(raw, enriched)
    except IngressReject:
        logger.warning(
            "broker ingress rejected plugin=%s queue=%s message_id=%s",
            plugin_name,
            meta.queue,
            message_id,
        )
        await nack(False)
        return None
    except IngressRetry:
        logger.info(
            "broker ingress retry plugin=%s queue=%s message_id=%s",
            plugin_name,
            meta.queue,
            message_id,
        )
        await _nack_with_retry_cap(
            nack=nack,
            meta=enriched,
            max_ingress_retries=max_ingress_retries,
            plugin_name=plugin_name,
            message_id=message_id,
            reason="IngressRetry",
        )
        return None
    except Exception:
        logger.exception(
            "broker ingress hook failed plugin=%s queue=%s message_id=%s",
            plugin_name,
            meta.queue,
            message_id,
        )
        await _nack_with_retry_cap(
            nack=nack,
            meta=enriched,
            max_ingress_retries=max_ingress_retries,
            plugin_name=plugin_name,
            message_id=message_id,
            reason="hook_error",
        )
        return None

    if not isinstance(draft, TaskDraft):
        logger.error(
            "on_ingress must return TaskDraft plugin=%s queue=%s got=%s",
            plugin_name,
            meta.queue,
            type(draft).__name__,
        )
        await nack(False)
        return None

    try:
        task_id = await insert_linear_task_with_ingress(
            db,
            plugin_name=plugin_name,
            draft=draft,
            ingress_queue=meta.queue,
            message_id=message_id,
            reply_to=meta.reply_to,
            content_type=meta.content_type,
            headers=dict(meta.headers) if meta.headers else None,
        )
    except DuplicateBrokerMessage as exc:
        logger.info(
            "broker ingress duplicate ack plugin=%s queue=%s message_id=%s task_id=%s",
            plugin_name,
            meta.queue,
            message_id,
            exc.task_id,
        )
        await ack()
        return exc.task_id
    except Exception:
        logger.exception(
            "broker ingress persist failed plugin=%s queue=%s message_id=%s",
            plugin_name,
            meta.queue,
            message_id,
        )
        await nack(True)
        return None

    await ack()
    logger.info(
        "broker ingress accepted plugin=%s queue=%s message_id=%s task_id=%s "
        "scenario=%s",
        plugin_name,
        meta.queue,
        message_id,
        task_id,
        draft.scenario,
    )
    return task_id


def meta_from_aio_pika_message(queue: str, message: Any) -> BrokerMessageMeta:
    """Build ``BrokerMessageMeta`` from an aio_pika incoming message."""
    headers_raw = getattr(message, "headers", None) or {}
    headers = {
        str(key): value for key, value in dict(headers_raw).items() if value is not None
    }
    return BrokerMessageMeta(
        queue=queue,
        delivery_tag=getattr(message, "delivery_tag", None),
        message_id=getattr(message, "message_id", None),
        reply_to=getattr(message, "reply_to", None),
        content_type=getattr(message, "content_type", None),
        headers=headers,
        redelivered=bool(getattr(message, "redelivered", False)),
    )
