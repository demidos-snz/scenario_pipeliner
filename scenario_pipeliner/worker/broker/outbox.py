"""Outbox reconciler + publisher (RFC-0004)."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.broker.settings import PluginBrokerSettings, ReplyDraft
from scenario_pipeliner.worker.broker.storage import (
    claim_pending_outbox,
    insert_outbox_for_terminal_tasks,
    load_task_result_ingress,
    mark_outbox_publish_failure,
    mark_outbox_sent,
    mark_outbox_skipped,
    store_outbox_reply,
)
from scenario_pipeliner.worker.broker.worker_plugin_broker_protocol import (
    WorkerPluginBrokerHooks,
)

logger = logging.getLogger(__name__)

PublishCallback = Callable[[str, bytes, dict | None], Awaitable[None]]


async def reconcile_terminal_broker_tasks(
    db: CoreDb,
    *,
    limit: int = 50,
) -> int:
    """Create PENDING outbox rows for terminal broker-originated tasks."""
    created = await insert_outbox_for_terminal_tasks(db, limit=limit)
    if created:
        logger.info("broker reconciler created outbox rows=%s", created)
    return created


def _resolve_routing_queues(
    *,
    draft: ReplyDraft,
    ingress_reply_to: str | None,
    plugin_output_queues: list[str],
) -> list[str]:
    if draft.routing_queues:
        return [queue for queue in draft.routing_queues if queue.strip()]
    if ingress_reply_to and ingress_reply_to.strip():
        return [ingress_reply_to.strip()]
    return [queue for queue in plugin_output_queues if queue.strip()]


def _normalize_headers(raw: Any) -> dict[str, Any]:
    """Coerce outbox headers payload to dict."""
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


async def _publish_all_queues(
    publish: PublishCallback,
    *,
    routing_queues: list[str],
    body: bytes,
    headers: dict[str, Any],
) -> None:
    """Publish to every queue; SENT is only valid after this returns.

    Fail-fast on the first error so later queues are not extra-duplicated on
    retry. Queues that already succeeded in this pass may still see a
    duplicate after ``PENDING`` reclaim — consumers must key on ``task_id``.
    """
    for queue_name in routing_queues:
        await publish(queue_name, body, headers)


async def publish_pending_outbox(
    db: CoreDb,
    *,
    hooks_by_plugin: Mapping[str, WorkerPluginBrokerHooks],
    settings_by_plugin: Mapping[str, PluginBrokerSettings],
    publish: PublishCallback,
    limit: int = 20,
    max_attempts: int = 10,
) -> int:
    """Fill body via on_egress when needed and publish claimed PROCESSING rows."""
    rows = await claim_pending_outbox(
        db,
        limit=limit,
        stale_processing_minutes=(
            PluginBrokerSettings.outbox_stale_processing_minutes(settings_by_plugin)
        ),
    )
    sent = 0
    for row in rows:
        outbox_id = int(row["outbox_id"])
        task_id = int(row["task_id"])
        ingress_id = int(row["ingress_id"])
        try:
            task, result, ingress = await load_task_result_ingress(
                db, task_id=task_id, ingress_id=ingress_id
            )
            hooks = hooks_by_plugin.get(ingress.plugin_name)
            settings = settings_by_plugin.get(ingress.plugin_name)
            if hooks is None or settings is None:
                await mark_outbox_skipped(db, outbox_id=outbox_id)
                logger.warning(
                    "outbox skip: no hooks/settings for plugin=%s task_id=%s",
                    ingress.plugin_name,
                    task_id,
                )
                continue

            body = row.get("body")
            routing_queues = list(row.get("routing_queues") or [])
            content_type = row.get("content_type")
            headers = _normalize_headers(row.get("headers"))

            if body is None:
                draft = await hooks.on_egress(task, result, ingress)
                if draft is None:
                    await mark_outbox_skipped(db, outbox_id=outbox_id)
                    continue
                routing_queues = _resolve_routing_queues(
                    draft=draft,
                    ingress_reply_to=ingress.reply_to,
                    plugin_output_queues=list(settings.output_queues),
                )
                if not routing_queues:
                    await mark_outbox_skipped(db, outbox_id=outbox_id)
                    logger.warning(
                        "outbox skip: empty routing task_id=%s plugin=%s",
                        task_id,
                        ingress.plugin_name,
                    )
                    continue
                await store_outbox_reply(
                    db,
                    outbox_id=outbox_id,
                    draft=draft,
                    routing_queues=routing_queues,
                )
                body = draft.body
                content_type = draft.content_type
                headers = _normalize_headers(draft.headers)
                if content_type:
                    headers.setdefault("content_type", content_type)

            assert body is not None
            publish_headers = _normalize_headers(headers)
            publish_headers.setdefault("task_id", str(task_id))
            if isinstance(content_type, str) and content_type.strip():
                publish_headers.setdefault("content_type", content_type.strip())

            await _publish_all_queues(
                publish,
                routing_queues=routing_queues,
                body=bytes(body),
                headers=publish_headers,
            )
            await mark_outbox_sent(db, outbox_id=outbox_id)
            sent += 1
        except Exception as exc:
            logger.exception(
                "outbox publish failed outbox_id=%s task_id=%s", outbox_id, task_id
            )
            await mark_outbox_publish_failure(
                db,
                outbox_id=outbox_id,
                error=str(exc),
                max_attempts=max_attempts,
            )
    return sent
