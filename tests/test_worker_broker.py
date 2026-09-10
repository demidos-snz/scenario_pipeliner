"""Tests for RFC-0004 broker helpers."""

from __future__ import annotations

import asyncio
import uuid
from typing import cast

import pytest
from sqlalchemy import func, select, update

from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.broker.enums import BrokerOutboxStatus
from scenario_pipeliner.worker.broker.exceptions import IngressReject, IngressRetry
from scenario_pipeliner.worker.broker.ingress import (
    ingress_delivery_attempt,
    process_ingress_message,
)
from scenario_pipeliner.worker.broker.outbox import _normalize_headers
from scenario_pipeliner.worker.broker.runner import BrokerRunner
from scenario_pipeliner.worker.broker.settings import (
    BrokerMessageMeta,
    BrokerPluginBinding,
    PluginBrokerSettings,
    TaskDraft,
)
from scenario_pipeliner.worker.broker.storage import (
    claim_pending_outbox,
    ensure_outbox_pending,
    insert_linear_task_with_ingress,
    insert_outbox_for_terminal_tasks,
)
from scenario_pipeliner.worker.broker.utils import resolve_message_id
from scenario_pipeliner.worker.core.enums import EnumDoc, TaskStatus
from scenario_pipeliner.worker.core.settings import RunnerBrokerSettings
from scenario_pipeliner.worker.core.states import TaskPayload
from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    ScenarioPluginDefinition,
)
from scenario_pipeliner.worker.runtime.app import build_broker_bindings


def test_task_draft_payload_is_task_payload() -> None:
    from_model = TaskDraft(
        scenario="demo.ping",
        payload=TaskPayload(type_doc=EnumDoc.JSON, data=["doc-1"]),
    )
    from_dict = TaskDraft.model_validate(
        {
            "scenario": "demo.ping",
            "payload": {
                "type_doc": "JSON",
                "data": ["doc-1"],
                "order_id": "dropped",
            },
        }
    )
    assert from_model.payload.type_doc is EnumDoc.JSON
    assert from_dict.payload == from_model.payload
    dumped = from_dict.payload.model_dump(mode="json")
    assert dumped["data"] == ["doc-1"]
    assert "order_id" not in dumped


def test_resolve_message_id_prefers_amqp_id() -> None:
    assert (
        resolve_message_id(
            queue="q1",
            body=b"abc",
            amqp_message_id=" producer-1 ",
        )
        == "producer-1"
    )


def test_resolve_message_id_fallback_is_stable() -> None:
    first = resolve_message_id(queue="q1", body=b"abc", amqp_message_id=None)
    second = resolve_message_id(queue="q1", body=b"abc", amqp_message_id="")
    third = resolve_message_id(queue="q1", body=b"abd", amqp_message_id=None)
    assert first == second
    assert first != third
    assert len(first) == 64


def test_resolve_message_id_uses_trace_id_when_amqp_missing() -> None:
    assert (
        resolve_message_id(
            queue="q1",
            body=b"abc",
            amqp_message_id=None,
            headers={"trace_id": " trace-123 "},
        )
        == "trace-123"
    )


def test_normalize_headers_accepts_json_string() -> None:
    assert _normalize_headers('{"trace_id":"t1","status":"created"}') == {
        "trace_id": "t1",
        "status": "created",
    }


def test_normalize_headers_invalid_string_returns_empty_dict() -> None:
    assert _normalize_headers("not-json") == {}


def test_runner_broker_settings_defaults_and_runner() -> None:
    defaults = RunnerBrokerSettings()
    assert defaults.RECONCILE_LIMIT == 50
    assert defaults.OUTBOX_LIMIT == 20
    assert defaults.POLL_INTERVAL_SECONDS == 60
    with pytest.raises(ValueError):
        RunnerBrokerSettings(RECONCILE_LIMIT=0)
    custom = RunnerBrokerSettings(
        POLL_INTERVAL_SECONDS=3,
        RECONCILE_LIMIT=7,
        OUTBOX_LIMIT=4,
    )
    runner = BrokerRunner(
        db=cast(CoreDb, object()),
        bindings=[],
        runner_settings=custom,
    )
    assert runner.runner_settings is custom


def test_plugin_broker_settings_outbox_stale_uses_minimum() -> None:
    assert PluginBrokerSettings().stale_processing_minutes == 15
    assert PluginBrokerSettings.outbox_stale_processing_minutes({}) == 15
    assert (
        PluginBrokerSettings.outbox_stale_processing_minutes(
            {
                "a": PluginBrokerSettings(stale_processing_minutes=20),
                "b": PluginBrokerSettings(stale_processing_minutes=5),
            }
        )
        == 5
    )
    with pytest.raises(ValueError):
        PluginBrokerSettings(stale_processing_minutes=0)


def test_plugin_broker_settings_resolve_scenario() -> None:
    settings = PluginBrokerSettings(
        input_queues=["in.a"],
        queue_bindings={"in.a": "hello_scenario.ping"},
    )
    assert (
        settings.resolve_scenario(queue="in.a", fallback_scenario=None)
        == "hello_scenario.ping"
    )
    assert (
        PluginBrokerSettings(input_queues=["in.a"]).resolve_scenario(
            queue="in.a", fallback_scenario="plugin.scenario"
        )
        == "plugin.scenario"
    )
    with pytest.raises(ValueError, match="no scenario binding"):
        PluginBrokerSettings(input_queues=["in.a"]).resolve_scenario(
            queue="in.a", fallback_scenario=None
        )


def test_registry_add_broker_hooks() -> None:
    registry = MainPipelinePluginRegistry()

    class _Hooks:
        async def on_ingress(self, raw, meta):  # noqa: ANN001
            return TaskDraft(scenario="s", payload=TaskPayload())

        async def on_egress(self, task, result, ingress):  # noqa: ANN001
            return None

    registry.add_broker_hooks(
        plugin_name="demo",
        hooks=_Hooks(),
        settings_provider=lambda: PluginBrokerSettings(input_queues=["q"]),
    )
    assert "demo" in registry.broker_plugins
    with pytest.raises(ValueError, match="duplicate broker hooks"):
        registry.add_broker_hooks(
            plugin_name="demo",
            hooks=_Hooks(),
            settings_provider=lambda: PluginBrokerSettings(input_queues=["q"]),
        )


def test_process_ingress_reject_nacks_without_requeue() -> None:
    class _Hooks:
        async def on_ingress(self, raw, meta):  # noqa: ANN001
            raise IngressReject("bad")

        async def on_egress(self, task, result, ingress):  # noqa: ANN001
            return None

    acked: list[bool] = []
    nacked: list[bool] = []

    async def ack() -> None:
        acked.append(True)

    async def nack(requeue: bool) -> None:
        nacked.append(requeue)

    async def _run() -> None:
        task_id = await process_ingress_message(
            db=cast(CoreDb, object()),  # unused on reject
            plugin_name="demo",
            hooks=_Hooks(),
            raw=b"{}",
            meta=BrokerMessageMeta(queue="q1", message_id="m1"),
            ack=ack,
            nack=nack,
        )
        assert task_id is None

    asyncio.run(_run())
    assert acked == []
    assert nacked == [False]


async def _seed_pending_outbox(core_db: CoreDb, *, message_id: str) -> int:
    task_id = await insert_linear_task_with_ingress(
        core_db,
        plugin_name="demo",
        draft=TaskDraft(
            scenario="demo.ping",
            payload=TaskPayload(type_doc=EnumDoc.JSON, data=["x"]),
        ),
        ingress_queue="q-outbox",
        message_id=message_id,
        reply_to=None,
        content_type=None,
        headers=None,
    )
    async with core_db.engine.connect() as conn:
        ingress_id = (
            await conn.execute(
                select(core_db.core.broker_ingress.c.id).where(
                    core_db.core.broker_ingress.c.task_id == task_id
                )
            )
        ).scalar_one()
    inserted = await ensure_outbox_pending(
        core_db, task_id=task_id, ingress_id=int(ingress_id)
    )
    assert inserted is True
    async with core_db.engine.connect() as conn:
        outbox_id = (
            await conn.execute(
                select(core_db.core.broker_outbox.c.id).where(
                    core_db.core.broker_outbox.c.task_id == task_id
                )
            )
        ).scalar_one()
    return int(outbox_id)


def test_claim_pending_outbox_is_exclusive(core_db: CoreDb) -> None:
    async def _run() -> None:
        outbox_id = await _seed_pending_outbox(
            core_db, message_id=f"claim-{uuid.uuid4().hex}"
        )
        first = await claim_pending_outbox(core_db, limit=10)
        assert len(first) == 1
        assert first[0]["outbox_id"] == outbox_id
        assert first[0]["status"] == BrokerOutboxStatus.PROCESSING.value
        second = await claim_pending_outbox(core_db, limit=10)
        assert second == []

    asyncio.run(_run())


def test_claim_pending_outbox_skips_locked_row(core_db: CoreDb) -> None:
    async def _run() -> None:
        await _seed_pending_outbox(core_db, message_id=f"race-{uuid.uuid4().hex}")
        left, right = await asyncio.gather(
            claim_pending_outbox(core_db, limit=10),
            claim_pending_outbox(core_db, limit=10),
        )
        claimed = left + right
        assert len(claimed) == 1
        assert claimed[0]["status"] == BrokerOutboxStatus.PROCESSING.value

    asyncio.run(_run())


def test_claim_pending_outbox_reclaims_stale_processing(core_db: CoreDb) -> None:
    async def _run() -> None:
        outbox_id = await _seed_pending_outbox(
            core_db, message_id=f"stale-{uuid.uuid4().hex}"
        )
        first = await claim_pending_outbox(core_db, limit=10)
        assert [row["outbox_id"] for row in first] == [outbox_id]
        outbox = core_db.core.broker_outbox
        async with core_db.engine.begin() as conn:
            await conn.execute(
                update(outbox)
                .where(outbox.c.id == outbox_id)
                .values(
                    updated_at=func.now() - func.make_interval(0, 0, 0, 0, 0, 2),
                )
            )
        still_fresh = await claim_pending_outbox(
            core_db, limit=10, stale_processing_minutes=10
        )
        assert still_fresh == []
        reclaimed = await claim_pending_outbox(
            core_db, limit=10, stale_processing_minutes=1
        )
        assert len(reclaimed) == 1
        assert reclaimed[0]["outbox_id"] == outbox_id
        assert reclaimed[0]["status"] == BrokerOutboxStatus.PROCESSING.value

    asyncio.run(_run())


async def _seed_terminal_broker_task(core_db: CoreDb, *, message_id: str) -> int:
    task_id = await insert_linear_task_with_ingress(
        core_db,
        plugin_name="demo",
        draft=TaskDraft(
            scenario="demo.ping",
            payload=TaskPayload(type_doc=EnumDoc.JSON, data=["x"]),
        ),
        ingress_queue="q-outbox",
        message_id=message_id,
        reply_to=None,
        content_type=None,
        headers=None,
    )
    async with core_db.engine.begin() as conn:
        await conn.execute(
            update(core_db.core.tasks)
            .where(core_db.core.tasks.c.id == task_id)
            .values(status=TaskStatus.FINISHED.value)
        )
    return task_id


def test_insert_outbox_for_terminal_tasks_is_exclusive(core_db: CoreDb) -> None:
    async def _run() -> None:
        await _seed_terminal_broker_task(
            core_db, message_id=f"recon-{uuid.uuid4().hex}"
        )
        first = await insert_outbox_for_terminal_tasks(core_db, limit=10)
        second = await insert_outbox_for_terminal_tasks(core_db, limit=10)
        assert first == 1
        assert second == 0

    asyncio.run(_run())


def test_insert_outbox_for_terminal_tasks_skips_locked_row(core_db: CoreDb) -> None:
    async def _run() -> None:
        await _seed_terminal_broker_task(
            core_db, message_id=f"recon-race-{uuid.uuid4().hex}"
        )
        left, right = await asyncio.gather(
            insert_outbox_for_terminal_tasks(core_db, limit=10),
            insert_outbox_for_terminal_tasks(core_db, limit=10),
        )
        assert left + right == 1

    asyncio.run(_run())


def test_ingress_delivery_attempt_from_headers() -> None:
    fresh = BrokerMessageMeta(queue="q", redelivered=False)
    assert ingress_delivery_attempt(fresh) == 1
    assert ingress_delivery_attempt(BrokerMessageMeta(queue="q", redelivered=True)) == 2
    assert (
        ingress_delivery_attempt(
            BrokerMessageMeta(queue="q", headers={"x-delivery-count": 4})
        )
        == 5
    )
    assert (
        ingress_delivery_attempt(
            BrokerMessageMeta(queue="q", headers={"x-death": [{"count": 3}]})
        )
        == 3
    )


def test_process_ingress_retry_respects_cap() -> None:
    class _Hooks:
        async def on_ingress(self, raw, meta):  # noqa: ANN001
            raise IngressRetry("later")

        async def on_egress(self, task, result, ingress):  # noqa: ANN001
            return None

    nacked: list[bool] = []

    async def ack() -> None:
        return None

    async def nack(requeue: bool) -> None:
        nacked.append(requeue)

    async def _run() -> None:
        await process_ingress_message(
            db=cast(CoreDb, object()),
            plugin_name="demo",
            hooks=_Hooks(),
            raw=b"{}",
            meta=BrokerMessageMeta(queue="q1", message_id="m1", redelivered=True),
            ack=ack,
            nack=nack,
            max_ingress_retries=2,
        )
        await process_ingress_message(
            db=cast(CoreDb, object()),
            plugin_name="demo",
            hooks=_Hooks(),
            raw=b"{}",
            meta=BrokerMessageMeta(queue="q1", message_id="m2", redelivered=True),
            ack=ack,
            nack=nack,
            max_ingress_retries=5,
        )

    asyncio.run(_run())
    assert nacked == [False, True]


def test_build_broker_bindings_requires_owned_scenarios() -> None:
    registry = MainPipelinePluginRegistry()
    registry.register(
        ScenarioPluginDefinition(
            scenario="other.ping",
            pipeline_factory=lambda: None,
            state_cls=object,
        )
    )

    class _Hooks:
        async def on_ingress(self, raw, meta):  # noqa: ANN001
            return TaskDraft(scenario="s", payload=TaskPayload())

        async def on_egress(self, task, result, ingress):  # noqa: ANN001
            return None

    registry.add_broker_hooks(
        plugin_name="demo",
        hooks=_Hooks(),
        settings_provider=lambda: PluginBrokerSettings(input_queues=["q"]),
    )
    with pytest.raises(ValueError, match="no owned scenarios"):
        build_broker_bindings(registry)


def test_build_broker_bindings_maps_owned_scenarios() -> None:
    registry = MainPipelinePluginRegistry()
    registry.register(
        ScenarioPluginDefinition(
            scenario="demo.ping",
            pipeline_factory=lambda: None,
            state_cls=object,
        )
    )

    class _Hooks:
        async def on_ingress(self, raw, meta):  # noqa: ANN001
            return TaskDraft(scenario="s", payload=TaskPayload())

        async def on_egress(self, task, result, ingress):  # noqa: ANN001
            return None

    hooks = _Hooks()
    registry.add_broker_hooks(
        plugin_name="demo",
        hooks=hooks,
        settings_provider=lambda: PluginBrokerSettings(input_queues=["q"]),
    )
    bindings = build_broker_bindings(registry)
    assert len(bindings) == 1
    assert bindings[0].scenarios == ["demo.ping"]
    assert bindings[0].hooks is hooks


def test_broker_runner_consume_callback_nacks_reject() -> None:
    class _Hooks:
        async def on_ingress(self, raw, meta):  # noqa: ANN001
            raise IngressReject("bad")

        async def on_egress(self, task, result, ingress):  # noqa: ANN001
            return None

    binding = BrokerPluginBinding(
        plugin_name="demo",
        hooks=_Hooks(),
        settings=PluginBrokerSettings(input_queues=["q1"]),
        scenarios=["demo.ping"],
    )
    runner = BrokerRunner(db=cast(CoreDb, object()), bindings=[binding])
    callback = runner._make_ingress_callback("q1", binding)

    class _Message:
        body = b"{}"
        headers: dict[str, object] = {}
        delivery_tag = 1
        message_id = "m1"
        reply_to = None
        content_type = None
        redelivered = False

        def __init__(self) -> None:
            self.nacked: list[bool] = []

        async def ack(self) -> None:
            return None

        async def nack(self, requeue: bool = True) -> None:
            self.nacked.append(requeue)

    message = _Message()
    asyncio.run(callback(message))
    assert message.nacked == [False]


def test_publish_pending_outbox_calls_publish_per_queue(core_db: CoreDb) -> None:
    from scenario_pipeliner.worker.broker.outbox import publish_pending_outbox
    from scenario_pipeliner.worker.broker.settings import ReplyDraft

    class _Hooks:
        async def on_ingress(self, raw, meta):  # noqa: ANN001
            raise AssertionError("ingress unused")

        async def on_egress(self, task, result, ingress):  # noqa: ANN001
            return ReplyDraft(body=b"reply", routing_queues=["q-a", "q-b"])

    published: list[tuple[str, bytes, dict]] = []

    async def _publish(queue: str, body: bytes, headers: dict | None) -> None:
        published.append((queue, body, dict(headers or {})))

    async def _run() -> str:
        await _seed_pending_outbox(core_db, message_id=f"pub-{uuid.uuid4().hex}")
        sent = await publish_pending_outbox(
            core_db,
            hooks_by_plugin={"demo": _Hooks()},
            settings_by_plugin={
                "demo": PluginBrokerSettings(input_queues=["q-outbox"])
            },
            publish=_publish,
        )
        assert sent == 1
        async with core_db.engine.connect() as conn:
            status = (
                await conn.execute(select(core_db.core.broker_outbox.c.status))
            ).scalar_one()
        return str(status)

    status = asyncio.run(_run())
    assert status == BrokerOutboxStatus.SENT.value
    assert [item[0] for item in published] == ["q-a", "q-b"]
    assert published[0][1] == b"reply"
    assert "task_id" in published[0][2]


def test_publish_pending_outbox_partial_failure_retries_all_queues(
    core_db: CoreDb,
) -> None:
    from scenario_pipeliner.worker.broker.outbox import publish_pending_outbox
    from scenario_pipeliner.worker.broker.settings import ReplyDraft

    class _Hooks:
        async def on_ingress(self, raw, meta):  # noqa: ANN001
            raise AssertionError("ingress unused")

        async def on_egress(self, task, result, ingress):  # noqa: ANN001
            return ReplyDraft(body=b"reply", routing_queues=["q-a", "q-b"])

    published: list[str] = []
    fail_once = {"q-b": True}

    async def _publish(queue: str, body: bytes, headers: dict | None) -> None:
        published.append(queue)
        if queue == "q-b" and fail_once["q-b"]:
            fail_once["q-b"] = False
            raise RuntimeError("queue down")

    async def _run() -> tuple[str, int]:
        await _seed_pending_outbox(core_db, message_id=f"part-{uuid.uuid4().hex}")
        first = await publish_pending_outbox(
            core_db,
            hooks_by_plugin={"demo": _Hooks()},
            settings_by_plugin={
                "demo": PluginBrokerSettings(input_queues=["q-outbox"])
            },
            publish=_publish,
        )
        async with core_db.engine.connect() as conn:
            status = (
                await conn.execute(select(core_db.core.broker_outbox.c.status))
            ).scalar_one()
        second = await publish_pending_outbox(
            core_db,
            hooks_by_plugin={"demo": _Hooks()},
            settings_by_plugin={
                "demo": PluginBrokerSettings(input_queues=["q-outbox"])
            },
            publish=_publish,
        )
        return str(status), first + second

    status, sent = asyncio.run(_run())
    assert status == BrokerOutboxStatus.PENDING.value
    assert sent == 1
    assert published == ["q-a", "q-b", "q-a", "q-b"]
