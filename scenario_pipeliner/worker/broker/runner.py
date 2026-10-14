"""Broker runner: ingress consume + outbox reconcile/publish (RFC-0004)."""

from __future__ import annotations

import asyncio
import logging
from types import TracebackType
from typing import Any

from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.broker.ingress import (
    meta_from_aio_pika_message,
    process_ingress_message,
)
from scenario_pipeliner.worker.broker.outbox import (
    publish_pending_outbox,
    reconcile_terminal_broker_tasks,
)
from scenario_pipeliner.worker.broker.settings import (
    BrokerPluginBinding,
    PluginBrokerSettings,
)
from scenario_pipeliner.worker.broker.worker_plugin_broker_protocol import (
    WorkerPluginBrokerHooks,
)
from scenario_pipeliner.worker.core.custom_clients.rabbitmq import AsyncRabbitMQClient
from scenario_pipeliner.worker.core.custom_settings import RabbitMQSettings
from scenario_pipeliner.worker.core.settings import RunnerBrokerSettings

logger = logging.getLogger(__name__)


class BrokerRunner:
    """Ingress + egress loops for ``run --mode broker``."""

    def __init__(
        self,
        *,
        db: CoreDb,
        bindings: list[BrokerPluginBinding],
        rabbit_settings: RabbitMQSettings | None = None,
        runner_settings: RunnerBrokerSettings | None = None,
    ) -> None:
        self.db = db
        self.bindings = [item for item in bindings if item.settings.enabled]
        self.rabbit_settings = rabbit_settings or RabbitMQSettings()
        self.runner_settings = runner_settings or RunnerBrokerSettings()
        self._stop = asyncio.Event()
        self._client: AsyncRabbitMQClient | None = None
        self._consuming = False

    def stop(self) -> None:
        self._stop.set()

    def _hooks_map(self) -> dict[str, WorkerPluginBrokerHooks]:
        return {item.plugin_name: item.hooks for item in self.bindings}

    def _settings_map(self) -> dict[str, PluginBrokerSettings]:
        return {item.plugin_name: item.settings for item in self.bindings}

    def _queue_owners(self) -> dict[str, BrokerPluginBinding]:
        owners: dict[str, BrokerPluginBinding] = {}
        for binding in self.bindings:
            for queue in binding.settings.input_queues:
                if queue in owners and owners[queue].plugin_name != binding.plugin_name:
                    raise ValueError(
                        f"queue {queue!r} claimed by multiple plugins: "
                        f"{owners[queue].plugin_name!r} and {binding.plugin_name!r}"
                    )
                owners[queue] = binding
        return owners

    @staticmethod
    def _hooks_for_queue(
        queue_name: str, binding: BrokerPluginBinding
    ) -> WorkerPluginBrokerHooks:
        if binding.settings.queue_bindings or len(binding.scenarios) == 1:
            return _ScenarioEnsuringHooks(
                inner=binding.hooks,
                settings=binding.settings,
                scenarios=binding.scenarios,
                queue=queue_name,
            )
        return binding.hooks

    def _make_ingress_callback(
        self, queue_name: str, binding: BrokerPluginBinding
    ) -> Any:
        async def _on_message(message: Any) -> None:
            if self._stop.is_set():
                await message.nack(requeue=True)
                return
            raw = bytes(message.body)
            meta = meta_from_aio_pika_message(queue_name, message)

            async def _ack() -> None:
                await message.ack()

            async def _nack(requeue: bool) -> None:
                await message.nack(requeue=requeue)

            try:
                await asyncio.wait_for(
                    process_ingress_message(
                        db=self.db,
                        plugin_name=binding.plugin_name,
                        hooks=self._hooks_for_queue(queue_name, binding),
                        raw=raw,
                        meta=meta,
                        ack=_ack,
                        nack=_nack,
                        max_ingress_retries=binding.settings.max_ingress_retries,
                    ),
                    timeout=self.runner_settings.INGRESS_HANDLER_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                logger.error(
                    "broker ingress timed out plugin=%s queue=%s",
                    binding.plugin_name,
                    queue_name,
                )
                try:
                    await message.nack(requeue=True)
                except Exception:
                    logger.exception(
                        "broker ingress timeout nack failed plugin=%s queue=%s",
                        binding.plugin_name,
                        queue_name,
                    )

        return _on_message

    async def __aenter__(self) -> "BrokerRunner":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()
        if self._client is not None:
            await self._client.disconnect()
            self._client = None

    async def execute(self) -> None:
        if not self.bindings:
            logger.warning("broker runner: no enabled plugin broker bindings; idle")
            while not self._stop.is_set():
                await asyncio.sleep(self.runner_settings.POLL_INTERVAL_SECONDS)
            return

        prefetch = max(item.settings.prefetch_count for item in self.bindings)
        client = AsyncRabbitMQClient(settings=self.rabbit_settings)
        failures = 0
        limit = self.runner_settings.PING_FAILURE_LIMIT

        try:
            await self._start_client(client, prefetch)
            while not self._stop.is_set():
                try:
                    await self._ensure_broker_alive(client, prefetch)
                except Exception:
                    failures += 1
                    logger.exception(
                        "broker restore failed (%s/%s)",
                        failures,
                        limit,
                    )
                    if failures >= limit:
                        raise
                    await asyncio.sleep(self.runner_settings.POLL_INTERVAL_SECONDS)
                    continue
                failures = 0
                if self._stop.is_set():
                    break
                await reconcile_terminal_broker_tasks(
                    self.db, limit=self.runner_settings.RECONCILE_LIMIT
                )
                await publish_pending_outbox(
                    self.db,
                    hooks_by_plugin=self._hooks_map(),
                    settings_by_plugin=self._settings_map(),
                    publish=self._make_publish(client),
                    limit=self.runner_settings.OUTBOX_LIMIT,
                )
                await asyncio.sleep(self.runner_settings.POLL_INTERVAL_SECONDS)
        finally:
            await self._abandon(client)
            self._client = None

    def _restore_timeout(self) -> int:
        queues = sum(len(item.settings.input_queues) for item in self.bindings)
        return self.rabbit_settings.RABBITMQ_CONNECT_TIMEOUT_SECONDS + (
            self.rabbit_settings.RABBITMQ_PING_TIMEOUT_SECONDS * max(queues, 1)
        )

    async def _start_client(self, client: AsyncRabbitMQClient, prefetch: int) -> None:
        self._consuming = False
        await client.connect_with_qos(prefetch_count=prefetch)
        self._client = client
        owners = self._queue_owners()
        for queue_name, binding in owners.items():
            await client.consume_unacked(
                queue_name, self._make_ingress_callback(queue_name, binding)
            )
        self._consuming = True
        logger.info(
            "broker runner started plugins=%s queues=%s prefetch=%s",
            ",".join(sorted({item.plugin_name for item in self.bindings})),
            ",".join(sorted(owners)),
            prefetch,
        )

    async def _abandon(self, client: AsyncRabbitMQClient) -> None:
        self._consuming = False
        try:
            await asyncio.wait_for(
                client.disconnect(),
                timeout=self.rabbit_settings.RABBITMQ_PING_TIMEOUT_SECONDS,
            )
        except Exception:
            logger.warning(
                "broker disconnect did not finish; dropping the client",
                exc_info=True,
            )
            client.drop()

    async def _reconnect(self, client: AsyncRabbitMQClient, prefetch: int) -> None:
        await self._abandon(client)
        await asyncio.wait_for(
            self._start_client(client, prefetch),
            timeout=self._restore_timeout(),
        )

    async def _ensure_broker_alive(
        self, client: AsyncRabbitMQClient, prefetch: int
    ) -> None:
        try:
            if self._consuming:
                await client.ping()
                return
        except Exception:
            logger.warning("broker ping failed; reconnecting", exc_info=True)
        await self._reconnect(client, prefetch)
        await client.ping()

    @staticmethod
    def _make_publish(client: AsyncRabbitMQClient):
        async def _publish(
            queue_name: str,
            body: bytes,
            headers: dict[str, Any] | None,
        ) -> None:
            content_type = None
            clean_headers: dict[str, Any] = {}
            if headers:
                content_type = headers.get("content_type")
                clean_headers = {
                    key: value
                    for key, value in headers.items()
                    if key != "content_type"
                }
            await client.publish_bytes(
                queue_name,
                body,
                headers=clean_headers,
                content_type=str(content_type) if content_type else None,
            )

        return _publish


class _ScenarioEnsuringHooks:
    """If draft.scenario empty/missing binding, fill from plugin settings."""

    def __init__(
        self,
        *,
        inner: WorkerPluginBrokerHooks,
        settings: PluginBrokerSettings,
        scenarios: list[str],
        queue: str,
    ) -> None:
        self._inner = inner
        self._settings = settings
        self._scenarios = scenarios
        self._queue = queue

    async def on_ingress(self, raw: bytes, meta):  # type: ignore[no-untyped-def]
        draft = await self._inner.on_ingress(raw, meta)
        if draft.scenario and draft.scenario.strip():
            return draft
        fallback = self._scenarios[0] if len(self._scenarios) == 1 else None
        scenario = self._settings.resolve_scenario(
            queue=self._queue, fallback_scenario=fallback
        )
        return type(draft)(
            scenario=scenario,
            payload=draft.payload,
            alias=draft.alias,
            max_executions=draft.max_executions,
        )

    async def on_egress(self, task, result, ingress):  # type: ignore[no-untyped-def]
        return await self._inner.on_egress(task, result, ingress)
