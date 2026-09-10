"""Plugin broker hooks protocol (RFC-0004)."""

from __future__ import annotations

from typing import Protocol

from scenario_pipeliner.worker.broker.settings import (
    BrokerIngressView,
    BrokerMessageMeta,
    ReplyDraft,
    ResultRowView,
    TaskDraft,
    TaskRowView,
)


class WorkerPluginBrokerHooks(Protocol):
    async def on_ingress(
        self,
        raw: bytes,
        meta: BrokerMessageMeta,
    ) -> TaskDraft: ...

    async def on_egress(
        self,
        task: TaskRowView,
        result: ResultRowView | None,
        ingress: BrokerIngressView,
    ) -> ReplyDraft | None: ...
