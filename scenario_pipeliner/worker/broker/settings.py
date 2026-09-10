"""Per-plugin broker settings and ingress/egress DTOs (RFC-0004)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import Field, field_validator

from scenario_pipeliner.base_settings import FrozenSettings
from scenario_pipeliner.worker.core.settings import Settings
from scenario_pipeliner.worker.core.states import TaskPayload


class PluginBrokerSettings(Settings):
    """Queue bindings and knobs for one plugin's broker participation."""

    input_queues: list[str] = Field(default_factory=list)
    output_queues: list[str] = Field(default_factory=list)
    queue_bindings: dict[str, str] = Field(default_factory=dict)
    prefetch_count: int = Field(default=10, ge=1, le=1000)
    stale_processing_minutes: int = Field(
        default=15,
        ge=1,
        le=1440,
        description=(
            "Outbox PROCESSING lease in minutes. Claim is process-wide "
            "(not per plugin); several plugins share the shortest value."
        ),
    )
    max_ingress_retries: int = Field(default=3, ge=1, le=5)
    enabled: bool = True

    @classmethod
    def outbox_stale_processing_minutes(
        cls, settings_by_plugin: Mapping[str, PluginBrokerSettings]
    ) -> int:
        """Lease for the process-wide outbox claim.

        ``claim_pending_outbox`` does not filter by plugin, so several plugins
        share one timeout: the shortest configured value (or this field's
        default).
        """
        if not settings_by_plugin:
            return int(cls.model_fields["stale_processing_minutes"].default)
        return min(
            item.stale_processing_minutes for item in settings_by_plugin.values()
        )

    @field_validator("input_queues", "output_queues", mode="before")
    @classmethod
    def _normalize_queue_list(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            item = value.strip()
            return [item] if item else []
        if isinstance(value, (list, tuple)):
            return [str(item).strip() for item in value if str(item).strip()]
        raise TypeError("queue list must be str or list[str]")

    def resolve_scenario(self, *, queue: str, fallback_scenario: str | None) -> str:
        """Map ingress queue to scenario key."""
        bound = self.queue_bindings.get(queue)
        if bound and bound.strip():
            return bound.strip()
        if fallback_scenario and fallback_scenario.strip():
            return fallback_scenario.strip()
        raise ValueError(
            f"no scenario binding for queue {queue!r}; set queue_bindings "
            "or register exactly one scenario for the plugin"
        )


class BrokerMessageMeta(FrozenSettings):
    queue: str
    delivery_tag: int | None = None
    message_id: str | None = None
    reply_to: str | None = None
    content_type: str | None = None
    headers: dict[str, Any] = Field(default_factory=dict)
    redelivered: bool = False


class TaskDraft(FrozenSettings):
    """Draft for broker-created tasks.

    ``type_task`` / ``interval_seconds`` are intentionally absent: library always
    inserts ``LINEAR`` tasks.

    ``payload`` is the same ``TaskPayload`` the DB worker passes to pipeline
    steps. A dict is coerced; unknown keys are ignored (Pydantic extra=ignore).
    """

    scenario: str
    payload: TaskPayload
    alias: str | None = None
    max_executions: int | None = None


class ReplyDraft(FrozenSettings):
    body: bytes
    routing_queues: list[str] | None = None
    content_type: str | None = "application/json"
    headers: dict[str, Any] | None = None


class TaskRowView(FrozenSettings):
    task_id: int
    scenario: str
    status: str
    payload: TaskPayload | None
    alias: str | None = None


class ResultRowView(FrozenSettings):
    result_id: int
    task_id: int
    result: dict[str, Any] | list[Any] | str | int | float | bool | None


class BrokerIngressView(FrozenSettings):
    ingress_id: int
    task_id: int
    plugin_name: str
    scenario: str
    ingress_queue: str
    message_id: str
    reply_to: str | None = None
    content_type: str | None = None
    headers: dict[str, Any] | None = None


class BrokerPluginBinding(FrozenSettings):
    plugin_name: str
    hooks: Any
    settings: PluginBrokerSettings
    scenarios: list[str]
