from pydantic import Field

from scenario_pipeliner.worker.core.settings import Settings
from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    PipelineFactory,
    StateClass,
)


class ExecuteSettings(Settings):
    """RFC-0001 bridge settings with registry-first contract."""

    pipelines_mapper: dict[str, PipelineFactory] = Field(default_factory=dict)
    states_mapper: dict[str, StateClass] = Field(default_factory=dict)

    @classmethod
    def from_registry(cls, registry: MainPipelinePluginRegistry) -> "ExecuteSettings":
        return cls(
            pipelines_mapper=registry.pipeline_factories,
            states_mapper=registry.state_classes,
        )
