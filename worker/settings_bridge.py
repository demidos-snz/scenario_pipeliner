from pydantic import BaseModel, ConfigDict, Field

from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    PipelineFactory,
    StateClass,
)


class ExecuteSettings(BaseModel):
    """RFC-0001 bridge settings with registry-first contract."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    plugin_registry: MainPipelinePluginRegistry = Field(
        default_factory=MainPipelinePluginRegistry
    )
    pipelines_mapper: dict[str, PipelineFactory] = Field(default_factory=dict)
    states_mapper: dict[str, StateClass] = Field(default_factory=dict)

    @classmethod
    def from_registry(cls, registry: MainPipelinePluginRegistry) -> "ExecuteSettings":
        return cls(
            plugin_registry=registry,
            pipelines_mapper=registry.pipeline_factories,
            states_mapper=registry.state_classes,
        )


class RabbitMQSettings(BaseModel):
    """Queue toggles derived from registry scenarios."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    plugin_registry: MainPipelinePluginRegistry = Field(
        default_factory=MainPipelinePluginRegistry
    )
    scenarios: list[str] = Field(default_factory=list)

    @classmethod
    def from_registry(cls, registry: MainPipelinePluginRegistry) -> "RabbitMQSettings":
        return cls(plugin_registry=registry, scenarios=registry.scenarios)
