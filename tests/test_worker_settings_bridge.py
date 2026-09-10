from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    ScenarioPluginDefinition,
)
from scenario_pipeliner.worker.runtime.settings_bridge import ExecuteSettings


def _pipeline_factory() -> object:
    return object()


class _State:
    pass


def test_settings_bridge_builds_execute_settings_from_registry() -> None:
    registry = MainPipelinePluginRegistry()
    registry.register(
        ScenarioPluginDefinition(
            scenario="scenario9",
            pipeline_factory=_pipeline_factory,
            state_cls=_State,
        )
    )

    settings = ExecuteSettings.from_registry(registry)

    assert settings.pipelines_mapper["scenario9"] is _pipeline_factory
    assert settings.states_mapper["scenario9"] is _State
    assert "plugin_registry" not in ExecuteSettings.model_fields
