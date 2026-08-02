from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    ScenarioPluginDefinition,
)
from scenario_pipeliner.worker.settings_bridge import (
    ExecuteSettings,
    RabbitMQSettings,
)


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


def test_settings_bridge_builds_rabbitmq_settings_from_registry() -> None:
    registry = MainPipelinePluginRegistry()
    registry.register(
        ScenarioPluginDefinition(
            scenario="scenario9",
            pipeline_factory=_pipeline_factory,
            state_cls=_State,
        )
    )
    registry.register(
        ScenarioPluginDefinition(
            scenario="scenario10",
            pipeline_factory=_pipeline_factory,
            state_cls=_State,
        )
    )

    settings = RabbitMQSettings.from_registry(registry)

    assert settings.scenarios == ["scenario10", "scenario9"]
