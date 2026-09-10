from pathlib import Path

import pytest

from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    PluginContext,
    ScenarioPluginDefinition,
)


def _pipeline_factory() -> object:
    return object()


class _State:
    pass


def test_registry_registers_definition() -> None:
    registry = MainPipelinePluginRegistry()
    registry.register(
        ScenarioPluginDefinition(
            scenario="scenario9",
            pipeline_factory=_pipeline_factory,
            state_cls=_State,
        )
    )

    assert registry.scenarios == ["scenario9"]
    assert registry.pipeline_factories["scenario9"] is _pipeline_factory
    assert registry.state_classes["scenario9"] is _State


def test_registry_rejects_empty_scenario() -> None:
    registry = MainPipelinePluginRegistry()

    with pytest.raises(ValueError, match="scenario must not be empty"):
        registry.register(
            ScenarioPluginDefinition(
                scenario=" ",
                pipeline_factory=_pipeline_factory,
                state_cls=_State,
            )
        )


def test_registry_rejects_duplicate_scenario() -> None:
    registry = MainPipelinePluginRegistry()
    definition = ScenarioPluginDefinition(
        scenario="scenario9",
        pipeline_factory=_pipeline_factory,
        state_cls=_State,
    )
    registry.register(definition)

    with pytest.raises(ValueError, match="duplicate scenario registration"):
        registry.register(definition)


class _Options:
    def __init__(self, *, marker: str) -> None:
        self.marker = marker


def test_plugin_context_resolve_options_from_mapping() -> None:
    context = PluginContext(
        plugin_name="plugin_a",
        plugin_dir=Path("/tmp/plugins/plugin_a"),
        plugins_root=Path("/tmp/plugins"),
        services={"plugin_a": {"marker": "ok"}},
    )

    options = context.resolve_options(_Options)

    assert options is not None
    assert options.marker == "ok"


def test_plugin_context_resolve_options_from_instance() -> None:
    instance = _Options(marker="ok")
    context = PluginContext(
        plugin_name="plugin_a",
        plugin_dir=Path("/tmp/plugins/plugin_a"),
        plugins_root=Path("/tmp/plugins"),
        services={"plugin_a": instance},
    )

    options = context.resolve_options(_Options)

    assert options is instance


def test_plugin_context_shared_services() -> None:
    context = PluginContext(
        plugin_name="plugin_a",
        plugin_dir=Path("/tmp/plugins/plugin_a"),
        plugins_root=Path("/tmp/plugins"),
        services={"__shared__": {"postgres_pool": "pool"}},
    )

    assert context.shared_services() == {"postgres_pool": "pool"}


def test_runtime_bootstrap_plugin_services_includes_shared_pool() -> None:
    from scenario_pipeliner.worker.plugin_registry import SHARED_PLUGIN_SERVICES_KEY
    from scenario_pipeliner.worker.runtime.app_bootstrap import RuntimeBootstrap

    services = RuntimeBootstrap.plugin_services(pool="pool")  # type: ignore[arg-type]

    assert services[SHARED_PLUGIN_SERVICES_KEY]["postgres_pool"] == "pool"
