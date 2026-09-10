from __future__ import annotations

from hello_scenario.pipeline import build_pipeline
from hello_scenario.states import HelloTaskState
from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    PluginContext,
    ScenarioPluginDefinition,
)

HELLO_SCENARIO = "hello_scenario.ping"


def register(
    registry: MainPipelinePluginRegistry,
    context: PluginContext | None = None,
) -> None:
    """Register the minimal hello scenario.

    ``context`` is unused in this skeleton; real plugins may resolve DI via
    ``context.resolve_options(...)`` or ``context.services``.
    """
    _ = context
    registry.register(
        ScenarioPluginDefinition(
            scenario=HELLO_SCENARIO,
            pipeline_factory=build_pipeline,
            state_cls=HelloTaskState,
        )
    )
