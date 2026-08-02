from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

TOptions = TypeVar("TOptions")


PipelineFactory = Callable[[], Any]
StateClass = type[Any]


@dataclass(frozen=True, slots=True)
class PluginContext:
    plugin_name: str
    plugin_dir: Path
    plugins_root: Path
    services: Mapping[str, Any]

    def resolve_options(self, options_type: type[TOptions]) -> TOptions | None:
        """Resolve plugin-specific options from services by plugin_name.

        Supports either:
        - direct instance of ``options_type`` in ``services[plugin_name]``
        - mapping payload, instantiated as ``options_type(**payload)``
        """
        payload = self.services.get(self.plugin_name)
        if payload is None:
            return None
        if isinstance(payload, options_type):
            return payload
        if isinstance(payload, Mapping):
            try:
                return options_type(**dict(payload))
            except TypeError as exc:
                raise ValueError(
                    f"invalid options for plugin {self.plugin_name!r}: {exc}"
                ) from exc
        raise TypeError(
            f"plugin options for {self.plugin_name!r} must be "
            f"{options_type.__name__} or mapping"
        )


@dataclass(frozen=True, slots=True)
class ScenarioPluginDefinition:
    scenario: str
    pipeline_factory: PipelineFactory
    state_cls: StateClass


class MainPipelinePluginRegistry:
    """Copied and decoupled registry for RFC-0001 refactor track.

    This version intentionally does not depend on ``src/worker`` runtime
    types, so we can evolve contract/API in ``scenario_pipeliner`` first.
    """

    def __init__(self) -> None:
        self._pipeline_factories: dict[str, PipelineFactory] = {}
        self._state_classes: dict[str, StateClass] = {}

    def register(self, definition: ScenarioPluginDefinition) -> None:
        scenario = definition.scenario.strip()
        if not scenario:
            raise ValueError("scenario must not be empty")
        if scenario in self._pipeline_factories:
            raise ValueError(f"duplicate scenario registration: {scenario!r}")

        self._pipeline_factories[scenario] = definition.pipeline_factory
        self._state_classes[scenario] = definition.state_cls

    @property
    def pipeline_factories(self) -> dict[str, PipelineFactory]:
        return dict(self._pipeline_factories)

    @property
    def state_classes(self) -> dict[str, StateClass]:
        return dict(self._state_classes)

    @property
    def scenarios(self) -> list[str]:
        return sorted(self._pipeline_factories.keys())
