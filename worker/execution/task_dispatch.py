from dataclasses import MISSING, Field, fields

from scenario_pipeliner.worker.core.states import TaskState
from scenario_pipeliner.worker.plugin_registry import PipelineFactory

DEFAULT_PIPELINE_KEY = "__default__"


class TaskExecutionRouter:
    """Resolve scenario pipeline and promote generic task states."""

    def __init__(
        self,
        *,
        pipeline_factories: dict[str, PipelineFactory],
        state_classes: dict[str, type[TaskState]],
        default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    ) -> None:
        self._pipeline_factories = pipeline_factories
        self._state_classes = state_classes
        self._default_pipeline_key = default_pipeline_key
        self._task_state_field_names = {
            field.name for field in fields(TaskState) if field.init
        }
        self._validate_state_class_compatibility()

    def _validate_state_class_compatibility(self) -> None:
        for scenario, state_cls in self._state_classes.items():
            required_extra_fields = self._required_extra_fields(state_cls)
            if required_extra_fields:
                field_names = ", ".join(sorted(required_extra_fields))
                raise ValueError(
                    "State class for scenario "
                    f"{scenario!r} cannot be promoted from TaskState: "
                    "required fields without defaults: "
                    f"{field_names}"
                )

    def _required_extra_fields(self, state_cls: type[TaskState]) -> list[str]:
        required_extra_fields: list[str] = []
        for field in fields(state_cls):
            if not field.init:
                continue
            if field.name in self._task_state_field_names:
                continue
            if self._is_required_field(field):
                required_extra_fields.append(field.name)
        return required_extra_fields

    @staticmethod
    def _is_required_field(field: Field[object]) -> bool:
        return field.default is MISSING and field.default_factory is MISSING

    def resolve_pipeline_factory(
        self,
        scenario: str,
    ) -> tuple[PipelineFactory | None, bool]:
        if factory := self._pipeline_factories.get(scenario):
            return factory, False
        if factory := self._pipeline_factories.get(self._default_pipeline_key):
            return factory, True
        return None, False

    def promote_task_state(self, state: TaskState) -> TaskState:
        """Promote base TaskState to scenario-specific TaskState subclass."""
        state_cls = self._state_classes.get(state.scenario)
        if state_cls is None:
            if state.scenario in self._pipeline_factories:
                raise LookupError(
                    f"No state class mapped for scenario {state.scenario!r} "
                    "while pipeline is registered"
                )
            return state

        if isinstance(state, state_cls):
            return state

        if type(state) is not TaskState:
            raise TypeError(
                f"Expected {state_cls.__name__} or bare TaskState for scenario "
                f"{state.scenario!r}, got {type(state).__name__}"
            )

        data = {
            field_name: getattr(state, field_name)
            for field_name in self._task_state_field_names
        }
        promoted = state_cls(**data)
        promoted.cancel_event = state.cancel_event
        return promoted
