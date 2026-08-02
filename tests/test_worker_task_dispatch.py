from dataclasses import dataclass, field

import pytest

from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.states import TaskState
from scenario_pipeliner.worker.execution.task_dispatch import (
    DEFAULT_PIPELINE_KEY,
    TaskExecutionRouter,
)


@dataclass
class _ScenarioState(TaskState):
    marker: str = "scenario"


@dataclass
class _OtherState(TaskState):
    marker: str = "other"


@dataclass
class _ScenarioStateRequired(TaskState):
    required_marker: str = field(kw_only=True)


def _pipeline() -> AsyncPipeline:
    return AsyncPipeline(steps=[])


def test_resolve_pipeline_prefers_exact_match() -> None:
    exact = _pipeline()
    fallback = _pipeline()
    router = TaskExecutionRouter(
        pipeline_factories={
            "scenario9": lambda: exact,
            DEFAULT_PIPELINE_KEY: lambda: fallback,
        },
        state_classes={},
    )

    factory, is_default = router.resolve_pipeline_factory("scenario9")

    assert factory is not None
    assert factory() is exact
    assert is_default is False


def test_resolve_pipeline_uses_default_fallback() -> None:
    fallback = _pipeline()
    router = TaskExecutionRouter(
        pipeline_factories={DEFAULT_PIPELINE_KEY: lambda: fallback},
        state_classes={},
    )

    factory, is_default = router.resolve_pipeline_factory("unknown")

    assert factory is not None
    assert factory() is fallback
    assert is_default is True


def test_resolve_pipeline_returns_none_when_unmapped() -> None:
    router = TaskExecutionRouter(pipeline_factories={}, state_classes={})

    factory, is_default = router.resolve_pipeline_factory("unknown")

    assert factory is None
    assert is_default is False


def test_promote_task_state_returns_same_when_unmapped() -> None:
    state = TaskState(task_id=1, scenario="unknown")
    router = TaskExecutionRouter(pipeline_factories={}, state_classes={})

    promoted = router.promote_task_state(state)

    assert promoted is state


def test_promote_task_state_raises_if_pipeline_exists_without_state_mapping() -> None:
    state = TaskState(task_id=1, scenario="scenario9")
    router = TaskExecutionRouter(
        pipeline_factories={"scenario9": _pipeline},
        state_classes={},
    )

    with pytest.raises(LookupError, match="No state class mapped"):
        router.promote_task_state(state)


def test_promote_task_state_returns_same_for_already_promoted_state() -> None:
    state = _ScenarioState(task_id=1, scenario="scenario9")
    router = TaskExecutionRouter(
        pipeline_factories={"scenario9": _pipeline},
        state_classes={"scenario9": _ScenarioState},
    )

    promoted = router.promote_task_state(state)

    assert promoted is state


def test_promote_task_state_raises_for_wrong_subclass_type() -> None:
    state = _OtherState(task_id=1, scenario="scenario9")
    router = TaskExecutionRouter(
        pipeline_factories={"scenario9": _pipeline},
        state_classes={"scenario9": _ScenarioState},
    )

    with pytest.raises(TypeError, match="Expected _ScenarioState"):
        router.promote_task_state(state)


def test_promote_task_state_copies_base_state_and_preserves_cancel_event() -> None:
    state = TaskState(task_id=7, scenario="scenario9")
    state.cancel_event.set()
    router = TaskExecutionRouter(
        pipeline_factories={"scenario9": _pipeline},
        state_classes={"scenario9": _ScenarioState},
    )

    promoted = router.promote_task_state(state)

    assert isinstance(promoted, _ScenarioState)
    assert promoted.task_id == 7
    assert promoted.scenario == "scenario9"
    assert promoted.cancel_event is state.cancel_event
    assert promoted.cancel_event.is_set() is True


def test_router_rejects_state_class_with_required_extra_fields() -> None:
    with pytest.raises(
        ValueError,
        match="cannot be promoted from TaskState",
    ):
        TaskExecutionRouter(
            pipeline_factories={"scenario9": _pipeline},
            state_classes={"scenario9": _ScenarioStateRequired},
        )
