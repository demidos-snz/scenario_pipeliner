import asyncio
from dataclasses import dataclass
from pathlib import Path

import pytest

import scenario_pipeliner.worker.runtime.factories as worker_bootstrap
from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.api.settings import ScenarioPipelinerConfig
from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.settings import RunnerDBSettings
from scenario_pipeliner.worker.core.states import TaskState
from scenario_pipeliner.worker.execution.db_orchestrator import (
    ExecutionBatchState,
)
from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    ScenarioPluginDefinition,
)
from scenario_pipeliner.worker.runtime.factories import (
    build_pipeline_factories_from_registry,
    create_native_task_repository,
    create_runner_db,
)
from scenario_pipeliner.worker.task_repositories import (
    PostgresTaskRepository,
    TaskRepository,
)


@dataclass
class _ScenarioState(TaskState):
    promoted: bool = True


class _SpyPipeline(AsyncPipeline):
    def __init__(self) -> None:
        super().__init__(steps=[])
        self.calls = 0
        self.enter_calls = 0
        self.exit_calls = 0

    async def __aenter__(self) -> AsyncPipeline:
        self.enter_calls += 1
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.exit_calls += 1
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def execute(self, state=None) -> None:  # type: ignore[override]
        self.calls += 1


def test_build_pipeline_factories_from_registry_returns_registered_factories() -> None:
    registry = MainPipelinePluginRegistry()
    produced: list[_SpyPipeline] = []

    def _factory() -> AsyncPipeline:
        pipeline = _SpyPipeline()
        produced.append(pipeline)
        return pipeline

    registry.register(
        ScenarioPluginDefinition(
            scenario="scenario9",
            pipeline_factory=_factory,
            state_cls=_ScenarioState,
        )
    )

    pipeline_factories = build_pipeline_factories_from_registry(registry)

    assert "scenario9" in pipeline_factories
    assert isinstance(pipeline_factories["scenario9"](), AsyncPipeline)
    assert produced


def test_create_runner_db_wires_registry_and_executes() -> None:
    registry = MainPipelinePluginRegistry()
    produced: list[_SpyPipeline] = []

    def _factory() -> AsyncPipeline:
        pipeline = _SpyPipeline()
        produced.append(pipeline)
        return pipeline

    registry.register(
        ScenarioPluginDefinition(
            scenario="scenario9",
            pipeline_factory=_factory,
            state_cls=_ScenarioState,
        )
    )

    calls = {"count": 0}

    async def fetch_batch_state() -> ExecutionBatchState:
        calls["count"] += 1
        if calls["count"] > 1:
            runner.stop()
            return ExecutionBatchState(tasks=[])
        return ExecutionBatchState(tasks=[TaskState(task_id=1, scenario="scenario9")])

    runner = create_runner_db(
        fetch_batch_state=fetch_batch_state,
        plugin_registry=registry,
        runner_settings=RunnerDBSettings(POLL_INTERVAL_SECONDS=1),
    )

    async def _wait_immediately(seconds: int) -> None:
        return

    runner._wait_before_next_poll = _wait_immediately  # type: ignore[method-assign]
    asyncio.run(runner.execute())

    assert calls["count"] >= 2
    assert len(produced) == 1
    assert produced[0].calls == 1
    assert produced[0].enter_calls == 1
    assert produced[0].exit_calls == 1


def test_create_runner_db_forwards_on_task_error_hook() -> None:
    registry = MainPipelinePluginRegistry()
    calls = {"count": 0}
    captured: list[tuple[int, str]] = []

    async def fetch_batch_state() -> ExecutionBatchState:
        calls["count"] += 1
        if calls["count"] > 1:
            runner.stop()
            return ExecutionBatchState(tasks=[])
        return ExecutionBatchState(tasks=[TaskState(task_id=10, scenario="unknown")])

    async def on_task_error(task: TaskState, error: Exception) -> None:
        captured.append((task.task_id, str(error)))

    runner = create_runner_db(
        fetch_batch_state=fetch_batch_state,
        plugin_registry=registry,
        runner_settings=RunnerDBSettings(POLL_INTERVAL_SECONDS=1),
        on_task_error=on_task_error,
    )

    async def _wait_immediately(seconds: int) -> None:
        return

    runner._wait_before_next_poll = _wait_immediately  # type: ignore[method-assign]
    asyncio.run(runner.execute())

    assert captured
    assert captured[0][0] == 10
    assert "Pipeline for scenario" in captured[0][1]


def test_create_runner_db_reports_factory_type_errors_via_on_task_error() -> None:
    registry = MainPipelinePluginRegistry()
    calls = {"count": 0}
    captured: list[tuple[int, str]] = []

    registry.register(
        ScenarioPluginDefinition(
            scenario="scenario9",
            pipeline_factory=lambda: object(),  # type: ignore[arg-type]
            state_cls=_ScenarioState,
        )
    )

    async def fetch_batch_state() -> ExecutionBatchState:
        calls["count"] += 1
        if calls["count"] > 1:
            runner.stop()
            return ExecutionBatchState(tasks=[])
        return ExecutionBatchState(tasks=[TaskState(task_id=11, scenario="scenario9")])

    async def on_task_error(task: TaskState, error: Exception) -> None:
        captured.append((task.task_id, str(error)))

    runner = create_runner_db(
        fetch_batch_state=fetch_batch_state,
        plugin_registry=registry,
        runner_settings=RunnerDBSettings(POLL_INTERVAL_SECONDS=1),
        on_task_error=on_task_error,
    )

    async def _wait_immediately(seconds: int) -> None:
        return

    runner._wait_before_next_poll = _wait_immediately  # type: ignore[method-assign]
    asyncio.run(runner.execute())

    assert captured
    assert captured[0][0] == 11
    assert "expected AsyncPipeline" in captured[0][1]


def test_create_runner_db_from_config_builds_runtime_registry(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = MainPipelinePluginRegistry()
    produced: list[_SpyPipeline] = []

    def _factory() -> AsyncPipeline:
        pipeline = _SpyPipeline()
        produced.append(pipeline)
        return pipeline

    registry.register(
        ScenarioPluginDefinition(
            scenario="scenario9",
            pipeline_factory=_factory,
            state_cls=_ScenarioState,
        )
    )

    monkeypatch.setattr(
        worker_bootstrap,
        "build_worker_registry_from_manifests",
        lambda _config, **_kwargs: registry,
    )

    config = ScenarioPipelinerConfig(
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    calls = {"count": 0}

    async def fetch_batch_state() -> ExecutionBatchState:
        calls["count"] += 1
        if calls["count"] > 1:
            runner.stop()
            return ExecutionBatchState(tasks=[])
        return ExecutionBatchState(tasks=[TaskState(task_id=99, scenario="scenario9")])

    runner = worker_bootstrap.create_runner_db_from_config(
        config=config,
        fetch_batch_state=fetch_batch_state,
        runner_settings=RunnerDBSettings(POLL_INTERVAL_SECONDS=1),
    )

    async def _wait_immediately(seconds: int) -> None:
        return

    runner._wait_before_next_poll = _wait_immediately  # type: ignore[method-assign]
    asyncio.run(runner.execute())

    assert calls["count"] >= 2
    assert len(produced) == 1
    assert produced[0].calls == 1


def test_create_runner_db_with_repository_wires_default_persistence_hooks() -> None:
    registry = MainPipelinePluginRegistry()
    produced: list[_SpyPipeline] = []

    def _factory() -> AsyncPipeline:
        pipeline = _SpyPipeline()
        produced.append(pipeline)
        return pipeline

    registry.register(
        ScenarioPluginDefinition(
            scenario="scenario9",
            pipeline_factory=_factory,
            state_cls=_ScenarioState,
        )
    )

    class _Repo(TaskRepository):
        def __init__(self) -> None:
            self.calls = 0
            self.marked_running: list[int] = []
            self.persisted_results: list[int] = []

        async def fetch_execution_batch(
            self,
            *,
            settings: RunnerDBSettings,
        ) -> ExecutionBatchState:
            self.calls += 1
            if self.calls > 1:
                runner.stop()
                return ExecutionBatchState(tasks=[])
            assert settings.TASKS_LIMIT == 10
            return ExecutionBatchState(
                tasks=[TaskState(task_id=77, scenario="scenario9")]
            )

        async def mark_task_running(self, task: TaskState) -> None:
            self.marked_running.append(task.task_id)

        async def persist_task_result(self, task: TaskState) -> None:
            self.persisted_results.append(task.task_id)

        async def persist_task_error(self, task: TaskState, error: Exception) -> None:
            raise AssertionError("unexpected error callback")

        async def persist_timeout(self, tasks: list[TaskState]) -> None:
            raise AssertionError("unexpected timeout callback")

    repository = _Repo()
    runner = worker_bootstrap.create_runner_db_with_repository(
        repository=repository,
        plugin_registry=registry,
        runner_settings=RunnerDBSettings(POLL_INTERVAL_SECONDS=1),
    )

    async def _wait_immediately(seconds: int) -> None:
        return

    runner._wait_before_next_poll = _wait_immediately  # type: ignore[method-assign]
    asyncio.run(runner.execute())

    assert repository.marked_running == [77]
    assert repository.persisted_results == [77]
    assert len(produced) == 1


def test_create_runner_db_with_repository_allows_error_hook_override() -> None:
    registry = MainPipelinePluginRegistry()
    repository_calls = {"default_error": 0}
    captured: list[tuple[int, str]] = []

    class _Repo(TaskRepository):
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_execution_batch(
            self,
            *,
            settings: RunnerDBSettings,
        ) -> ExecutionBatchState:
            self.calls += 1
            if self.calls > 1:
                runner.stop()
                return ExecutionBatchState(tasks=[])
            return ExecutionBatchState(
                tasks=[TaskState(task_id=91, scenario="unknown")]
            )

        async def mark_task_running(self, task: TaskState) -> None:
            return

        async def persist_task_result(self, task: TaskState) -> None:
            return

        async def persist_task_error(self, task: TaskState, error: Exception) -> None:
            repository_calls["default_error"] += 1

        async def persist_timeout(self, tasks: list[TaskState]) -> None:
            return

    async def custom_on_task_error(task: TaskState, error: Exception) -> None:
        captured.append((task.task_id, str(error)))

    runner = worker_bootstrap.create_runner_db_with_repository(
        repository=_Repo(),
        plugin_registry=registry,
        runner_settings=RunnerDBSettings(POLL_INTERVAL_SECONDS=1),
        on_task_error=custom_on_task_error,
    )

    async def _wait_immediately(seconds: int) -> None:
        return

    runner._wait_before_next_poll = _wait_immediately  # type: ignore[method-assign]
    asyncio.run(runner.execute())

    assert captured
    assert captured[0][0] == 91
    assert "Pipeline for scenario" in captured[0][1]
    assert repository_calls["default_error"] == 0


def test_create_native_task_repository_requires_db_for_postgres() -> None:
    with pytest.raises(ValueError, match="db is required"):
        create_native_task_repository(db_backend=DbBackend.POSTGRESQL)


def test_create_native_task_repository_builds_postgres_repository(
    core_db: CoreDb,
) -> None:
    repository = create_native_task_repository(
        db_backend=DbBackend.POSTGRESQL,
        db=core_db,
    )

    assert isinstance(repository, PostgresTaskRepository)


def test_create_runner_db_from_config_with_native_repository_forwards_plugin_services(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    core_db: CoreDb,
) -> None:
    sentinel_runner = object()

    class _Repo(TaskRepository):
        async def fetch_execution_batch(
            self,
            *,
            settings: RunnerDBSettings,
        ) -> ExecutionBatchState:
            return ExecutionBatchState(tasks=[])

        async def mark_task_running(self, task: TaskState) -> None:
            return

        async def persist_task_result(self, task: TaskState) -> None:
            return

        async def persist_task_error(self, task: TaskState, error: Exception) -> None:
            return

        async def persist_timeout(self, tasks: list[TaskState]) -> None:
            return

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        worker_bootstrap,
        "create_native_task_repository",
        lambda **_kwargs: _Repo(),
    )

    def _fake_create_runner_db_from_config_and_repository(**kwargs):
        captured.update(kwargs)
        return sentinel_runner

    monkeypatch.setattr(
        worker_bootstrap,
        "create_runner_db_from_config_and_repository",
        _fake_create_runner_db_from_config_and_repository,
    )

    config = ScenarioPipelinerConfig(
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    plugin_services = {
        "hello_scenario": {
            "postgres_pool": object(),
            "api_client_provider": lambda: object(),
        }
    }
    runner = worker_bootstrap.create_runner_db_from_config_with_native_repository(
        config=config,
        db=core_db,
        plugin_services=plugin_services,
    )

    assert runner is sentinel_runner
    assert captured["plugin_services"] is plugin_services


def test_runtime_bootstrap_reuses_worker_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scenario_pipeliner.worker.runtime.app_bootstrap import RuntimeBootstrap

    calls = {"n": 0}
    registry = MainPipelinePluginRegistry()

    def _build(*_args, **_kwargs):
        calls["n"] += 1
        return registry

    monkeypatch.setattr(
        "scenario_pipeliner.worker.runtime.app_bootstrap.build_worker_registry_from_manifests",
        _build,
    )
    bootstrap = RuntimeBootstrap.__new__(RuntimeBootstrap)
    bootstrap._worker_registry = None

    class _Env:
        plugins_root = Path("plugins")

        def scenario_config(self):
            return object()

    bootstrap.env = _Env()  # type: ignore[assignment]
    first = bootstrap.worker_registry({})
    second = bootstrap.worker_registry({})
    assert first is second is registry
    assert calls["n"] == 1
