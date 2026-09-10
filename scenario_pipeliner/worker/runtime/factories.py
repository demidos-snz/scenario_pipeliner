import logging
from collections.abc import Awaitable, Callable
from typing import Any

from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.api.settings import ScenarioPipelinerConfig
from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.settings import RunnerDBSettings
from scenario_pipeliner.worker.execution.batch_executor import (
    ExecutionBatchRunner,
)
from scenario_pipeliner.worker.execution.db_orchestrator import (
    BatchRuntimeControl,
    DBExecutionOrchestrator,
    ExecutionBatchState,
    PollerFactory,
    TaskErrorHandler,
    TaskStateHandler,
    TimeoutHandler,
)
from scenario_pipeliner.worker.execution.runner_db import (
    RunnerDB,
)
from scenario_pipeliner.worker.execution.task_dispatch import (
    DEFAULT_PIPELINE_KEY,
    TaskExecutionRouter,
)
from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    PipelineFactory,
)
from scenario_pipeliner.worker.runtime.registry import (
    build_worker_registry_from_manifests,
)
from scenario_pipeliner.worker.runtime.settings_bridge import ExecuteSettings
from scenario_pipeliner.worker.task_repositories import (
    PostgresTaskRepository,
    PostgresTaskStorage,
    TaskRepository,
)

logger = logging.getLogger(__name__)

FetchBatchState = Callable[[], Awaitable[ExecutionBatchState]]


def build_pipeline_factories_from_registry(
    registry: MainPipelinePluginRegistry,
) -> dict[str, PipelineFactory]:
    return dict(registry.pipeline_factories)


def create_runner_db(
    *,
    fetch_batch_state: FetchBatchState,
    plugin_registry: MainPipelinePluginRegistry,
    runner_settings: RunnerDBSettings | None = None,
    max_concurrent_tasks: int | None = None,
    default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    poller_factory: PollerFactory | None = None,
    runtime_control: BatchRuntimeControl | None = None,
    root_pipeline: AsyncPipeline | None = None,
    on_task_start: TaskStateHandler | None = None,
    on_task_success: TaskStateHandler | None = None,
    on_task_error: TaskErrorHandler | None = None,
    on_timeout: TimeoutHandler | None = None,
) -> RunnerDB:
    settings = runner_settings or RunnerDBSettings()
    execute_settings = ExecuteSettings.from_registry(plugin_registry)
    pipeline_factories = build_pipeline_factories_from_registry(plugin_registry)
    router = TaskExecutionRouter(
        pipeline_factories=pipeline_factories,
        state_classes=execute_settings.states_mapper,
        default_pipeline_key=default_pipeline_key,
    )
    batch_runner = ExecutionBatchRunner(
        shutdown_timeout_seconds=settings.SHUTDOWN_TIMEOUT_SECONDS
    )
    concurrent = (
        max_concurrent_tasks
        if max_concurrent_tasks is not None
        else settings.MAX_CONCURRENT_TASKS
    )
    orchestrator = DBExecutionOrchestrator(
        router=router,
        batch_runner=batch_runner,
        max_concurrent_tasks=concurrent,
        poller_factory=poller_factory,
        runtime_control=runtime_control,
        heartbeat_interval_seconds=settings.HEARTBEAT_INTERVAL_SECONDS,
    )

    return RunnerDB(
        pipeline=root_pipeline or AsyncPipeline(steps=[]),
        settings=settings,
        fetch_batch_state=fetch_batch_state,
        orchestrator=orchestrator,
        on_task_start=on_task_start,
        on_task_success=on_task_success,
        on_task_error=on_task_error,
        on_timeout=on_timeout,
    )


def create_runner_db_from_config(
    *,
    config: ScenarioPipelinerConfig,
    fetch_batch_state: FetchBatchState,
    plugin_services: dict[str, Any] | None = None,
    runner_settings: RunnerDBSettings | None = None,
    max_concurrent_tasks: int | None = None,
    default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    poller_factory: PollerFactory | None = None,
    runtime_control: BatchRuntimeControl | None = None,
    root_pipeline: AsyncPipeline | None = None,
    on_task_start: TaskStateHandler | None = None,
    on_task_success: TaskStateHandler | None = None,
    on_task_error: TaskErrorHandler | None = None,
    on_timeout: TimeoutHandler | None = None,
) -> RunnerDB:
    plugin_registry = build_worker_registry_from_manifests(
        config,
        plugin_services=plugin_services,
    )
    return create_runner_db(
        fetch_batch_state=fetch_batch_state,
        plugin_registry=plugin_registry,
        runner_settings=runner_settings,
        max_concurrent_tasks=max_concurrent_tasks,
        default_pipeline_key=default_pipeline_key,
        poller_factory=poller_factory,
        runtime_control=runtime_control,
        root_pipeline=root_pipeline,
        on_task_start=on_task_start,
        on_task_success=on_task_success,
        on_task_error=on_task_error,
        on_timeout=on_timeout,
    )


def create_runner_db_with_repository(
    *,
    repository: TaskRepository,
    plugin_registry: MainPipelinePluginRegistry,
    runner_settings: RunnerDBSettings | None = None,
    max_concurrent_tasks: int | None = None,
    default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    poller_factory: PollerFactory | None = None,
    runtime_control: BatchRuntimeControl | None = None,
    root_pipeline: AsyncPipeline | None = None,
    on_task_error: TaskErrorHandler | None = None,
    on_timeout: TimeoutHandler | None = None,
) -> RunnerDB:
    settings = runner_settings or RunnerDBSettings()
    control = runtime_control
    if control is None and isinstance(repository, PostgresTaskRepository):
        control = repository

    async def fetch_batch_state() -> ExecutionBatchState:
        return await repository.fetch_execution_batch(settings=settings)

    async def default_on_task_start(task) -> None:
        await repository.mark_task_running(task)

    async def default_on_task_success(task) -> None:
        try:
            await repository.persist_task_result(task)
            logger.info(
                "Task %s finished ok=%s scenario=%s",
                task.task_id,
                task.result.ok,
                task.scenario,
            )
        except Exception:
            logger.exception("Failed to persist result for task %s", task.task_id)
            raise

    async def default_on_task_error(task, error: Exception) -> None:
        try:
            await repository.persist_task_error(task, error)
        except Exception:
            logger.exception("Failed to persist error for task %s", task.task_id)
            raise

    async def default_on_timeout(tasks) -> None:
        logger.warning(
            "Persisting timeout for %s task(s) after shutdown timeout",
            len(tasks),
        )
        await repository.persist_timeout(tasks)

    return create_runner_db(
        fetch_batch_state=fetch_batch_state,
        plugin_registry=plugin_registry,
        runner_settings=settings,
        max_concurrent_tasks=max_concurrent_tasks,
        default_pipeline_key=default_pipeline_key,
        poller_factory=poller_factory,
        runtime_control=control,
        root_pipeline=root_pipeline,
        on_task_start=default_on_task_start,
        on_task_success=default_on_task_success,
        on_task_error=on_task_error or default_on_task_error,
        on_timeout=on_timeout or default_on_timeout,
    )


def create_runner_db_from_config_and_repository(
    *,
    config: ScenarioPipelinerConfig,
    repository: TaskRepository,
    plugin_services: dict[str, Any] | None = None,
    plugin_registry: MainPipelinePluginRegistry | None = None,
    runner_settings: RunnerDBSettings | None = None,
    max_concurrent_tasks: int | None = None,
    default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    poller_factory: PollerFactory | None = None,
    runtime_control: BatchRuntimeControl | None = None,
    root_pipeline: AsyncPipeline | None = None,
    on_task_error: TaskErrorHandler | None = None,
    on_timeout: TimeoutHandler | None = None,
) -> RunnerDB:
    registry = plugin_registry or build_worker_registry_from_manifests(
        config,
        plugin_services=plugin_services,
    )
    return create_runner_db_with_repository(
        repository=repository,
        plugin_registry=registry,
        runner_settings=runner_settings,
        max_concurrent_tasks=max_concurrent_tasks,
        default_pipeline_key=default_pipeline_key,
        poller_factory=poller_factory,
        runtime_control=runtime_control,
        root_pipeline=root_pipeline,
        on_task_error=on_task_error,
        on_timeout=on_timeout,
    )


def create_native_task_repository(
    *,
    db_backend: DbBackend,
    db: CoreDb | None = None,
) -> TaskRepository:
    if db is None:
        raise ValueError("db is required for PostgreSQL native repository")
    storage = PostgresTaskStorage(engine=db.engine, core=db.core)
    return PostgresTaskRepository(storage=storage)


def create_runner_db_from_config_with_native_repository(
    *,
    config: ScenarioPipelinerConfig,
    db: CoreDb,
    plugin_services: dict[str, Any] | None = None,
    plugin_registry: MainPipelinePluginRegistry | None = None,
    runner_settings: RunnerDBSettings | None = None,
    max_concurrent_tasks: int | None = None,
    default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    poller_factory: PollerFactory | None = None,
    runtime_control: BatchRuntimeControl | None = None,
    root_pipeline: AsyncPipeline | None = None,
    on_task_error: TaskErrorHandler | None = None,
    on_timeout: TimeoutHandler | None = None,
) -> RunnerDB:
    repository = create_native_task_repository(
        db_backend=config.db_backend,
        db=db,
    )
    return create_runner_db_from_config_and_repository(
        config=config,
        repository=repository,
        plugin_services=plugin_services,
        plugin_registry=plugin_registry,
        runner_settings=runner_settings,
        max_concurrent_tasks=max_concurrent_tasks,
        default_pipeline_key=default_pipeline_key,
        poller_factory=poller_factory,
        runtime_control=runtime_control,
        root_pipeline=root_pipeline,
        on_task_error=on_task_error,
        on_timeout=on_timeout,
    )
