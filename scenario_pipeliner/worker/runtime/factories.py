import logging
from collections.abc import Awaitable, Callable
from typing import Any

from scenario_pipeliner.api.config import ScenarioPipelinerConfig
from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.worker.core.pipeline import AsyncPipeline
from scenario_pipeliner.worker.core.settings import (
    RepositoryPollSettings,
    RunnerDBSettings,
    TableSettings,
)
from scenario_pipeliner.worker.execution.batch_executor import (
    ExecutionBatchRunner,
)
from scenario_pipeliner.worker.execution.db_orchestrator import (
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
    PostgresPoolProtocol,
    PostgresTaskRepository,
    PostgresTaskStorage,
    SQLiteTaskRepositoryStub,
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
    max_concurrent_tasks: int = 5,
    default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    poller_factory: PollerFactory | None = None,
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
    orchestrator = DBExecutionOrchestrator(
        router=router,
        batch_runner=batch_runner,
        max_concurrent_tasks=max_concurrent_tasks,
        poller_factory=poller_factory,
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
    max_concurrent_tasks: int = 5,
    default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    poller_factory: PollerFactory | None = None,
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
    max_concurrent_tasks: int = 5,
    default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    poller_factory: PollerFactory | None = None,
    root_pipeline: AsyncPipeline | None = None,
    on_task_error: TaskErrorHandler | None = None,
    on_timeout: TimeoutHandler | None = None,
) -> RunnerDB:
    settings = runner_settings or RunnerDBSettings()
    poll_settings = RepositoryPollSettings(
        tasks_limit=settings.TASKS_LIMIT,
        zombie_timeout_minutes=settings.ZOMBIE_TASKS_TIMEOUT_MINUTES,
    )

    async def fetch_batch_state() -> ExecutionBatchState:
        return await repository.fetch_execution_batch(settings=poll_settings)

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
            logger.exception(
                "Failed to persist result for task %s", task.task_id
            )
            raise

    async def default_on_task_error(task, error: Exception) -> None:
        try:
            await repository.persist_task_error(task, error)
        except Exception:
            logger.exception(
                "Failed to persist error for task %s", task.task_id
            )
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
    runner_settings: RunnerDBSettings | None = None,
    max_concurrent_tasks: int = 5,
    default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    poller_factory: PollerFactory | None = None,
    root_pipeline: AsyncPipeline | None = None,
    on_task_error: TaskErrorHandler | None = None,
    on_timeout: TimeoutHandler | None = None,
) -> RunnerDB:
    plugin_registry = build_worker_registry_from_manifests(
        config,
        plugin_services=plugin_services,
    )
    return create_runner_db_with_repository(
        repository=repository,
        plugin_registry=plugin_registry,
        runner_settings=runner_settings,
        max_concurrent_tasks=max_concurrent_tasks,
        default_pipeline_key=default_pipeline_key,
        poller_factory=poller_factory,
        root_pipeline=root_pipeline,
        on_task_error=on_task_error,
        on_timeout=on_timeout,
    )


def create_native_task_repository(
    *,
    db_backend: DbBackend,
    postgres_pool: PostgresPoolProtocol | None = None,
    table_settings: TableSettings | None = None,
) -> TaskRepository:
    table_settings = table_settings or TableSettings()
    if db_backend == DbBackend.POSTGRESQL:
        if postgres_pool is None:
            raise ValueError(
                "postgres_pool is required for DbBackend.POSTGRESQL native repository"
            )
        storage = PostgresTaskStorage(
            pool=postgres_pool,
            table_settings=table_settings,
        )
        return PostgresTaskRepository(storage=storage)
    return SQLiteTaskRepositoryStub()


def create_runner_db_from_config_with_native_repository(
    *,
    config: ScenarioPipelinerConfig,
    postgres_pool: PostgresPoolProtocol | None = None,
    table_settings: TableSettings | None = None,
    plugin_services: dict[str, Any] | None = None,
    runner_settings: RunnerDBSettings | None = None,
    max_concurrent_tasks: int = 5,
    default_pipeline_key: str = DEFAULT_PIPELINE_KEY,
    poller_factory: PollerFactory | None = None,
    root_pipeline: AsyncPipeline | None = None,
    on_task_error: TaskErrorHandler | None = None,
    on_timeout: TimeoutHandler | None = None,
) -> RunnerDB:
    repository = create_native_task_repository(
        db_backend=config.db_backend,
        postgres_pool=postgres_pool,
        table_settings=table_settings or TableSettings(),
    )
    return create_runner_db_from_config_and_repository(
        config=config,
        repository=repository,
        plugin_services=plugin_services,
        runner_settings=runner_settings,
        max_concurrent_tasks=max_concurrent_tasks,
        default_pipeline_key=default_pipeline_key,
        poller_factory=poller_factory,
        root_pipeline=root_pipeline,
        on_task_error=on_task_error,
        on_timeout=on_timeout,
    )
