from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from typing import Any

import asyncpg

from scenario_pipeliner.db.schema_utils import (
    require_core_tables,
    sync_core_identity_sequences,
)
from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.env_loader import load_environment_file
from scenario_pipeliner.observability import setup_logging
from scenario_pipeliner.worker.broker.runner import BrokerRunner
from scenario_pipeliner.worker.broker.settings import (
    BrokerPluginBinding,
    PluginBrokerSettings,
)
from scenario_pipeliner.worker.core.custom_settings import RabbitMQSettings
from scenario_pipeliner.worker.execution.runner_db import RunnerDB
from scenario_pipeliner.worker.plugin_registry import MainPipelinePluginRegistry
from scenario_pipeliner.worker.runtime.app_bootstrap import RuntimeBootstrap
from scenario_pipeliner.worker.runtime.runner_helpers import (
    build_postgres_runner,
    ensure_worker_settings,
)

logger = logging.getLogger(__name__)


def build_broker_bindings(
    registry: MainPipelinePluginRegistry,
) -> list[BrokerPluginBinding]:
    bindings: list[BrokerPluginBinding] = []
    for plugin_name, definition in registry.broker_plugins.items():
        settings = definition.settings_provider()
        if not isinstance(settings, PluginBrokerSettings):
            settings = PluginBrokerSettings.model_validate(settings)
        plugin_scenarios = [
            scenario
            for scenario in registry.scenarios
            if scenario.startswith(f"{plugin_name}.") or scenario == plugin_name
        ]
        if not plugin_scenarios:
            raise ValueError(
                f"broker plugin {plugin_name!r} has no owned scenarios "
                f"(expected keys starting with {plugin_name!r}. or "
                f"equal to {plugin_name!r})"
            )
        bindings.append(
            BrokerPluginBinding(
                plugin_name=plugin_name,
                hooks=definition.hooks,
                settings=settings,
                scenarios=plugin_scenarios,
            )
        )
    return bindings


class RunnerApp:
    """Opinionated process entrypoint used by CLI ``run`` and ``examples/main.py``."""

    def __init__(
        self,
        *,
        bootstrap: RuntimeBootstrap,
        pool: asyncpg.Pool[Any],
        db: CoreDb,
        runner: RunnerDB | None = None,
        broker_runner: BrokerRunner | None = None,
        runner_mode: str = "db",
    ) -> None:
        self.bootstrap = bootstrap
        self.pool = pool
        self.db = db
        self.runner = runner
        self.broker_runner = broker_runner
        self.runner_mode = runner_mode
        self._stop_event = asyncio.Event()

    @classmethod
    async def from_env(cls) -> "RunnerApp":
        load_environment_file()
        setup_logging()
        bootstrap = RuntimeBootstrap.from_env()
        runner_mode = bootstrap.env.runner_mode
        logger.info(
            "Starting worker: runner_mode=%s db_backend=%s db_schema=%s "
            "apply_migrations=%s plugins_root=%s",
            runner_mode,
            bootstrap.env.db_backend.value,
            bootstrap.db_schema(),
            bootstrap.env.apply_migrations,
            bootstrap.env.plugins_root.as_posix(),
        )
        engine = bootstrap.create_engine()
        db = bootstrap.core_db(engine)
        pool = await bootstrap.create_pool()
        try:
            if bootstrap.env.apply_migrations:
                logger.info("Applying core and plugin migrations")
                await bootstrap.apply_core_migrations()
                applied = await bootstrap.apply_plugin_migrations(engine)
                if applied:
                    logger.info("Applied plugin migrations: %s", ", ".join(applied))
            else:
                logger.info(
                    "Skipping migrations "
                    "(--skip-migrations / RUNNER_APPLY_MIGRATIONS=false)"
                )
            plugin_services = bootstrap.plugin_services(pool)
            await require_core_tables(db)
            await sync_core_identity_sequences(db.engine, db.core)
            registry = bootstrap.worker_registry(plugin_services)
            scenarios = bootstrap.list_scenarios(plugin_services)
            for scenario in scenarios:
                await ensure_worker_settings(db=db, scenario=scenario)

            db_runner: RunnerDB | None = None
            broker_runner: BrokerRunner | None = None

            if runner_mode in {"db", "all"}:
                db_runner = build_postgres_runner(
                    config=bootstrap.env.scenario_config(),
                    db=db,
                    runner_settings=bootstrap.env.runner_settings(),
                    plugin_services=plugin_services,
                    plugin_registry=registry,
                )
                logger.info(
                    "db runner ready; scenarios=%s poll_interval=%ss tasks_limit=%s",
                    ", ".join(scenarios),
                    bootstrap.env.runner_poll_interval_seconds,
                    bootstrap.env.runner_tasks_limit,
                )

            if runner_mode in {"broker", "all"}:
                bindings = build_broker_bindings(registry)
                broker_runner = BrokerRunner(
                    db=db,
                    bindings=bindings,
                    rabbit_settings=RabbitMQSettings.model_validate(dict(os.environ)),
                    runner_settings=bootstrap.env.broker_runner_settings(),
                )
                logger.info(
                    "broker runner ready; plugins_with_hooks=%s",
                    ",".join(sorted(registry.broker_plugins)) or "(none)",
                )

            return cls(
                bootstrap=bootstrap,
                pool=pool,
                db=db,
                runner=db_runner,
                broker_runner=broker_runner,
                runner_mode=runner_mode,
            )
        except Exception:
            await engine.dispose()
            await pool.close()
            raise

    def stop(self) -> None:
        if self._stop_event.is_set():
            return
        logger.info("Stop event received, halting runner")
        self._stop_event.set()
        if self.runner is not None:
            self.runner.stop()
        if self.broker_runner is not None:
            self.broker_runner.stop()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.stop)

        started_at = loop.time()
        timeout_seconds = self.bootstrap.env.run_seconds
        tasks: list[asyncio.Task[Any]] = []

        async def _supervise() -> None:
            while not self._stop_event.is_set():
                if timeout_seconds is not None and (
                    loop.time() - started_at >= timeout_seconds
                ):
                    logger.info("run timeout %ss reached; stopping", timeout_seconds)
                    self.stop()
                    break
                await asyncio.sleep(1)

        try:
            if self.runner is not None:
                await self.runner.__aenter__()
                tasks.append(
                    asyncio.create_task(
                        self.runner.execute(), name="scenario-db-runner"
                    )
                )
            if self.broker_runner is not None:
                await self.broker_runner.__aenter__()
                tasks.append(
                    asyncio.create_task(
                        self.broker_runner.execute(), name="scenario-broker-runner"
                    )
                )
            if not tasks:
                raise RuntimeError(
                    f"no runners configured for mode={self.runner_mode!r}"
                )

            supervisor = asyncio.create_task(_supervise(), name="scenario-supervisor")
            try:
                await supervisor
            finally:
                self.stop()
                drain = self.bootstrap.env.runner_shutdown_timeout_seconds
                if tasks:
                    _done, pending = await asyncio.wait(tasks, timeout=drain)
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            if self.broker_runner is not None:
                await self.broker_runner.__aexit__(None, None, None)
            if self.runner is not None:
                await self.runner.__aexit__(None, None, None)
            await self.pool.close()
            await self.db.engine.dispose()
