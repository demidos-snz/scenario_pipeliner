from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from typing import Any

import asyncpg

from scenario_pipeliner.observability import setup_logging
from scenario_pipeliner.worker.execution.runner_db import RunnerDB
from scenario_pipeliner.worker.runtime.app_bootstrap import RuntimeBootstrap
from scenario_pipeliner.worker.runtime.runner_helpers import (
    build_postgres_runner,
    ensure_worker_settings,
)

logger = logging.getLogger(__name__)


class RunnerApp:
    """Opinionated process entrypoint used by CLI ``run`` and ``examples/main.py``."""

    def __init__(
        self,
        *,
        bootstrap: RuntimeBootstrap,
        pool: asyncpg.Pool[Any],
        runner: RunnerDB,
    ) -> None:
        self.bootstrap = bootstrap
        self.pool = pool
        self.runner = runner
        self._stop_event = asyncio.Event()

    @classmethod
    async def from_env(cls) -> "RunnerApp":
        setup_logging()
        bootstrap = RuntimeBootstrap.from_env()
        logger.info(
            "Starting worker: runner_mode=db db_backend=%s apply_migrations=%s "
            "plugins_root=%s",
            bootstrap.env.db_backend.value,
            bootstrap.env.apply_migrations,
            bootstrap.env.plugins_root.as_posix(),
        )
        pool = await bootstrap.create_pool()
        try:
            if bootstrap.env.apply_migrations:
                logger.info("Applying core and plugin migrations")
                await bootstrap.apply_core_migrations()
                applied = await bootstrap.apply_plugin_migrations(pool)
                if applied:
                    logger.info("Applied plugin migrations: %s", ", ".join(applied))
            else:
                logger.info(
                    "Skipping migrations "
                    "(--skip-migrations / RUNNER_APPLY_MIGRATIONS=false)"
                )
            plugin_services = bootstrap.plugin_services(pool)
            scenarios = bootstrap.list_scenarios(plugin_services)
            for scenario in scenarios:
                await ensure_worker_settings(pool=pool, scenario=scenario)
            runner = build_postgres_runner(
                config=bootstrap.env.scenario_config(),
                postgres_pool=pool,
                runner_settings=bootstrap.env.runner_settings(),
                plugin_services=plugin_services,
            )
            logger.info(
                "runner ready; scenarios=%s poll_interval=%ss tasks_limit=%s",
                ", ".join(scenarios),
                bootstrap.env.runner_poll_interval_seconds,
                bootstrap.env.runner_tasks_limit,
            )
            return cls(bootstrap=bootstrap, pool=pool, runner=runner)
        except Exception:
            await pool.close()
            raise

    def stop(self) -> None:
        if self._stop_event.is_set():
            return
        logger.info("Stop event received, halting runner")
        self._stop_event.set()
        self.runner.stop()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.stop)

        started_at = loop.time()
        timeout_seconds = self.bootstrap.env.run_seconds

        async with self.runner:
            runner_task = asyncio.create_task(
                self.runner.execute(), name="scenario-runner"
            )
            try:
                while not self._stop_event.is_set():
                    if timeout_seconds is not None and (
                        loop.time() - started_at >= timeout_seconds
                    ):
                        logger.info(
                            "run timeout %ss reached; stopping", timeout_seconds
                        )
                        self.stop()
                        break
                    await asyncio.sleep(1)
            finally:
                await runner_task
        await self.pool.close()
