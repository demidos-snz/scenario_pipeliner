from __future__ import annotations

from typing import Any

import asyncpg

from scenario_pipeliner.api.config import CoreMigrationConfig
from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.core.core_migrate import apply_core_migrations_async
from scenario_pipeliner.core.plugin_migrate import apply_plugin_migrations_async
from scenario_pipeliner.worker.runtime.registry import (
    build_worker_registry_from_manifests,
)
from scenario_pipeliner.worker.runtime.settings import (
    PostgresRuntimeSettings,
    RuntimeEnvSettings,
)


class RuntimeBootstrap:
    def __init__(
        self,
        env: RuntimeEnvSettings,
        postgres: PostgresRuntimeSettings,
    ) -> None:
        self.env = env
        self.postgres = postgres

    @classmethod
    def from_env(cls) -> "RuntimeBootstrap":
        return cls(
            env=RuntimeEnvSettings.from_env(),
            postgres=PostgresRuntimeSettings.from_env(),
        )

    async def create_pool(self) -> asyncpg.Pool[Any]:
        return await asyncpg.create_pool(
            dsn=self.postgres.sync_url,
            min_size=self.postgres.DB_POOL_MIN_SIZE,
            max_size=self.postgres.DB_POOL_MAX_SIZE,
        )

    async def apply_core_migrations(self) -> None:
        await apply_core_migrations_async(
            CoreMigrationConfig(
                db_backend=DbBackend.POSTGRESQL,
                postgres_host=self.postgres.POSTGRES_HOST,
                postgres_port=self.postgres.POSTGRES_PORT,
                postgres_db=self.postgres.POSTGRES_DB,
                postgres_user=self.postgres.POSTGRES_USER,
                postgres_password=self.postgres.POSTGRES_PASSWORD,
            )
        )

    async def apply_plugin_migrations(self, pool: asyncpg.Pool[Any]) -> list[str]:
        return await apply_plugin_migrations_async(self.env.scenario_config(), pool)

    @staticmethod
    def plugin_services(pool: asyncpg.Pool[Any]) -> dict[str, Any]:
        """Hook for host-app DI. Default is empty; plugins may read env themselves."""
        _ = pool
        return {}

    def list_scenarios(self, plugin_services: dict[str, Any]) -> list[str]:
        registry = build_worker_registry_from_manifests(
            self.env.scenario_config(),
            plugin_services=plugin_services,
        )
        if not registry.scenarios:
            raise RuntimeError(
                f"no registered scenarios in {self.env.plugins_root.as_posix()}"
            )
        return list(registry.scenarios)
