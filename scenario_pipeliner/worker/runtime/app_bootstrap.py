from __future__ import annotations

from typing import Any

import asyncpg
from sqlalchemy.ext.asyncio import AsyncEngine

from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.api.settings import CoreMigrationConfig
from scenario_pipeliner.core.core_migrate import apply_core_migrations_async
from scenario_pipeliner.core.plugin_migrate import apply_plugin_migrations_async
from scenario_pipeliner.db.engine import (
    create_async_engine_for_schema,
    create_asyncpg_pool_for_schema,
)
from scenario_pipeliner.db.schemes_names import validate_schema_name
from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.db.tables import build_core_schema
from scenario_pipeliner.worker.plugin_registry import (
    SHARED_PLUGIN_SERVICES_KEY,
    MainPipelinePluginRegistry,
)
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
        self._worker_registry: MainPipelinePluginRegistry | None = None

    @classmethod
    def from_env(cls) -> "RuntimeBootstrap":
        return cls(
            env=RuntimeEnvSettings.from_env(),
            postgres=PostgresRuntimeSettings.from_env(),
        )

    def db_schema(self) -> str:
        return validate_schema_name(self.env.db_schema)

    def async_url(self) -> str:
        return self.postgres.async_url

    def create_engine(self) -> AsyncEngine:
        return create_async_engine_for_schema(
            self.async_url(),
            self.db_schema(),
            **self.postgres.pool_kwargs,
        )

    def core_db(self, engine: AsyncEngine) -> CoreDb:
        return CoreDb(engine=engine, core=build_core_schema(self.db_schema()))

    async def create_pool(self) -> asyncpg.Pool[Any]:
        return await create_asyncpg_pool_for_schema(
            self.postgres.sync_url,
            self.db_schema(),
            **self.postgres.pool_kwargs,
        )

    async def apply_core_migrations(self) -> None:
        await apply_core_migrations_async(
            CoreMigrationConfig(
                db_backend=DbBackend.POSTGRESQL,
                db_schema=self.db_schema(),
                postgres_url=self.postgres.POSTGRES_URL,
                postgres_host=self.postgres.POSTGRES_HOST or None,
                postgres_port=self.postgres.POSTGRES_PORT,
                postgres_db=self.postgres.POSTGRES_DB or None,
                postgres_user=self.postgres.POSTGRES_USER or None,
                postgres_password=self.postgres.POSTGRES_PASSWORD or None,
            )
        )

    async def apply_plugin_migrations(self, engine: AsyncEngine) -> list[str]:
        return await apply_plugin_migrations_async(self.env.scenario_config(), engine)

    @staticmethod
    def plugin_services(pool: asyncpg.Pool[Any]) -> dict[str, Any]:
        """Host DI for plugins.

        Shared runtime resources live under ``SHARED_PLUGIN_SERVICES_KEY``.
        Plugin-specific options can still be supplied under ``services[plugin_name]``.
        """
        return {SHARED_PLUGIN_SERVICES_KEY: {"postgres_pool": pool}}

    def worker_registry(
        self, plugin_services: dict[str, Any]
    ) -> MainPipelinePluginRegistry:
        if self._worker_registry is None:
            self._worker_registry = build_worker_registry_from_manifests(
                self.env.scenario_config(),
                plugin_services=plugin_services,
            )
        return self._worker_registry

    def list_scenarios(self, plugin_services: dict[str, Any]) -> list[str]:
        registry = self.worker_registry(plugin_services)
        if not registry.scenarios:
            raise RuntimeError(
                f"no registered scenarios in {self.env.plugins_root.as_posix()}"
            )
        return list(registry.scenarios)
