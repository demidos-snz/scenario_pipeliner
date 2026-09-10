from __future__ import annotations

import asyncio

from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.api.settings import CoreMigrationConfig, CoreMigrationReport
from scenario_pipeliner.core.core_migrate import apply_core_migrations_async
from scenario_pipeliner.db.schemes_names import DEFAULT_DB_SCHEMA


def apply_core_migrations(
    config: CoreMigrationConfig | None = None,
    *,
    db_backend: DbBackend | None = None,
    postgres_host: str | None = None,
    postgres_port: int = 5432,
    postgres_db: str | None = None,
    postgres_user: str | None = None,
    postgres_password: str | None = None,
    db_schema: str | None = None,
) -> CoreMigrationReport:
    """Apply core Alembic migrations (sync wrapper for CLI).

    Cannot be called from a running event loop. Embedders should
    ``await apply_core_migrations_async(config)`` instead.
    """
    if config is None:
        config = CoreMigrationConfig(
            db_backend=db_backend or DbBackend.POSTGRESQL,
            db_schema=db_schema or DEFAULT_DB_SCHEMA,
            postgres_host=postgres_host,
            postgres_port=postgres_port,
            postgres_db=postgres_db,
            postgres_user=postgres_user,
            postgres_password=postgres_password,
        )
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(apply_core_migrations_async(config))
    raise RuntimeError(
        "apply_core_migrations() cannot be used from a running event loop; "
        "await apply_core_migrations_async(config) instead"
    )
