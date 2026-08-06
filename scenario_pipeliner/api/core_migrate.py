from __future__ import annotations

import asyncio
from pathlib import Path
from warnings import deprecated

from scenario_pipeliner.api.config import CoreMigrationConfig
from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.api.models import CoreMigrationReport
from scenario_pipeliner.core.core_migrate import apply_core_migrations_async


def apply_core_migrations(
    config: CoreMigrationConfig | None = None,
    *,
    db_backend: DbBackend | None = None,
    postgres_host: str | None = None,
    postgres_port: int = 5432,
    postgres_db: str | None = None,
    postgres_user: str | None = None,
    postgres_password: str | None = None,
    sqlite_path: str | None = None,
) -> CoreMigrationReport:
    if config is None:
        if db_backend is None:
            raise ValueError("db_backend is required when config is not provided")
        config = CoreMigrationConfig(
            db_backend=db_backend,
            postgres_host=postgres_host,
            postgres_port=postgres_port,
            postgres_db=postgres_db,
            postgres_user=postgres_user,
            postgres_password=postgres_password,
            sqlite_path=Path(sqlite_path) if sqlite_path is not None else None,
        )
    return asyncio.run(apply_core_migrations_async(config))


# fixme not used
@deprecated("Use apply_core_migrations instead")
def apply_core_migrations_from_params(
    *,
    db_backend: DbBackend,
    postgres_host: str | None = None,
    postgres_port: int = 5432,
    postgres_db: str | None = None,
    postgres_user: str | None = None,
    postgres_password: str | None = None,
    sqlite_path: str | None = None,
) -> CoreMigrationReport:
    return apply_core_migrations(
        db_backend=db_backend,
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_db=postgres_db,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        sqlite_path=sqlite_path,
    )
