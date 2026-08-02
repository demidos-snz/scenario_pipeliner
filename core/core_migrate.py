from __future__ import annotations

import asyncio
from importlib.resources import files

from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL

from scenario_pipeliner.api.config import CoreMigrationConfig
from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.api.models import CoreMigrationReport

_CORE_TABLES = ("tasks", "settings", "results")
_DEFAULT_SETTINGS: dict[str, str] = {
    "pipeline_active_default": "1",
    "worker_enabled": "1",
}


async def apply_core_migrations_async(
    config: CoreMigrationConfig,
) -> CoreMigrationReport:
    db_url = _build_database_url(config)
    await asyncio.to_thread(_run_alembic_upgrade, db_url)

    return CoreMigrationReport(
        db_backend=config.db_backend,
        tables=list(_CORE_TABLES),
        default_settings=dict(_DEFAULT_SETTINGS),
    )


def _run_alembic_upgrade(db_url: str) -> None:
    config = Config()
    config.set_main_option(
        "script_location", str(files("scenario_pipeliner.core_migrations"))
    )
    config.set_main_option("sqlalchemy.url", db_url)
    config.attributes["configure_logger"] = False
    command.upgrade(config, "head")


def _build_database_url(config: CoreMigrationConfig) -> str:
    if config.db_backend == DbBackend.SQLITE:
        assert config.sqlite_path is not None
        config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        return URL.create(
            drivername="sqlite+aiosqlite",
            database=config.sqlite_path.as_posix(),
        ).render_as_string(hide_password=False)

    assert config.postgres_host is not None
    assert config.postgres_db is not None
    assert config.postgres_user is not None
    assert config.postgres_password is not None
    return URL.create(
        drivername="postgresql+asyncpg",
        username=config.postgres_user,
        password=config.postgres_password,
        host=config.postgres_host,
        port=config.postgres_port,
        database=config.postgres_db,
    ).render_as_string(hide_password=False)
