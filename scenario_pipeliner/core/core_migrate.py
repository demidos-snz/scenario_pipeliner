from __future__ import annotations

import asyncio
from importlib.resources import files

from alembic import command
from alembic.config import Config

from scenario_pipeliner.api.settings import CoreMigrationConfig, CoreMigrationReport
from scenario_pipeliner.db.engine import (
    create_async_engine_for_schema,
    create_schema_if_not_exists,
    resolve_postgres_urls,
)
from scenario_pipeliner.db.schema_utils import (
    DB_SCHEMA_KEY,
    persist_core_schema_on_engine,
    sync_core_identity_sequences,
)
from scenario_pipeliner.db.schemes_names import validate_schema_name
from scenario_pipeliner.db.tables import build_core_schema

_CORE_TABLES = (
    "tasks",
    "settings",
    "results",
    "broker_ingress",
    "broker_outbox",
    "plugin_migrations",
)
_DEFAULT_SETTINGS: dict[str, str] = {
    "worker_enabled": "1",
}


async def apply_core_migrations_async(
    config: CoreMigrationConfig,
) -> CoreMigrationReport:
    schema = validate_schema_name(config.db_schema)
    urls = resolve_postgres_urls(
        url=config.postgres_url,
        host=config.postgres_host,
        port=config.postgres_port,
        database=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    create_schema_if_not_exists(urls.sync_url, schema)
    await asyncio.to_thread(_run_alembic_upgrade, urls.async_url, schema)

    engine = create_async_engine_for_schema(urls.async_url, schema)
    try:
        core = build_core_schema(schema)
        await persist_core_schema_on_engine(engine, core)
        await sync_core_identity_sequences(engine, core)
    finally:
        await engine.dispose()

    defaults = dict(_DEFAULT_SETTINGS)
    defaults[DB_SCHEMA_KEY] = schema
    return CoreMigrationReport(
        db_backend=config.db_backend,
        db_schema=schema,
        tables=list(_CORE_TABLES),
        default_settings=defaults,
    )


def _run_alembic_upgrade(db_url: str, schema: str) -> None:
    config = Config()
    config.set_main_option(
        "script_location", str(files("scenario_pipeliner.core_migrations"))
    )
    config.set_main_option("sqlalchemy.url", db_url.replace("%", "%%"))
    config.attributes["configure_logger"] = False
    config.attributes["db_schema"] = schema
    command.upgrade(config, "head")
