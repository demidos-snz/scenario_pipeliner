"""Schema identity keys in ``{core_schema}.settings`` and persist helpers."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from scenario_pipeliner.db.exceptions import (
    CoreSchemaMissingError,
    PluginMigrationConflictError,
    SchemaMappingMismatchError,
)
from scenario_pipeliner.db.schemes_names import validate_schema_name
from scenario_pipeliner.db.settings import CoreDb, CoreSchema

logger = logging.getLogger(__name__)

DB_SCHEMA_KEY = "db_schema"
PLUGIN_SCHEMA_KEY_PREFIX = "plugin_schema_"


def plugin_schema_settings_key(plugin_name: str) -> str:
    return f"{PLUGIN_SCHEMA_KEY_PREFIX}{plugin_name}"


async def require_core_tables(db: CoreDb) -> None:
    """Fail if ``{schema}.settings`` is missing (typical with ``--skip-migrations``)."""
    schema = db.core.schema_name
    async with db.engine.connect() as conn:
        exists = (
            await conn.execute(
                text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = :schema AND table_name = 'settings'"
                ),
                {"schema": schema},
            )
        ).scalar_one_or_none()
    if exists is not None:
        return
    raise CoreSchemaMissingError(
        f"relation {schema}.settings does not exist. "
        "Run `scenario_pipeliner db migrate-core` once for this database "
        f"(SCENARIO_PIPELINER_DB_SCHEMA={schema}). "
        "`run --skip-migrations` does not create the library schema; "
        "tables in public from older installs are not used."
    )


async def sync_core_identity_sequences(engine: AsyncEngine, core: CoreSchema) -> None:
    """Advance SERIAL/IDENTITY sequences past ``MAX(id)`` for core tables.

    ``COPY`` / ``INSERT`` with explicit ids (typical after moving rows into a
    new schema) leave sequences at 1, so the next ``INSERT`` without ``id``
    collides on ``tasks_pkey``.
    """
    schema = validate_schema_name(core.schema_name)
    async with engine.begin() as conn:
        for table in core.identity_tables:
            table_name = validate_schema_name(table.name)
            qualified = f"{schema}.{table_name}"
            await conn.execute(
                text(
                    f"""
                    SELECT CASE
                        WHEN seq IS NULL THEN NULL
                        WHEN max_id IS NULL THEN setval(seq::regclass, 1, false)
                        ELSE setval(seq::regclass, max_id, true)
                    END
                    FROM (
                        SELECT
                            pg_get_serial_sequence(:regclass, 'id') AS seq,
                            (SELECT MAX(id) FROM {qualified}) AS max_id
                    ) s
                    """
                ),
                {"regclass": qualified},
            )


def is_identity_pkey_violation(exc: BaseException) -> bool:
    """True when PostgreSQL rejected an INSERT because a SERIAL/IDENTITY id exists."""
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        constraint = getattr(current, "constraint_name", None)
        if constraint is None:
            diag = getattr(current, "diag", None)
            constraint = getattr(diag, "constraint_name", None)
        if isinstance(constraint, str) and constraint.endswith("_pkey"):
            return True
        blob = str(current)
        if "duplicate key value violates unique constraint" in blob and "_pkey" in blob:
            return True
        current = current.__cause__ or getattr(current, "orig", None)
    return False


async def run_with_identity_sequence_retry[T](
    db: CoreDb,
    operation: Callable[[], Awaitable[T]],
) -> T:
    """Run ``operation``; on identity pkey collision, ``setval`` and retry once.

    Startup ``setval`` covers a stale sequence before the first INSERT.
    A collision during the process lifetime (COPY with explicit ids while
    the worker is up, or a worker that started before sequences were healed)
    still needs this retry: the aborted transaction cannot reuse the same
    connection state, and ``nextval`` is not rolled back.
    """
    try:
        return await operation()
    except IntegrityError as exc:
        if not is_identity_pkey_violation(exc):
            raise
        logger.warning(
            "identity sequence collided on insert; syncing sequences and retrying once"
        )
        await sync_core_identity_sequences(db.engine, db.core)
        return await operation()


async def persist_core_schema(conn: AsyncConnection, core: CoreSchema) -> None:
    existing = await _get_setting(conn, core, DB_SCHEMA_KEY)
    if existing is None:
        await _upsert_setting(conn, core, DB_SCHEMA_KEY, core.schema_name)
        return
    if existing != core.schema_name:
        raise SchemaMappingMismatchError(
            f"settings.{DB_SCHEMA_KEY}={existing!r} does not match "
            f"configured schema {core.schema_name!r}"
        )


async def persist_plugin_schema(
    conn: AsyncConnection,
    core: CoreSchema,
    *,
    plugin_name: str,
    plugin_schema: str,
) -> None:
    key = plugin_schema_settings_key(plugin_name)
    existing = await _get_setting(conn, core, key)
    if existing is None:
        await _upsert_setting(conn, core, key, plugin_schema)
        return
    if existing != plugin_schema:
        raise SchemaMappingMismatchError(
            f"settings.{key}={existing!r} does not match "
            f"plugin schema {plugin_schema!r}"
        )


async def get_plugin_migration(
    conn: AsyncConnection,
    core: CoreSchema,
    plugin_name: str,
) -> tuple[str, str] | None:
    """Return ``(migration_order, sql_sha256)`` or ``None``."""
    row = (
        (
            await conn.execute(
                select(
                    core.plugin_migrations.c.migration_order,
                    core.plugin_migrations.c.sql_sha256,
                ).where(core.plugin_migrations.c.plugin_name == plugin_name)
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return str(row["migration_order"]), str(row["sql_sha256"])


def check_plugin_migration_replay(
    *,
    plugin_name: str,
    migration_order: str,
    sql_sha256: str,
    existing: tuple[str, str] | None,
) -> bool:
    """Return True when this file was already applied (skip). Raise on conflict."""
    if existing is None:
        return False
    applied_order, applied_sha = existing
    if applied_order > migration_order:
        raise PluginMigrationConflictError(
            f"plugin {plugin_name!r} migration_order {migration_order!r} is older "
            f"than already applied {applied_order!r}"
        )
    if applied_sha == sql_sha256:
        return True
    if applied_order == migration_order:
        raise PluginMigrationConflictError(
            f"plugin {plugin_name!r} SQL changed without bumping "
            f"migrations.migration_order ({migration_order!r}). "
            "Bump migration_order after changing the SQL file."
        )
    return False


async def record_plugin_migration(
    conn: AsyncConnection,
    core: CoreSchema,
    *,
    plugin_name: str,
    migration_order: str,
    sql_sha256: str,
) -> None:
    table = core.plugin_migrations
    stmt = (
        pg_insert(table)
        .values(
            plugin_name=plugin_name,
            migration_order=migration_order,
            sql_sha256=sql_sha256,
        )
        .on_conflict_do_update(
            index_elements=[table.c.plugin_name],
            set_={
                "migration_order": migration_order,
                "sql_sha256": sql_sha256,
                "applied_at": func.now(),
            },
        )
    )
    await conn.execute(stmt)


async def persist_core_schema_on_engine(engine: AsyncEngine, core: CoreSchema) -> None:
    async with engine.begin() as conn:
        await persist_core_schema(conn, core)


async def _get_setting(conn: AsyncConnection, core: CoreSchema, key: str) -> str | None:
    result = await conn.execute(
        select(core.settings.c.value).where(core.settings.c.key == key)
    )
    value = result.scalar_one_or_none()
    return str(value) if value is not None else None


async def _upsert_setting(
    conn: AsyncConnection, core: CoreSchema, key: str, value: str
) -> None:
    stmt = (
        pg_insert(core.settings)
        .values(key=key, value=value)
        .on_conflict_do_nothing(index_elements=[core.settings.c.key])
    )
    await conn.execute(stmt)
