from __future__ import annotations

from typing import Any

import asyncpg
from psycopg.errors import InsufficientPrivilege
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import URL, Connection, Engine, make_url
from sqlalchemy.exc import ArgumentError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import Pool

from scenario_pipeliner.db.exceptions import SchemaPrivilegeError
from scenario_pipeliner.db.schemes_names import validate_schema_name
from scenario_pipeliner.db.settings import (
    DEFAULT_DB_POOL_MAX_SIZE,
    DEFAULT_DB_POOL_MIN_SIZE,
    PostgresUrls,
    check_pool_bounds,
    sqlalchemy_pool_params,
)

_POSTGRES_DIALECTS = {"postgres", "postgresql"}


def normalize_postgres_url_env(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def try_parse_postgres_url(url: str | None) -> URL | None:
    """Parse a Postgres URL. Returns None when missing or not a usable Postgres URL.

    Accepts ``postgres://``, ``postgresql://``, and SQLAlchemy drivers such as
    ``postgresql+asyncpg`` / ``postgresql+psycopg``. Does not open a connection:
    invalid means unparseable, non-Postgres, or missing host/database.
    """
    raw = normalize_postgres_url_env(url)
    if raw is None:
        return None
    try:
        parsed = make_url(raw)
    except (ArgumentError, ValueError):
        return None
    dialect = (parsed.drivername or "").split("+", 1)[0].lower()
    if dialect not in _POSTGRES_DIALECTS:
        return None
    if not parsed.host or not parsed.database:
        return None
    return parsed


def resolve_postgres_urls(
    *,
    url: str | None = None,
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> PostgresUrls:
    """Prefer ``POSTGRES_URL`` when it is a valid Postgres URL; else ``POSTGRES_*``."""
    parsed = try_parse_postgres_url(url)
    if parsed is None:
        host_s = (host or "").strip()
        database_s = (database or "").strip()
        user_s = (user or "").strip()
        if not host_s or not database_s or not user_s:
            raise ValueError(
                "POSTGRES_URL is missing or invalid; "
                "set POSTGRES_HOST, POSTGRES_DB, and POSTGRES_USER"
            )
        parsed = URL.create(
            drivername="postgresql",
            username=user_s,
            password=password if password is not None else "",
            host=host_s,
            port=int(port or 5432),
            database=database_s,
        )
    return PostgresUrls(
        async_url=_render(parsed, "postgresql+asyncpg"),
        sync_url=_render(parsed, "postgresql+psycopg"),
        dsn=_render(parsed, "postgresql"),
    )


def _render(url: URL, drivername: str) -> str:
    return url.set(drivername=drivername).render_as_string(hide_password=False)


def search_path_sql(schema: str) -> str:
    """``SET search_path`` for a validated schema identifier (unquoted)."""
    return f"SET search_path TO {validate_schema_name(schema)}"


def _attach_search_path(sync_engine: Engine, schema: str) -> None:
    statement = search_path_sql(schema)

    @event.listens_for(sync_engine, "connect")
    def _set_search_path(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(statement)
        finally:
            cursor.close()


def create_async_engine_for_schema(
    url: str,
    schema: str,
    *,
    min_size: int = DEFAULT_DB_POOL_MIN_SIZE,
    max_size: int = DEFAULT_DB_POOL_MAX_SIZE,
    poolclass: type[Pool] | None = None,
) -> AsyncEngine:
    kwargs: dict[str, Any] = {"pool_pre_ping": True}
    if poolclass is not None:
        kwargs["poolclass"] = poolclass
    else:
        kwargs["pool_size"], kwargs["max_overflow"] = sqlalchemy_pool_params(
            min_size, max_size
        )
    engine = create_async_engine(url, **kwargs)
    _attach_search_path(engine.sync_engine, schema)
    return engine


async def create_asyncpg_pool_for_schema(
    dsn: str,
    schema: str,
    *,
    min_size: int = DEFAULT_DB_POOL_MIN_SIZE,
    max_size: int = DEFAULT_DB_POOL_MAX_SIZE,
) -> asyncpg.Pool[Any]:
    """asyncpg pool with the same core-schema ``search_path`` as SQLAlchemy."""
    check_pool_bounds(min_size, max_size)
    schema_name = validate_schema_name(schema)
    statement = search_path_sql(schema_name)

    async def _init(connection: asyncpg.Connection[Any]) -> None:
        await connection.execute(statement)

    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size,
        max_size=max_size,
        init=_init,
        server_settings={"search_path": schema_name},
    )


_SCHEMA_EXISTS = text("SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = :schema")
_SESSION_IDENTITY = text("SELECT current_user AS role, current_database() AS db")


def _schema_privilege_message(schema: str, role: str, database: str) -> str:
    return (
        f"Cannot CREATE SCHEMA {schema}: role {role!r} lacks CREATE "
        f"on database {database!r} "
        "(PostgreSQL 15+ does not grant CREATE to PUBLIC by default). "
        "Ask a DBA to run either "
        f"`GRANT CREATE ON DATABASE {database} TO {role};` "
        "or "
        f"`CREATE SCHEMA {schema} AUTHORIZATION {role}; "
        f"GRANT USAGE, CREATE ON SCHEMA {schema} TO {role};` "
        "then re-run `scenario_pipeliner db migrate-core`. "
        "Plugin schemas need the same grant or a pre-created schema."
    )


def _raise_if_schema_privilege(
    exc: ProgrammingError, *, schema: str, role: str, database: str
) -> None:
    orig = getattr(exc, "orig", None)
    if isinstance(orig, InsufficientPrivilege):
        raise SchemaPrivilegeError(
            _schema_privilege_message(schema, role, database)
        ) from exc


def create_schema_if_not_exists(sync_url: str, schema: str) -> None:
    """Create ``schema`` when missing. Does not run ``CREATE SCHEMA`` if it exists.

    PostgreSQL still requires ``CREATE`` on the database for
    ``CREATE SCHEMA IF NOT EXISTS`` even when the schema is already there.
    """
    schema_name = validate_schema_name(schema)
    engine = create_engine(sync_url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            ensure_postgres_schema_sync(conn, schema_name)
    finally:
        engine.dispose()


def ensure_postgres_schema_sync(conn: Connection, schema: str) -> None:
    schema_name = validate_schema_name(schema)
    exists = conn.execute(_SCHEMA_EXISTS, {"schema": schema_name}).scalar_one_or_none()
    if exists is not None:
        return
    identity = conn.execute(_SESSION_IDENTITY).mappings().one()
    try:
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
    except ProgrammingError as exc:
        _raise_if_schema_privilege(
            exc,
            schema=schema_name,
            role=str(identity["role"]),
            database=str(identity["db"]),
        )
        raise


async def ensure_postgres_schema(conn: AsyncConnection, schema: str) -> None:
    schema_name = validate_schema_name(schema)
    exists = (
        await conn.execute(_SCHEMA_EXISTS, {"schema": schema_name})
    ).scalar_one_or_none()
    if exists is not None:
        return
    identity = (await conn.execute(_SESSION_IDENTITY)).mappings().one()
    try:
        await conn.execute(text(f"CREATE SCHEMA {schema_name}"))
    except ProgrammingError as exc:
        _raise_if_schema_privilege(
            exc,
            schema=schema_name,
            role=str(identity["role"]),
            database=str(identity["db"]),
        )
        raise
