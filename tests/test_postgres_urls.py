from __future__ import annotations

import pytest
from pydantic import ValidationError

from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.api.settings import CoreMigrationConfig
from scenario_pipeliner.db.engine import (
    create_async_engine_for_schema,
    resolve_postgres_urls,
    try_parse_postgres_url,
)
from scenario_pipeliner.db.exceptions import InvalidSchemaNameError
from scenario_pipeliner.db.settings import (
    PostgresPoolSettings,
    check_pool_bounds,
    sqlalchemy_pool_params,
)
from scenario_pipeliner.worker.core.custom_settings import PostgreSQLClientSettings


def test_try_parse_accepts_sync_and_async_urls() -> None:
    assert try_parse_postgres_url("postgresql://u:p@h:5432/db") is not None
    assert try_parse_postgres_url("postgres://u:p@h/db") is not None
    assert try_parse_postgres_url("postgresql+asyncpg://u:p@h/db") is not None
    assert try_parse_postgres_url("postgresql+psycopg://u:p@h/db") is not None


@pytest.mark.parametrize(
    "url",
    [
        None,
        "",
        "   ",
        "not-a-url",
        "mysql://u:p@h/db",
        "postgresql://u:p@h",
        "postgresql:///dbname",
    ],
)
def test_try_parse_rejects_invalid(url: str | None) -> None:
    assert try_parse_postgres_url(url) is None


def test_resolve_rewrites_drivers_and_keeps_query() -> None:
    urls = resolve_postgres_urls(
        url="postgresql+psycopg://u:p@h:5432/db?sslmode=require"
    )
    assert urls.async_url.startswith("postgresql+asyncpg://")
    assert urls.sync_url.startswith("postgresql+psycopg://")
    assert urls.dsn.startswith("postgresql://")
    assert "sslmode=require" in urls.async_url
    assert "sslmode=require" in urls.sync_url
    assert "sslmode=require" in urls.dsn
    assert "u:p@h:5432/db" in urls.dsn


def test_resolve_valid_url_ignores_discrete_parts() -> None:
    urls = resolve_postgres_urls(
        url="postgresql://urluser:urlpass@urlhost:5999/urldb",
        host="other",
        port=5432,
        database="otherdb",
        user="otheruser",
        password="otherpass",
    )
    assert "urluser:urlpass@urlhost:5999/urldb" in urls.dsn
    assert "other" not in urls.dsn


def test_resolve_invalid_url_falls_back_to_parts() -> None:
    urls = resolve_postgres_urls(
        url="not-valid",
        host="h",
        port=5433,
        database="d",
        user="u",
        password="p",
    )
    assert "u:p@h:5433/d" in urls.dsn
    assert urls.async_url.startswith("postgresql+asyncpg://")
    assert urls.sync_url.startswith("postgresql+psycopg://")


def test_resolve_missing_url_uses_parts() -> None:
    urls = resolve_postgres_urls(
        host="h",
        port=5432,
        database="d",
        user="u",
        password="p",
    )
    assert "u:p@h:5432/d" in urls.dsn


def test_resolve_raises_when_neither_url_nor_parts() -> None:
    with pytest.raises(ValueError, match="POSTGRES_URL is missing or invalid"):
        resolve_postgres_urls(url="mysql://u:p@h/db")


def test_core_migration_config_accepts_url_only() -> None:
    config = CoreMigrationConfig(
        db_backend=DbBackend.POSTGRESQL,
        postgres_url="postgresql+asyncpg://u:p@h:5432/db",
    )
    assert config.postgres_url is not None
    assert config.postgres_host is None


def test_core_migration_config_invalid_url_requires_parts() -> None:
    with pytest.raises(ValidationError, match="POSTGRES_URL is missing or invalid"):
        CoreMigrationConfig(
            db_backend=DbBackend.POSTGRESQL,
            postgres_url="not-a-url",
        )


def test_core_migration_config_invalid_url_uses_parts() -> None:
    config = CoreMigrationConfig(
        db_backend=DbBackend.POSTGRESQL,
        postgres_url="not-a-url",
        postgres_host="localhost",
        postgres_db="db",
        postgres_user="user",
        postgres_password="password",
    )
    assert config.postgres_host == "localhost"


def test_postgresql_client_settings_url_and_fallback() -> None:
    from_url = PostgreSQLClientSettings(
        POSTGRES_URL="postgresql://u:p@h:5432/db",
        POSTGRES_HOST="ignored",
        POSTGRES_DB="ignored",
        POSTGRES_USER="ignored",
        POSTGRES_PASSWORD="ignored",
    )
    assert "u:p@h:5432/db" in from_url.sync_url
    assert from_url.async_url.startswith("postgresql+asyncpg://")

    fallback = PostgreSQLClientSettings(
        POSTGRES_URL="http://example.invalid/db",
        POSTGRES_HOST="h",
        POSTGRES_PORT=5433,
        POSTGRES_DB="d",
        POSTGRES_USER="u",
        POSTGRES_PASSWORD="p",
    )
    assert "u:p@h:5433/d" in fallback.sync_url
    assert fallback.SCENARIO_PIPELINER_DB_SCHEMA == "sp"

    with pytest.raises((ValidationError, InvalidSchemaNameError)):
        PostgreSQLClientSettings(
            POSTGRES_HOST="h",
            POSTGRES_DB="d",
            POSTGRES_USER="u",
            POSTGRES_PASSWORD="p",
            SCENARIO_PIPELINER_DB_SCHEMA="public",
        )


def test_postgres_pool_settings_shared_and_mapped_to_sqlalchemy() -> None:
    settings = PostgresPoolSettings(DB_POOL_MIN_SIZE=2, DB_POOL_MAX_SIZE=5)
    assert settings.pool_kwargs == {"min_size": 2, "max_size": 5}

    with pytest.raises(ValidationError):
        PostgresPoolSettings(DB_POOL_MIN_SIZE=5, DB_POOL_MAX_SIZE=2)
    with pytest.raises(ValueError, match="must be <="):
        check_pool_bounds(5, 2)
    assert sqlalchemy_pool_params(2, 5) == (2, 3)

    client = PostgreSQLClientSettings(
        POSTGRES_HOST="h",
        POSTGRES_DB="d",
        POSTGRES_USER="u",
        POSTGRES_PASSWORD="p",
        DB_POOL_MIN_SIZE=2,
        DB_POOL_MAX_SIZE=6,
    )
    assert client.pool_kwargs == {"min_size": 2, "max_size": 6}

    engine = create_async_engine_for_schema(
        "postgresql+asyncpg://u:p@localhost/db",
        "sp",
        min_size=settings.DB_POOL_MIN_SIZE,
        max_size=settings.DB_POOL_MAX_SIZE,
    )
    try:
        assert engine.sync_engine.pool is not None
    finally:
        engine.sync_engine.dispose()
