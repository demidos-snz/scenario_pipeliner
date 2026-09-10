"""Shared Postgres fixtures. Missing POSTGRES_URL and POSTGRES_* is an error, never a skip."""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from scenario_pipeliner.api.core_migrate import apply_core_migrations
from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.api.settings import CoreMigrationConfig
from scenario_pipeliner.db.engine import (
    create_async_engine_for_schema,
    resolve_postgres_urls,
    try_parse_postgres_url,
)
from scenario_pipeliner.db.schemes_names import validate_schema_name
from scenario_pipeliner.db.settings import CoreDb, PostgresUrls
from scenario_pipeliner.db.tables import build_core_schema

_REQUIRED_PG_ENV = (
    "POSTGRES_HOST",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
)


def _drop_schema_cascade(sync_url: str, schema: str) -> None:
    schema_name = validate_schema_name(schema)
    engine = create_engine(sync_url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
    finally:
        engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def postgres_params() -> dict[str, Any]:
    load_dotenv(find_dotenv(usecwd=True), override=False)
    parsed = try_parse_postgres_url(os.getenv("POSTGRES_URL"))
    if parsed is not None:
        assert parsed.host is not None
        assert parsed.database is not None
        return {
            "host": parsed.host,
            "port": int(parsed.port or 5432),
            "database": parsed.database,
            "user": parsed.username or "",
            "password": parsed.password or "",
        }
    missing = [name for name in _REQUIRED_PG_ENV if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Postgres tests require POSTGRES_URL or environment variables: "
            + ", ".join(missing)
        )
    return {
        "host": os.environ["POSTGRES_HOST"],
        "port": int(os.getenv("POSTGRES_PORT") or "5432"),
        "database": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }


@pytest.fixture(scope="session")
def postgres_urls(postgres_params: dict[str, Any]) -> PostgresUrls:
    return resolve_postgres_urls(
        host=str(postgres_params["host"]),
        port=int(postgres_params["port"]),
        database=str(postgres_params["database"]),
        user=str(postgres_params["user"]),
        password=str(postgres_params["password"]),
    )


@pytest.fixture
def isolated_schema(postgres_urls: PostgresUrls) -> Iterator[str]:
    schema = validate_schema_name(f"sp_test_{uuid.uuid4().hex[:12]}")
    try:
        yield schema
    finally:
        _drop_schema_cascade(postgres_urls.sync_url, schema)


@pytest.fixture
def schema_janitor(postgres_urls: PostgresUrls) -> Iterator[list[str]]:
    """Collect extra schemas (plugin) to drop after the test."""
    created: list[str] = []
    try:
        yield created
    finally:
        for name in created:
            _drop_schema_cascade(postgres_urls.sync_url, name)


@pytest.fixture
def core_db(
    postgres_params: dict[str, Any],
    postgres_urls: PostgresUrls,
    isolated_schema: str,
) -> Iterator[CoreDb]:
    apply_core_migrations(
        CoreMigrationConfig(
            db_backend=DbBackend.POSTGRESQL,
            db_schema=isolated_schema,
            postgres_host=str(postgres_params["host"]),
            postgres_port=int(postgres_params["port"]),
            postgres_db=str(postgres_params["database"]),
            postgres_user=str(postgres_params["user"]),
            postgres_password=str(postgres_params["password"]),
        )
    )
    engine = create_async_engine_for_schema(
        postgres_urls.async_url,
        isolated_schema,
        poolclass=NullPool,
    )
    db = CoreDb(engine=engine, core=build_core_schema(isolated_schema))
    try:
        yield db
    finally:
        asyncio.run(engine.dispose())
