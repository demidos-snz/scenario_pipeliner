from __future__ import annotations

from typing import Self, TypedDict

from pydantic import Field, model_validator
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.asyncio import AsyncEngine

from scenario_pipeliner.base_settings import FrozenSettings, Settings

DEFAULT_DB_POOL_MIN_SIZE = 1
DEFAULT_DB_POOL_MAX_SIZE = 5


def check_pool_bounds(min_size: int, max_size: int) -> None:
    if min_size > max_size:
        raise ValueError(
            f"DB_POOL_MIN_SIZE ({min_size}) must be <= DB_POOL_MAX_SIZE ({max_size})"
        )


def sqlalchemy_pool_params(min_size: int, max_size: int) -> tuple[int, int]:
    """Return ``(pool_size, max_overflow)`` so SQLAlchemy caps at ``max_size``."""
    check_pool_bounds(min_size, max_size)
    return min_size, max_size - min_size


class PostgresPoolKwargs(TypedDict):
    min_size: int
    max_size: int


class PostgresPoolSettings(Settings):
    """Connection-pool bounds shared by SQLAlchemy and asyncpg."""

    DB_POOL_MIN_SIZE: int = Field(default=DEFAULT_DB_POOL_MIN_SIZE, ge=1, le=10)
    DB_POOL_MAX_SIZE: int = Field(default=DEFAULT_DB_POOL_MAX_SIZE, ge=1, le=10)

    @model_validator(mode="after")
    def validate_pool_bounds(self) -> Self:
        check_pool_bounds(self.DB_POOL_MIN_SIZE, self.DB_POOL_MAX_SIZE)
        return self

    @property
    def pool_kwargs(self) -> PostgresPoolKwargs:
        return {
            "min_size": self.DB_POOL_MIN_SIZE,
            "max_size": self.DB_POOL_MAX_SIZE,
        }


class PostgresUrls(FrozenSettings):
    """SQLAlchemy async/sync URLs plus a libpq DSN for asyncpg."""

    async_url: str
    sync_url: str
    dsn: str


class CoreSchema(FrozenSettings):
    """SQLAlchemy Core tables for one library schema."""

    schema_name: str
    metadata: MetaData
    tasks: Table
    settings: Table
    results: Table
    broker_ingress: Table
    broker_outbox: Table
    plugin_migrations: Table
    task_status: ENUM
    task_source: ENUM
    task_type: ENUM
    outbox_status: ENUM

    @property
    def enums(self) -> tuple[ENUM, ...]:
        return (
            self.task_status,
            self.task_source,
            self.task_type,
            self.outbox_status,
        )

    @property
    def tables(self) -> tuple[Table, ...]:
        return tuple(self.metadata.sorted_tables)

    @property
    def identity_tables(self) -> tuple[Table, ...]:
        return self.tasks, self.results, self.broker_ingress, self.broker_outbox


class CoreDb(FrozenSettings):
    """Library SQLAlchemy engine bound to one core schema."""

    engine: AsyncEngine
    core: CoreSchema
