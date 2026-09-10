import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from scenario_pipeliner.db.engine import create_asyncpg_pool_for_schema
from scenario_pipeliner.worker.core.clients import AsyncDBClient
from scenario_pipeliner.worker.core.custom_settings import PostgreSQLClientSettings
from scenario_pipeliner.worker.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class AsyncPostgreSQLClient(
    AsyncDBClient[list[tuple[Any, ...]], PostgreSQLClientSettings]
):
    """Async PostgreSQL client (asyncpg pool with core search_path)."""

    def __init__(self, settings: PostgreSQLClientSettings | None = None):
        super().__init__(settings or PostgreSQLClientSettings())
        self.pool: asyncpg.Pool | None = None

    @staticmethod
    def _log_query_error(operation: str, error: Exception, query: str) -> None:
        logger.error(
            "PostgreSQL %s error: %s (query_len=%s)",
            operation,
            error,
            len(query),
        )

    async def connect(self) -> None:
        """Open the asyncpg pool."""
        self.pool = await create_asyncpg_pool_for_schema(
            self.settings.sync_url,
            self.settings.SCENARIO_PIPELINER_DB_SCHEMA,
            **self.settings.pool_kwargs,
        )
        self.initialized = True

    async def disconnect(self) -> None:
        """Close the pool."""
        if self.pool:
            await self.pool.close()
        self.pool = None
        self.initialized = False

    async def check_connection(self):
        """Raise if the pool is not open."""
        await super().check_connection()
        assert self.pool is not None

    async def receive(
        self, query: str, parameters: tuple[Any, ...] | list[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Fetch all rows for a parameterized query."""
        await self.check_connection()
        assert self.pool is not None

        try:
            async with self.pool.acquire() as conn:
                if parameters is not None:
                    rows = await conn.fetch(query, *parameters)
                else:
                    rows = await conn.fetch(query)
                return [tuple(row) for row in rows]
        except Exception as e:
            self._log_query_error("receive", e, query)
            raise DatabaseError("Database receive error") from e

    async def insert(self, query: str, param: list[Any]) -> None:
        """Execute an insert statement."""
        await self._execute(query, param, "insert")

    async def update(self, query: str, param: list[Any]) -> None:
        """Execute an update statement."""
        await self._execute(query, param, "update")

    async def _execute(self, query: str, param: list[Any], operation_name: str) -> None:
        """Execute a statement; do not log SQL text (may contain literals)."""
        await self.check_connection()
        assert self.pool is not None

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, *param)
        except Exception as e:
            self._log_query_error(operation_name, e, query)
            raise DatabaseError(f"Database operation '{operation_name}' failed") from e

    @asynccontextmanager
    async def transaction(self):
        """Yield a connection inside a transaction."""
        await self.check_connection()
        assert self.pool is not None

        async with self.pool.acquire() as conn:
            try:
                async with conn.transaction():
                    yield conn
            except asyncio.CancelledError:
                logger.warning("PostgreSQL transaction cancelled")
                raise
            except Exception as e:
                logger.error("PostgreSQL transaction error: %s", e)
                raise DatabaseError("Database transaction failed") from e
