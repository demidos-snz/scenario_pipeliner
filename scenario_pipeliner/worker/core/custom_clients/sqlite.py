import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from typing import Any

import aiosqlite

from scenario_pipeliner.worker.core.clients import AsyncDBClient
from scenario_pipeliner.worker.core.custom_settings.settings import SQLiteClientSettings
from scenario_pipeliner.worker.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class AsyncSQLiteClient(AsyncDBClient[list[tuple[Any, ...]], SQLiteClientSettings]):
    """Async SQLite client."""

    def __init__(self, settings: SQLiteClientSettings | None = None):
        super().__init__(settings or SQLiteClientSettings())
        self.connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Open the database connection."""
        self.connection = await aiosqlite.connect(self.settings.DB_PATH)
        self.initialized = True

    async def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            await self.connection.close()
        self.initialized = False

    async def check_connection(self):
        """Raise if the connection is not open."""
        await super().check_connection()
        assert self.connection is not None

    async def receive(
        self, query: str, parameters: tuple[Any, ...] | list[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Fetch all rows for a parameterized query."""
        await self.check_connection()
        assert self.connection is not None

        try:
            async with self.connection.execute(query, parameters) as cursor:
                return await cursor.fetchall()  # type: ignore[return-value]
        except Exception as e:
            logger.error("SQLite receive error: %s, query: %s", e, query)
            raise DatabaseError("Database receive error") from e

    async def insert(self, query: str, param: list[Any]) -> None:
        """Execute an insert statement."""
        await self._execute(query, param, "insert")

    async def update(self, query: str, param: list[Any]) -> None:
        """Execute an update statement."""
        await self._execute(query, param, "update")

    async def _execute(self, query: str, param: list[Any], operation_name: str) -> None:
        """Execute a statement and commit.

        ``CancelledError`` is a ``BaseException``, so this catches
        ``BaseException`` to roll back an open transaction and not jam the
        single connection.
        """
        await self.check_connection()
        assert self.connection is not None

        async with self._lock:
            try:
                await self.connection.execute(query, param)
                await self.connection.commit()
            except BaseException as e:
                # Roll back any uncommitted transaction, including
                # asyncio.CancelledError on forced shutdown.
                with suppress(Exception):
                    await self.connection.rollback()
                if isinstance(e, asyncio.CancelledError):
                    logger.warning("SQLite %s cancelled, rolled back", operation_name)
                    raise
                if isinstance(e, Exception):
                    logger.error(
                        "SQLite %s error: %s, query: %s", operation_name, e, query
                    )
                    raise DatabaseError(
                        f"Database operation '{operation_name}' failed"
                    ) from e
                # KeyboardInterrupt / SystemExit — re-raise as-is.
                raise

    @asynccontextmanager
    async def transaction(self):
        """Async context manager for a SQLite transaction.

        ``CancelledError`` is a ``BaseException``: without catching it,
        rollback never ran, the transaction stayed open, and the connection
        jammed.
        """
        await self.check_connection()
        assert self.connection is not None

        async with self._lock:
            # SQLite needs an explicit BEGIN to avoid write races.
            await self.connection.execute("BEGIN IMMEDIATE")
            try:
                async with self.connection.cursor() as cursor:
                    yield cursor
                await self.connection.commit()
            except BaseException as e:
                with suppress(Exception):
                    await self.connection.rollback()
                if isinstance(e, asyncio.CancelledError):
                    logger.warning("SQLite transaction cancelled, rolled back")
                    raise
                if isinstance(e, Exception):
                    logger.error("SQLite transaction error: %s", e)
                    raise DatabaseError("Database transaction failed") from e
                raise
