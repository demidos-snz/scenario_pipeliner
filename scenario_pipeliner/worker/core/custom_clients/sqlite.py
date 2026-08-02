import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from typing import Any

import aiosqlite

from scenario_pipeliner.worker.core.clients import AsyncDBClient
from scenario_pipeliner.worker.core.custom_settings import SQLiteClientSettings
from scenario_pipeliner.worker.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class AsyncSQLiteClient(AsyncDBClient[list[tuple[Any, ...]], SQLiteClientSettings]):
    """Асинхронный клиент для SQLite."""

    def __init__(self, settings: SQLiteClientSettings | None = None):
        super().__init__(settings or SQLiteClientSettings())
        self.connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Подключиться к БД."""
        self.connection = await aiosqlite.connect(self.settings.DB_PATH)
        self.initialized = True

    async def disconnect(self) -> None:
        """Закрыть соединение с БД."""
        if self.connection:
            await self.connection.close()
        self.initialized = False

    async def check_connection(self):
        """Проверка подключения к БД."""
        await super().check_connection()
        assert self.connection is not None

    async def receive(
        self, query: str, parameters: tuple[Any, ...] | list[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Получить все результаты из БД."""
        await self.check_connection()
        assert self.connection is not None

        try:
            async with self.connection.execute(query, parameters) as cursor:
                return await cursor.fetchall()  # type: ignore[return-value]
        except Exception as e:
            logger.error("SQLite receive error: %s, query: %s", e, query)
            raise DatabaseError("Database receive error") from e

    async def insert(self, query: str, param: list[Any]) -> None:
        """Вставить данные в БД."""
        await self._execute(query, param, "insert")

    async def update(self, query: str, param: list[Any]) -> None:
        """Обновить данные в БД."""
        await self._execute(query, param, "update")

    async def _execute(self, query: str, param: list[Any], operation_name: str) -> None:
        """Выполнить запрос и сохранить изменения.

        ``CancelledError`` — ``BaseException``, поэтому ловим ``BaseException``,
        чтобы откатить открытую транзакцию и не заклинить единственный коннект.
        """
        await self.check_connection()
        assert self.connection is not None

        async with self._lock:
            try:
                await self.connection.execute(query, param)
                await self.connection.commit()
            except BaseException as e:
                # Гарантируем откат любой незакоммиченной транзакции,
                # включая asyncio.CancelledError при forced shutdown.
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
                # KeyboardInterrupt / SystemExit — пробрасываем как есть.
                raise

    @asynccontextmanager
    async def transaction(self):
        """Асинхронный контекстный менеджер для управления транзакциями.

        ``CancelledError`` — ``BaseException``: без ``except BaseException`` rollback
        не выполнялся, транзакция оставалась открытой и заклинивала коннект.
        """
        await self.check_connection()
        assert self.connection is not None

        async with self._lock:
            # В SQLite нужно явно начать транзакцию для предотвращения гонок
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
