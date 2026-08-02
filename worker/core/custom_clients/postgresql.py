import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from scenario_pipeliner.worker.core.clients import AsyncDBClient
from scenario_pipeliner.worker.core.custom_settings import PostgreSQLClientSettings
from scenario_pipeliner.worker.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class AsyncPostgreSQLClient(
    AsyncDBClient[list[tuple[Any, ...]], PostgreSQLClientSettings]
):
    """Асинхронный клиент для PostgreSQL."""

    def __init__(self, settings: PostgreSQLClientSettings | None = None):
        super().__init__(settings or PostgreSQLClientSettings())
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        """Подключиться к БД."""
        self.pool = await asyncpg.create_pool(
            dsn=self.settings.sync_url,
            min_size=self.settings.DB_POOL_MIN_SIZE,
            max_size=self.settings.DB_POOL_MAX_SIZE,
        )
        self.initialized = True

    async def disconnect(self) -> None:
        """Закрыть соединение с БД."""
        if self.pool:
            await self.pool.close()
        self.pool = None
        self.initialized = False

    async def check_connection(self):
        """Проверка подключения к БД."""
        await super().check_connection()
        assert self.pool is not None

    async def receive(
        self, query: str, parameters: tuple[Any, ...] | list[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Получить все результаты из БД."""
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
            logger.error("PostgreSQL receive error: %s, query: %s", e, query)
            raise DatabaseError("Database receive error") from e

    async def insert(self, query: str, param: list[Any]) -> None:
        """Вставить данные в БД."""
        await self._execute(query, param, "insert")

    async def update(self, query: str, param: list[Any]) -> None:
        """Обновить данные в БД."""
        await self._execute(query, param, "update")

    async def _execute(self, query: str, param: list[Any], operation_name: str) -> None:
        """Выполнить запрос и сохранить изменения."""
        await self.check_connection()
        assert self.pool is not None

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, *param)
        except Exception as e:
            logger.error("PostgreSQL %s error: %s, query: %s", operation_name, e, query)
            raise DatabaseError(f"Database operation '{operation_name}' failed") from e

    @asynccontextmanager
    async def transaction(self):
        """Асинхронный контекстный менеджер для управления транзакциями."""
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
