from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest

from scenario_pipeliner.worker.core.custom_clients.httpxapi import AsyncHTTPxAPIClient
from scenario_pipeliner.worker.core.custom_clients.postgresql import (
    AsyncPostgreSQLClient,
)
from scenario_pipeliner.worker.core.custom_clients.rabbitmq import AsyncRabbitMQClient
from scenario_pipeliner.worker.core.custom_clients.sqlite import AsyncSQLiteClient
from scenario_pipeliner.worker.core.custom_settings import (
    APIClientSettings,
    PostgreSQLClientSettings,
    RabbitMQSettings,
)
from scenario_pipeliner.worker.core.custom_settings.settings import SQLiteClientSettings
from scenario_pipeliner.worker.core.exceptions import DatabaseError


def test_postgres_receive_logs_query_length_not_sql(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = AsyncPostgreSQLClient.__new__(AsyncPostgreSQLClient)
    client.initialized = True
    client.settings = PostgreSQLClientSettings(
        POSTGRES_HOST="h",
        POSTGRES_DB="d",
        POSTGRES_USER="u",
        POSTGRES_PASSWORD="p",
    )
    client.pool = object()  # type: ignore[assignment]

    class _Conn:
        async def fetch(self, query: str, *parameters):  # noqa: ANN002
            raise RuntimeError("boom secret=should-not-leak")

    class _Acquire:
        async def __aenter__(self) -> _Conn:
            return _Conn()

        async def __aexit__(self, *args: object) -> None:
            return None

    class _Pool:
        def acquire(self, timeout: int | None = None) -> _Acquire:
            return _Acquire()

    client.pool = _Pool()  # type: ignore[assignment]

    async def _connect() -> None:
        return None

    client.check_connection = _connect  # type: ignore[method-assign]
    query = "SELECT * FROM secrets WHERE token = 'super-secret-literal'"
    with caplog.at_level(logging.ERROR), pytest.raises(DatabaseError):
        asyncio.run(client.receive(query))
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "query_len=" in text
    assert str(len(query)) in text
    assert "super-secret-literal" not in text


def test_postgres_ping_runs_select_one() -> None:
    client = AsyncPostgreSQLClient.__new__(AsyncPostgreSQLClient)
    client.initialized = True
    client.settings = PostgreSQLClientSettings(
        POSTGRES_HOST="h",
        POSTGRES_DB="d",
        POSTGRES_USER="u",
        POSTGRES_PASSWORD="p",
        DB_COMMAND_TIMEOUT_SECONDS=1,
    )

    class _Conn:
        def __init__(self) -> None:
            self.queries: list[str] = []

        async def fetchval(self, query: str) -> int:
            self.queries.append(query)
            return 1

    class _Acquire:
        def __init__(self, conn: _Conn) -> None:
            self._conn = conn

        async def __aenter__(self) -> _Conn:
            return self._conn

        async def __aexit__(self, *args: object) -> None:
            return None

    conn = _Conn()

    class _Pool:
        def acquire(self, timeout: int | None = None) -> _Acquire:
            assert timeout == 1
            return _Acquire(conn)

    client.pool = _Pool()  # type: ignore[assignment]
    asyncio.run(client.ping())
    assert conn.queries == ["SELECT 1"]


def test_http_ping_checks_session_without_a_request() -> None:
    client = AsyncHTTPxAPIClient.__new__(AsyncHTTPxAPIClient)
    client.initialized = True
    client.settings = APIClientSettings(
        API_BASE_URL="http://example.test", API_TIMEOUT=1
    )

    class _Http:
        def __init__(self, *, closed: bool) -> None:
            self.is_closed = closed

        async def request(self, *args: object, **kwargs: object) -> None:
            raise AssertionError("ping must not call the network")

    client._http = _Http(closed=False)  # type: ignore[assignment]
    asyncio.run(client.ping())

    client._http = _Http(closed=True)  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="HTTP client is closed"):
        asyncio.run(client.ping())


def test_sqlite_ping_runs_select_one(tmp_path: Path) -> None:
    settings = SQLiteClientSettings(DB_PATH=str(tmp_path / "ping.db"))
    assert settings.SQLITE_PING_TIMEOUT_SECONDS == 5
    client = AsyncSQLiteClient(settings)

    async def _run() -> None:
        await client.connect()
        try:
            await client.ping()
        finally:
            await client.disconnect()

    asyncio.run(_run())


def test_rabbit_ping_passive_declares_cached_queues() -> None:
    client = AsyncRabbitMQClient.__new__(AsyncRabbitMQClient)
    client.initialized = True
    client.settings = RabbitMQSettings(RABBITMQ_PING_TIMEOUT_SECONDS=1)
    client.queues = {}

    class _Connection:
        is_closed = False

    class _Channel:
        def __init__(self) -> None:
            self.is_closed = False
            self.queues: list[str] = []
            self.exchanges: list[str] = []

        async def declare_exchange(
            self,
            name: str,
            *,
            passive: bool = False,
            timeout: int | None = None,
        ) -> None:
            assert passive is True
            assert timeout == 1
            self.exchanges.append(name)

        async def declare_queue(
            self,
            name: str,
            *,
            passive: bool = False,
            timeout: int | None = None,
        ) -> None:
            assert passive is True
            assert timeout == 1
            self.queues.append(name)

    channel = _Channel()
    client.connection = _Connection()  # type: ignore[assignment]
    client.channel = channel  # type: ignore[assignment]

    asyncio.run(client.ping())
    assert channel.exchanges == ["amq.direct"]

    client.queues = {"in.q": object()}  # type: ignore[dict-item]
    asyncio.run(client.ping())
    assert channel.queues == ["in.q"]

    channel.is_closed = True
    with pytest.raises(RuntimeError, match="channel is closed"):
        asyncio.run(client.ping())


def test_rabbit_drop_forgets_handles() -> None:
    client = AsyncRabbitMQClient()
    client.initialized = True
    client.connection = object()  # type: ignore[assignment]
    client.channel = object()  # type: ignore[assignment]
    client.queues["q"] = object()  # type: ignore[assignment]
    client.drop()
    assert client.connection is None
    assert client.channel is None
    assert client.queues == {}
    assert client.initialized is False
