from __future__ import annotations

import asyncio
import logging

import pytest

from scenario_pipeliner.worker.core.custom_clients.postgresql import (
    AsyncPostgreSQLClient,
)
from scenario_pipeliner.worker.core.exceptions import DatabaseError


def test_postgres_receive_logs_query_length_not_sql(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = AsyncPostgreSQLClient.__new__(AsyncPostgreSQLClient)
    client.initialized = True
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
        def acquire(self) -> _Acquire:
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
