from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any

from scenario_pipeliner.worker.core.settings import ClientSettings


class AsyncClient[T, TClientSettings: ClientSettings](ABC):
    initialized: bool = False

    def __init__(self, settings: TClientSettings):
        self.settings: TClientSettings = settings

    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        pass

    @abstractmethod
    async def receive(self, *args: Any, **kwargs: Any) -> T | None:
        pass

    async def check_connection(self) -> None:
        if not self.initialized:
            raise RuntimeError(f"{self.__class__.__name__} is not initialized")

    async def __aenter__(self) -> "AsyncClient[T, TClientSettings]":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.disconnect()


class AsyncBrokerClient[TClientSettings: ClientSettings](
    AsyncClient[str, TClientSettings], ABC
):
    """Broker connection client. Ingress/egress use named queues, not receive/send."""


class AsyncDBClient[T, TClientSettings: ClientSettings](
    AsyncClient[T, TClientSettings], ABC
):
    """Abstract async database client."""


class AsyncAPIClient[T, TClientSettings: ClientSettings](
    AsyncClient[T, TClientSettings], ABC
):
    """Abstract async HTTP client with auth and CRUD."""

    @abstractmethod
    async def authenticate(self) -> str:
        """Authenticate and return an access token."""

    @abstractmethod
    async def create(
        self,
        path: str,
        data: dict[str, Any] | None = None,
        *,
        params: dict[str, Any] | None = None,
    ) -> T:
        """POST — create a resource."""

    @abstractmethod
    async def read(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> T:
        """GET — fetch a resource or a list."""

    @abstractmethod
    async def update(
        self,
        path: str,
        data: dict[str, Any],
        *,
        partial: bool = True,
        params: dict[str, Any] | None = None,
    ) -> T:
        """PUT / PATCH — update a resource."""

    @abstractmethod
    async def delete(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> T:
        """DELETE — delete a resource."""

    async def receive(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> T:
        """Fetch data (``read`` alias for ``AsyncClient``)."""
        if kwargs:
            merged = dict(params or {})
            merged.update(kwargs)
            params = merged
        return await self.read(path, params=params)
