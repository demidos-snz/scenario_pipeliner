from abc import ABC, abstractmethod
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
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> None:
        await self.disconnect()


class AsyncBrokerClient[TClientSettings: ClientSettings](
    AsyncClient[str, TClientSettings], ABC
):
    @abstractmethod
    async def send(self, message: str, source: str = "default") -> None:
        pass

    @abstractmethod
    async def receive(
        self, source: str = "default", *args: Any, **kwargs: Any
    ) -> str | None:
        pass


class AsyncDBClient[T, TClientSettings: ClientSettings](
    AsyncClient[T, TClientSettings], ABC
):
    pass


class AsyncAPIClient[T, TClientSettings: ClientSettings](
    AsyncClient[T, TClientSettings], ABC
):
    """Абстрактный асинхронный HTTP-клиент с авторизацией и CRUD."""

    @abstractmethod
    async def authenticate(self) -> str:
        """Аутентифицироваться."""

    @abstractmethod
    async def create(
        self,
        path: str,
        data: dict[str, Any] | None = None,
        *,
        params: dict[str, Any] | None = None,
    ) -> T:
        """POST — создать ресурс."""

    @abstractmethod
    async def read(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> T:
        """GET — получить ресурс или список."""

    @abstractmethod
    async def update(
        self,
        path: str,
        data: dict[str, Any],
        *,
        partial: bool = True,
        params: dict[str, Any] | None = None,
    ) -> T:
        """PUT / PATCH — обновить ресурс."""

    @abstractmethod
    async def delete(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> T:
        """DELETE — удалить ресурс."""

    async def receive(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> T:
        """Получить данные (алиас read для AsyncClient)."""
        if kwargs:
            merged = dict(params or {})
            merged.update(kwargs)
            params = merged
        return await self.read(path, params=params)
