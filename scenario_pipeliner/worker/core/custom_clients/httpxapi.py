import asyncio
import logging
from collections.abc import Coroutine
from contextlib import suppress
from typing import Any, Literal, TypeVar

import httpx

from scenario_pipeliner.worker.core.clients import AsyncAPIClient
from scenario_pipeliner.worker.core.custom_settings import APIClientSettings
from scenario_pipeliner.worker.core.exceptions import ApiError, PipelineCancelledError
from scenario_pipeliner.worker.core.utils import get_cancel_event

logger = logging.getLogger(__name__)

_HTTPMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
HTTPResponse = dict[str, Any] | list[Any] | None
TAPISettings = TypeVar("TAPISettings", bound=APIClientSettings)


class AsyncHTTPxAPIClient(AsyncAPIClient[HTTPResponse, TAPISettings]):
    """Асинхронный HTTP-клиент с авторизацией и CRUD."""

    def __init__(self, settings: TAPISettings) -> None:
        super().__init__(settings=settings)
        self._http: httpx.AsyncClient | None = None
        self._access_token: str | None = None

    async def connect(self) -> None:
        """Создать HTTP-сессию и при необходимости авторизоваться."""
        self._http = httpx.AsyncClient(
            base_url=self.settings.API_BASE_URL.rstrip("/"),
            timeout=self.settings.API_TIMEOUT,
        )

        if self.settings.API_TOKEN:
            self._access_token = self.settings.API_TOKEN
        elif self.settings.API_USERNAME and self.settings.API_PASSWORD:
            await self.authenticate()

        self.initialized = True

    async def disconnect(self) -> None:
        """Закрыть HTTP-сессию."""
        if self._http is not None:
            await self._http.aclose()
        self._http = None
        self._access_token = None
        self.initialized = False

    async def check_connection(self) -> None:
        await super().check_connection()
        if self._http is None:
            self.initialized = False
            raise RuntimeError("HTTP client is not connected")

    def _auth_headers(self) -> dict[str, str]:
        if not self._access_token:
            return {}

        header = self.settings.API_TOKEN_HEADER
        prefix = self.settings.API_TOKEN_PREFIX.strip()
        value = f"{prefix} {self._access_token}" if prefix else self._access_token
        return {header: value}

    async def authenticate(self) -> str:
        """Получить access token (OAuth2 password form или статический API_TOKEN)."""
        if self.settings.API_TOKEN:
            self._access_token = self.settings.API_TOKEN
            return self._access_token

        if not self.settings.API_USERNAME or not self.settings.API_PASSWORD:
            raise ApiError("API credentials are not configured")

        if self._http is None:
            raise RuntimeError("HTTP client is not connected")

        response = await self._http.post(
            self.settings.API_AUTH_PATH,
            data={
                "username": self.settings.API_USERNAME,
                "password": self.settings.API_PASSWORD,
            },
        )

        if response.status_code >= 400:
            raise ApiError(
                f"Authentication failed with status {response.status_code}",
                status_code=response.status_code,
                response_body=response.text,
            )

        payload = response.json()
        if not isinstance(payload, dict):
            raise ApiError("Authentication response must be a JSON object")

        token = payload.get(self.settings.API_TOKEN_RESPONSE_KEY)
        if not isinstance(token, str) or not token:
            raise ApiError(
                f"Token key {self.settings.API_TOKEN_RESPONSE_KEY!r} "
                "is missing in auth response"
            )

        self._access_token = token
        logger.info("API authentication succeeded")
        return token

    @staticmethod
    def _parse_response(response: httpx.Response) -> dict[str, Any] | list[Any] | None:
        if response.status_code >= 400:
            raise ApiError(
                f"API request failed with status {response.status_code}",
                status_code=response.status_code,
                response_body=response.text,
            )

        if response.status_code == 204 or not response.content:
            return None

        data: Any = response.json()
        if isinstance(data, (dict, list)):
            return data
        return {"data": data}

    async def _execute_http(
        self, coro: Coroutine[Any, Any, httpx.Response]
    ) -> httpx.Response:
        """Run an httpx coroutine; abort when pipeline cancel event is set."""
        cancel_event = get_cancel_event()
        if cancel_event is None:
            return await coro

        if cancel_event.is_set():
            raise PipelineCancelledError("HTTP request cancelled by pipeline token")

        http_task: asyncio.Task[httpx.Response] = asyncio.create_task(coro)
        cancel_waiter = asyncio.create_task(cancel_event.wait())
        try:
            done, pending = await asyncio.wait(
                {http_task, cancel_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

            # cancel may finish first and leave http_task already cancelled/done;
            # always map that to PipelineCancelledError (not bare CancelledError).
            if cancel_event.is_set():
                if not http_task.done():
                    http_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await http_task
                raise PipelineCancelledError("HTTP request cancelled by pipeline token")

            return http_task.result()
        finally:
            if not cancel_waiter.done():
                cancel_waiter.cancel()
                with suppress(asyncio.CancelledError):
                    await cancel_waiter

    async def _request(
        self,
        method: _HTTPMethod,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any] | list[Any] | None:
        await self.check_connection()
        assert self._http is not None

        request_headers = self._auth_headers()
        if headers:
            request_headers.update(headers)

        response = await self._execute_http(
            self._http.request(
                method,
                path,
                params=params,
                json=json,
                data=data,
                headers=request_headers or None,
            )
        )
        return self._parse_response(response)

    async def create(
        self,
        path: str,
        data: dict[str, Any] | None = None,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any] | None:
        """POST — создать ресурс."""
        return await self._request("POST", path, params=params, json=data)

    async def read(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any] | None:
        """GET — получить ресурс или список."""
        return await self._request("GET", path, params=params)

    async def update(
        self,
        path: str,
        data: dict[str, Any],
        *,
        partial: bool = True,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any] | None:
        """PUT / PATCH — обновить ресурс."""
        method: _HTTPMethod = "PATCH" if partial else "PUT"
        return await self._request(method, path, params=params, json=data)

    async def delete(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any] | None:
        """DELETE — удалить ресурс."""
        return await self._request("DELETE", path, params=params)
