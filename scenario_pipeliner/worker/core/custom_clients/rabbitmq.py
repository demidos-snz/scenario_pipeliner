from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote, urlparse, urlunparse

import aio_pika
import aio_pika.abc
from aiormq.exceptions import ChannelPreconditionFailed

from scenario_pipeliner.worker.core.clients import AsyncBrokerClient
from scenario_pipeliner.worker.core.custom_settings import RabbitMQSettings

logger = logging.getLogger(__name__)


class AsyncRabbitMQClient(AsyncBrokerClient[RabbitMQSettings]):
    """Async RabbitMQ client."""

    def __init__(self, settings: RabbitMQSettings | None = None):
        super().__init__(settings=settings or RabbitMQSettings())

        self.connection: aio_pika.abc.AbstractRobustConnection | None = None
        self.channel: aio_pika.abc.AbstractChannel | None = None
        self.queues: dict[str, aio_pika.abc.AbstractQueue] = {}
        self._prefetch_count: int | None = None

    def _connection_url(self) -> str:
        """Build AMQP URL; vhost from ``RABBITMQ_VHOST`` or URL path.

        ``aio_pika.connect*(url=...)`` reads vhost only from the URL path.
        Precedence: non-default ``RABBITMQ_VHOST`` → URL path → ``/``.
        """
        parsed = urlparse(self.settings.RABBITMQ_URL)
        configured = (self.settings.RABBITMQ_VHOST or "/").strip() or "/"
        url_path = parsed.path or ""

        if configured != "/":
            vhost = configured if configured.startswith("/") else f"/{configured}"
        elif url_path not in {"", "/"}:
            vhost = url_path
        else:
            vhost = "/"

        path = "/" if vhost == "/" else quote(vhost, safe="/")
        return urlunparse(
            (parsed.scheme, parsed.netloc, path, "", parsed.query, parsed.fragment)
        )

    async def connect(self) -> None:
        """Connect to RabbitMQ."""
        self.connection = await aio_pika.connect_robust(url=self._connection_url())
        self.channel = await self.connection.channel()
        self.initialized = True

    async def connect_with_qos(self, *, prefetch_count: int) -> None:
        await self.connect()
        assert self.channel is not None
        self._prefetch_count = prefetch_count
        await self.channel.set_qos(prefetch_count=prefetch_count)

    async def _reopen_channel(self) -> aio_pika.abc.AbstractChannel:
        """Open a fresh channel (and restore QoS) after a channel-level error."""
        if self.connection is None or self.connection.is_closed:
            raise RuntimeError("Not connected to RabbitMQ")
        self.channel = await self.connection.channel()
        self.queues.clear()
        if self._prefetch_count is not None:
            await self.channel.set_qos(prefetch_count=self._prefetch_count)
        return self.channel

    async def declare_named_queue(self, queue_name: str) -> aio_pika.abc.AbstractQueue:
        """Declare/cache a queue by exact name.

        Tries ``durable=True`` first (create-or-match). If the queue already
        exists with different args (common for externally managed queues),
        reopens the channel and attaches passively.
        """
        await self.check_connection()
        assert self.channel is not None
        if queue_name in self.queues:
            return self.queues[queue_name]
        try:
            queue = await self.channel.declare_queue(name=queue_name, durable=True)
        except ChannelPreconditionFailed:
            logger.warning(
                "queue %r exists with incompatible declare args; attaching passively",
                queue_name,
            )
            channel = await self._reopen_channel()
            queue = await channel.declare_queue(name=queue_name, passive=True)
        self.queues[queue_name] = queue
        return queue

    async def disconnect(self) -> None:
        """Close the RabbitMQ connection."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()

        self.initialized = False
        self.queues.clear()

    async def publish_bytes(
        self,
        queue_name: str,
        body: bytes,
        *,
        headers: dict[str, Any] | None = None,
        content_type: str | None = None,
    ) -> None:
        """Publish raw bytes to an exact queue name."""
        await self.check_connection()
        assert self.channel is not None
        queue = await self.declare_named_queue(queue_name)
        message = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers=headers or {},
            content_type=content_type,
        )
        await self.channel.default_exchange.publish(
            message,
            routing_key=queue.name,
        )

    async def consume_unacked(
        self,
        queue_name: str,
        callback: Any,
    ) -> aio_pika.abc.ConsumerTag:
        """Start a consumer on an exact queue name without auto-ack."""
        queue = await self.declare_named_queue(queue_name)
        return await queue.consume(callback, no_ack=False)

    async def receive(self, *args: Any, **kwargs: Any) -> str | None:
        raise NotImplementedError(f"{type(self).__name__} does not implement receive()")

    async def check_connection(self) -> None:
        """Raise if the client is not connected."""
        await super().check_connection()
        if not self.channel:
            self.initialized = False
            raise RuntimeError("Not connected to RabbitMQ")
