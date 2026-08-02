import aio_pika
import aio_pika.abc

from scenario_pipeliner.worker.core.clients import AsyncBrokerClient
from scenario_pipeliner.worker.core.custom_settings import RabbitMQSettings


class AsyncRabbitMQClient(AsyncBrokerClient[RabbitMQSettings]):
    """Асинхронный клиент для RabbitMQ."""

    def __init__(self, settings: RabbitMQSettings | None = None):
        super().__init__(settings=settings or RabbitMQSettings())

        self.connection: aio_pika.abc.AbstractRobustConnection | None = None
        self.channel: aio_pika.abc.AbstractChannel | None = None
        self.queues: dict[str, aio_pika.abc.AbstractQueue] = {}

    async def connect(self) -> None:
        """Подключиться к RabbitMQ."""
        self.connection = await aio_pika.connect_robust(
            url=self.settings.RABBITMQ_URL,
            virtualhost=self.settings.RABBITMQ_VHOST,
        )
        self.channel = await self.connection.channel()
        self.initialized = True

    async def get_or_declare_queue(self, source: str) -> aio_pika.abc.AbstractQueue:
        """Получить или создать очередь для конкретного источника."""
        await self.check_connection()
        assert self.channel is not None

        if source not in self.queues:
            queue_name = f"{self.settings.RABBITMQ_QUEUE}_{source}"
            self.queues[source] = await self.channel.declare_queue(
                name=queue_name,
                durable=True,
            )
        return self.queues[source]

    async def disconnect(self) -> None:
        """Отключиться от RabbitMQ."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()

        self.initialized = False
        self.queues.clear()

    async def send(self, message: str, source: str = "default") -> None:
        """Отправить сообщение в RabbitMQ."""
        await self.check_connection()
        assert self.channel is not None

        queue = await self.get_or_declare_queue(source)
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=message.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=queue.name,
        )

    async def receive(self, source: str = "default", *args, **kwargs) -> str | None:
        """Получить сообщение из RabbitMQ."""
        await self.check_connection()

        queue = await self.get_or_declare_queue(source)
        try:
            incoming_message = await queue.get(
                timeout=self.settings.RABBITMQ_TIMEOUT,
            )
            if incoming_message:
                await incoming_message.ack()
                return incoming_message.body.decode()
        except (TimeoutError, aio_pika.exceptions.QueueEmpty):
            pass

        return None

    async def check_connection(self) -> None:
        """Проверка подключения к RabbitMQ."""
        await super().check_connection()
        if not self.channel:
            self.initialized = False
            raise RuntimeError("Not connected to RabbitMQ")
