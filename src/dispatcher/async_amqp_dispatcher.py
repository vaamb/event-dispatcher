import asyncio
import logging

from .ABC import AsyncDispatcher

try:
    import aio_pika
except ImportError:
    aio_pika = None


class AsyncAMQPDispatcher(AsyncDispatcher):
    def __init__(
            self,
            namespace: str,
            url: str = "amqp://guest:guest@localhost:5672//",
            parent_logger: logging.Logger = None
    ) -> None:
        self.url = url
        super().__init__(namespace=namespace, parent_logger=parent_logger)

    async def _connection(self):
        return await aio_pika.connect_robust(self.url)

    async def _channel(self, connection):
        return await connection.channel()

    async def _exchange(self, channel):
        return await channel.declare_exchange("ouranos")

    async def _queue(self, channel, exchange):
        queue = await channel.declare_queue(name=self.namespace)
        await queue.bind(exchange, routing_key=self.namespace)
        return queue

    async def _publish(self, namespace: str, payload: bytes):
        connection = await self._connection()
        channel = await self._channel(connection)
        exchange = await self._exchange(channel)
        await exchange.publish(
            aio_pika.Message(body=payload,
                             delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=namespace
        )

    async def _listen(self):
        while True:
            try:
                connection = await self._connection()
                channel = await self._channel(connection)
                exchange = await self._exchange(channel)
                queue = await self._queue(channel, exchange)
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            yield message.body
            except Exception:
                self.logger.exception("Connection error while reading from queue")

    def initialize(self) -> None:
        asyncio.ensure_future(self._handle_connect())
