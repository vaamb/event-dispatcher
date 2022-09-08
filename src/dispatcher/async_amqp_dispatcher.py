import logging
import pickle

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
        return await channel.declare_exchange(self.namespace)

    async def _queue(self, channel, exchange):
        queue = await channel.declare_queue()
        await queue.bind(exchange)
        return queue

    def _parse_payload(self, payload: bytes) -> dict:
        return pickle.loads(payload)

    async def _publish(self, namespace: str, payload: dict):
        connection = await self._connection()
        channel = await self._channel(connection)
        exchange = await self._exchange(channel)
        await exchange.publish(
            aio_pika.Message(body=pickle.dumps(payload),
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
        self._trigger_event("connect")
