from __future__ import annotations

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
            parent_logger: logging.Logger = None,
            exchange_options: dict = None,
            queue_options: dict = None,
    ) -> None:
        if aio_pika is None:
            raise RuntimeError(
                "Install 'aio_pika' package to use AsyncAMQPDispatcher"
            )
        super().__init__(namespace=namespace, parent_logger=parent_logger)
        self.url = url
        self.exchange_options = exchange_options or {}
        self.queue_options = queue_options or {}

    async def _connection(self) -> "aio_pika.RobustConnection":
        return await aio_pika.connect_robust(self.url)

    async def _channel(self, connection) -> "aio_pika.Channel":
        return await connection.channel()

    async def _exchange(self, channel) -> "aio_pika.Exchange":
        options = {**self.exchange_options}
        name = options.pop("name", "dispatcher")
        return await channel.declare_exchange(name, **options)

    async def _queue(self, channel, exchange) -> "aio_pika.Queue":
        options = {**self.queue_options}
        name = options.pop("name", self.namespace)
        extra_routing_keys = options.pop("extra_routing_keys", [])
        queue = await channel.declare_queue(name, **options)
        await queue.bind(exchange, routing_key=self.namespace)
        if isinstance(extra_routing_keys, str):
            extra_routing_keys = [extra_routing_keys]
        if name != self.namespace:
            extra_routing_keys.append(name)
        for key in extra_routing_keys:
            await queue.bind(exchange, routing_key=key)
        return queue

    async def _publish(self, namespace: str, payload: bytes,
                       ttl: int | None = None) -> None:
        connection = await self._connection()
        channel = await self._channel(connection)
        exchange = await self._exchange(channel)
        await exchange.publish(
            aio_pika.Message(
                body=payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                expiration=ttl,
            ),
            routing_key=namespace
        )

    async def _listen(self):
        connection = await self._connection()
        channel = await self._channel(connection)
        exchange = await self._exchange(channel)
        queue = await self._queue(channel, exchange)
        while self._running.is_set():
            try:
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            yield message.body
            except Exception as e:
                self.logger.exception(
                    f"Error while reading from queue. Error msg: {e.args}"
                )

    def initialize(self) -> None:
        asyncio.ensure_future(self._handle_connect())
