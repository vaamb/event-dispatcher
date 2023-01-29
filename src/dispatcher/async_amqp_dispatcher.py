from __future__ import annotations

import logging

from .ABC import AsyncDispatcher

try:
    import aio_pika
except ImportError:
    aio_pika = None


class AsyncAMQPDispatcher(AsyncDispatcher):
    """An event dispatcher that uses RabbitMQ as message broker

    This class implements an event dispatcher using RabbitMQ as the message
    broker.

    :param namespace: The name of the dispatcher the events will be sent from
                      and sent to.
    :param url: The connection URL for the RabbitMQ server.
    :param parent_logger: A logging.Logger instance. The dispatcher logger
                          will be set to 'parent_logger.namespace'.
    :param exchange_options: Options for the aio_pika exchange.
    :param queue_options: Options to pass to aio_pika queue.
    :param exchange_opt: Options to pass to aio_pika exchange.
    :param connection_options: Options to pass to aio_pika connection.
    """
    def __init__(
            self,
            namespace: str,
            url: str = "amqp://guest:guest@localhost:5672//",
            parent_logger: logging.Logger = None,
            exchange_options: dict = None,
            queue_options: dict = None,
            connection_options: dict = None,
    ) -> None:
        if aio_pika is None:
            raise RuntimeError(
                "Install 'aio_pika' package to use AsyncAMQPDispatcher"
            )
        super().__init__(namespace=namespace, parent_logger=parent_logger)
        self.url = url
        self.exchange_options = exchange_options or {}
        self.queue_options = queue_options or {}
        self.connection_options = connection_options or {}
        self.__connection_pool = None
        self.__channel_pool = None

    async def _connection(self) -> "aio_pika.RobustConnection":
        return await aio_pika.connect_robust(self.url)

    @property
    def _connection_pool(self) -> "aio_pika.pool.Pool":
        if self.__connection_pool is None:
            pool_size = self.connection_options.get("connection_pool_max_size", 2)
            self.__connection_pool = aio_pika.pool.Pool(self._connection, max_size=pool_size)
        return self.__connection_pool

    async def _channel(self) -> "aio_pika.RobustChannel":
        async with self._connection_pool.acquire() as connection:
            return await connection.channel()

    @property
    def _channel_pool(self) -> "aio_pika.pool.Pool":
        if not self.__channel_pool:
            pool_size = self.connection_options.get("channel_pool_max_size", 10)
            self.__channel_pool = aio_pika.pool.Pool(self._channel, max_size=pool_size)
        return self.__channel_pool

    async def _exchange(self, channel: "aio_pika.RobustChannel") -> "aio_pika.Exchange":
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
        async with self._channel_pool.acquire() as channel:
            exchange = await self._exchange(channel)
            await exchange.publish(
                aio_pika.Message(
                    body=payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    expiration=ttl,
                ),
                routing_key=namespace
            )

    async def _listen(self):
        async with self._channel_pool.acquire() as channel:
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

    async def initialize(self) -> None:
        pass
