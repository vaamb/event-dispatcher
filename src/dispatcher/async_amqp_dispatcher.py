from __future__ import annotations

import logging
from typing import AsyncIterator

from .ABC import AsyncDispatcher

try:
    import aio_pika
    import aiormq
except ImportError:
    aio_pika = None
    aiormq = None


class AsyncAMQPDispatcher(AsyncDispatcher):
    """An event dispatcher that uses RabbitMQ as message broker

    This class implements an event dispatcher using RabbitMQ as the message
    broker.

    :param namespace: The name of the dispatcher the events will be sent from
                      and sent to.
    :param url: The connection URL for the RabbitMQ server.
    :param parent_logger: A logging.Logger instance. The dispatcher logger
                          will be set to 'parent_logger.namespace'.
    :param exchange_options: Options to pass to aio_pika exchange.
    :param queue_options: Options to pass to aio_pika queue.
    :param connection_options: Options to pass to aio_pika connection.
    :param connection_options: Options to pass to aio_pika.Exchange().publish.
    """
    def __init__(
            self,
            namespace: str,
            url: str = "amqp://guest:guest@localhost:5672//",
            parent_logger: logging.Logger = None,
            connection_options: dict = None,
            exchange_options: dict = None,
            queue_options: dict = None,
            publisher_options: dict = None,
            publisher_pool_size: int = 10,
    ) -> None:
        if aio_pika is None:
            raise RuntimeError(
                "Install 'aio_pika' package to use AsyncAMQPDispatcher")
        super().__init__(namespace=namespace, parent_logger=parent_logger)
        self.url = url
        self.connection_options: dict = connection_options or {}
        self.exchange_options: dict = exchange_options or {}
        self.queue_options: dict = queue_options or {}
        self.publisher_options: dict = publisher_options or {}
        self.publisher_pool_size: int = publisher_pool_size or 10
        self.listener_connection: "aio_pika.Connection" | None = None
        self.listener_channel: "aio_pika.Channel" | None = None
        self.listener_queue: "aio_pika.Queue" | None = None
        self.__publisher_channel_pool: "aio_pika.pool.Pool" | None = None

    async def _broker_reachable(self) -> bool:
        try:
            await aio_pika.connect(self.url)
        except Exception as e:
            self.logger.warning(str(e))
            return False
        else:
            return True

    async def _connection(self) -> "aio_pika.Connection":
        return await aio_pika.connect(url=self.url, **self.connection_options)  # noqa

    async def _channel(
            self,
            connection: "aio_pika.Connection"
    ) -> "aio_pika.Channel":
        return await connection.channel()  # noqa

    @property
    def _publisher_channel_pool(self) -> "aio_pika.pool.Pool":
        if not self.__publisher_channel_pool:
            async def channel_constructor():
                connection = await self._connection()
                channel = await connection.channel()
                return channel

            self.__publisher_channel_pool = aio_pika.pool.Pool(
                channel_constructor, max_size=self.publisher_pool_size)
        return self.__publisher_channel_pool

    async def _exchange(
            self,
            channel: "aio_pika.Channel"
    ) -> "aio_pika.Exchange":
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

    async def _publish(
            self,
            namespace: str,
            payload: bytes,
            ttl: int | None = None
    ) -> None:
        try:
            async with self._publisher_channel_pool.acquire() as channel:
                exchange = await self._exchange(channel)
                await exchange.publish(
                    aio_pika.Message(
                        body=payload,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        expiration=ttl,
                        content_type='application/binary', content_encoding='binary'
                    ),
                    routing_key=namespace,
                    **self.publisher_options
                )
        except Exception as e:
            self.logger.error(f"{e.__class__.__name__}: {e}")
            raise ConnectionError("Failed to publish payload")

    async def _listen(self) -> AsyncIterator[bytes]:
        while True:
            try:
                if self.listener_connection is None:
                    self.listener_connection = await self._connection()
                    self.listener_channel = await self._channel(
                        self.listener_connection)
                    exchange = await self._exchange(self.listener_channel)
                    self.listener_queue = await self._queue(
                        self.listener_channel, exchange)
                async with self.listener_queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        message: aio_pika.IncomingMessage
                        async with message.process():
                            yield message.body
            except Exception as e:  # noqa
                self.logger.error(f"{e.__class__.__name__}: {e}")
                raise ConnectionError("Connection to broker lost")
