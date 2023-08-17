from __future__ import annotations

import logging

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
        self.listener_connection = None
        self.listener_channel = None
        self.listener_queue = None
        self.__publisher_channel_pool = None

    async def _broker_reachable(self) -> bool:
        try:
            await aio_pika.connect(self.url)
        except Exception as e:
            self.logger.warning(str(e))
            return False
        else:
            return True

    async def _connection(self) -> "aio_pika.RobustConnection":
        return await aio_pika.connect_robust(self.url)

    async def _channel(
            self,
            connection: "aio_pika.RobustConnection"
    ) -> "aio_pika.RobustChannel":
        return await connection.channel()

    @property
    def _publisher_channel_pool(self) -> "aio_pika.pool.Pool":
        if not self.__publisher_channel_pool:
            async def channel_constructor():
                connection = await self._connection()
                channel = await connection.channel()
                return channel

            self.__publisher_channel_pool = aio_pika.pool.Pool(
                channel_constructor, max_size=10)
        return self.__publisher_channel_pool

    async def _exchange(
            self,
            channel: "aio_pika.RobustChannel"
    ) -> "aio_pika.RobustExchange":
        options = {**self.exchange_options}
        name = options.pop("name", "dispatcher")
        return await channel.declare_exchange(name, **options)

    async def _queue(self, channel, exchange) -> "aio_pika.Queue":
        options = {**self.queue_options}
        name = options.pop("name", self.namespace)
        extra_routing_keys = options.pop("extra_routing_keys", [])
        queue = await channel.declare_queue(
            name, arguments={"x-expires": 3600000, "x-message-ttl": 60000},
            **options)
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
                        body=payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        expiration=ttl,
                    ),
                    routing_key=namespace
                )
        except Exception as e:
            self.logger.error(f"{e.__class__.__name__}: {e}")
            raise ConnectionError("Failed to publish payload")

    async def _listen(self):
        while True:
            try:
                if self.listener_connection is None:
                    self.listener_connection = await self._connection()
                    self.listener_channel = await self._channel(
                        self.listener_connection
                    )
                    exchange = await self._exchange(self.listener_channel)
                    self.listener_queue = await self._queue(
                        self.listener_channel, exchange
                    )
                async with self.listener_queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            yield message.body
            except Exception as e:  # noqa
                self.logger.error(f"{e.__class__.__name__}: {e}")
                raise ConnectionError
