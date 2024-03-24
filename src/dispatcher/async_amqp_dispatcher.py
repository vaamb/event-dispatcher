from __future__ import annotations

from asyncio import Queue
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
        self._publisher_connection: "aio_pika.Connection" | None = None
        self._listener_connection: "aio_pika.Connection" | None = None
        self._listener_message_queue: Queue = Queue()

    async def _broker_reachable(self) -> bool:
        try:
            if self.listener_connection.transport is None:
                await self.listener_connection.connect()
        except Exception as e:
            self.logger.debug(
                f"Encountered an exception while trying to reach the broker. "
                f"ERROR msg: `{e.__class__.__name__} :{e}`."
            )
            return False
        else:
            return True

    def _connection(self) -> "aio_pika.Connection":
        return aio_pika.Connection(url=self.url, **self.connection_options)  # noqa

    @property
    def publisher_connection(self) -> "aio_pika.Connection":
        if self._publisher_connection is None:
            self._publisher_connection = self._connection()

            async def reset(*args, **kwargs) -> None:
                self._publisher_connection.transport = None

            self._publisher_connection.close_callbacks.add(reset)
        return self._publisher_connection

    @property
    def listener_connection(self) -> "aio_pika.Connection":
        if self._listener_connection is None:
            self._listener_connection = self._connection()

            async def reset(*args, **kwargs) -> None:
                if self._listener_connection.transport is not None:
                    await self._listener_message_queue.put(None)
                self._listener_connection.transport = None

            self._listener_connection.close_callbacks.add(reset)
        return self._listener_connection

    async def _exchange(
            self,
            channel: "aio_pika.Channel"
    ) -> "aio_pika.Exchange":
        options = {**self.exchange_options}
        name = options.pop("name", "dispatcher")
        exchange = await channel.declare_exchange(name, **options)
        return exchange

    async def _queue(
            self,
            channel: "aio_pika.Channel",
            exchange: "aio_pika.Exchange",
    ) -> "aio_pika.Queue":
        options = {**self.queue_options}
        name = options.pop("name", self.namespace)
        extra_routing_keys = options.pop("extra_routing_keys", [])
        queue = await channel.declare_queue(name)
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
            if self.publisher_connection.transport is None:
                await self.publisher_connection.connect()
            async with self.publisher_connection.channel() as channel:
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
            self.logger.error(
                f"Encountered an exception while trying to publish message. "
                f"ERROR msg: `{e.__class__.__name__}: {e}`."
            )
            raise ConnectionError("Failed to publish payload")

    async def _listen(self) -> AsyncIterator[bytes]:
        if self.listener_connection.transport is None:
            await self.listener_connection.connect()

        async with  self.listener_connection.channel() as channel:
            exchange = await self._exchange(channel)
            listener_queue = await self._queue(channel, exchange)

            async def on_message(message: "aio_pika.IncomingMessage") -> None:
                await self._listener_message_queue.put(message)

            async def consume() -> None:
                await listener_queue.consume(on_message)

            while self.running:
                try:
                    await consume()
                    message = await self._listener_message_queue.get()
                    if message is None:
                        raise ConnectionError(
                            "Connection lost."
                        )
                    async with message.process():
                        yield message.body
                except Exception as e:  # noqa
                    self.logger.error(
                        f"Encountered an exception while trying to listen to "
                        f"messages. ERROR msg: `{e.__class__.__name__}: {e}`."
                    )
                    raise ConnectionError("Connection to broker lost")
