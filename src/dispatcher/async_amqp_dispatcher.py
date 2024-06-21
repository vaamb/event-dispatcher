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
            namespace: str = "event_dispatcher",
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

    async def _broker_reachable(self) -> bool:
        try:
            await self._clear_connections()
            await self._ensure_connected(self.listener_connection)
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
                self._listener_connection.transport = None

            self._listener_connection.close_callbacks.add(reset)
        return self._listener_connection

    @staticmethod
    async def _ensure_connected(
            connection: "aio_pika.Connection"
    ) -> "aio_pika.Connection":
        if connection.transport is None:
            await connection.connect()
        return connection

    async def _clear_connections(self) -> None:
        if self._publisher_connection is not None:
            await self._publisher_connection.close()
            self._publisher_connection = None
        if self._listener_connection is not None:
            await self._listener_connection.close()
            self._listener_connection = None

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
            ttl: int | None = None,
            timeout: int | float | None = None,
    ) -> None:
        try:
            await self._ensure_connected(self.publisher_connection)
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
                    timeout=timeout,
                    **self.publisher_options
                )
        except Exception as e:
            self.logger.error(
                f"Encountered an exception while trying to publish message. "
                f"ERROR msg: `{e.__class__.__name__}: {e}`."
            )
            await self._clear_connections()
            raise ConnectionError("Failed to publish payload")

    async def _listen(self) -> AsyncIterator[bytes]:
        await self._ensure_connected(self.listener_connection)
        async with self.listener_connection.channel() as channel:
            await channel.get_underlay_channel()
            exchange = await self._exchange(channel)
            listener_queue = await self._queue(channel, exchange)

            while self.running:
                try:
                    message: aio_pika.IncomingMessage = await listener_queue.get(timeout=60)
                    await message.ack()
                    yield message.body
                except aio_pika.exceptions.QueueEmpty:
                    # listener_queue.get() timed out, start a new cycle
                    continue
                except Exception as e:  # noqa
                    self.logger.error(
                        f"Encountered an exception while trying to listen to "
                        f"messages. ERROR msg: `{e.__class__.__name__}: {e}`."
                    )
                    raise ConnectionError("Connection to broker lost")

    async def _handle_broker_disconnect(self) -> None:
        await self._clear_connections()
        await super()._handle_broker_disconnect()

    async def _handle_stop_signal(self, *args, **kwargs) -> None:
        await self._clear_connections()
        await super()._handle_stop_signal()
