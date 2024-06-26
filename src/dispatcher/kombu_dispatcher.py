from __future__ import annotations

import queue
import logging
from typing import Iterator

try:
    import kombu
except ImportError:
    kombu = None

from .ABC import Dispatcher


class KombuDispatcher(Dispatcher):
    """An event dispatcher that uses Kombu as message broker

    This class implements an event dispatcher backend for event sharing across
    multiple processes, using RabbitMQ, Redis or any other messaging mechanism
    supported by 'kombu'.

    :param namespace: The name of the dispatcher the events will be sent from
                      and sent to.
    :param url: The connection URL for the message broker. For example,
                'amqp://guest:guest@localhost:5672//' is used for RabbitMQ
                and 'redis://localhost:6379/' for Redis.
    :param parent_logger: A logging.Logger instance. The dispatcher logger
                          will be set to 'parent_logger.namespace'.
    :param exchange_options: Options to pass to 'kombu.Exchange'.
    :param queue_options: Options to pass to 'kombu.Queue'.
    :param publisher_options: Options to pass to 'kombu.Producer().publish'.
    """
    def __init__(
            self,
            namespace: str = "event_dispatcher",
            url: str = "memory://",
            parent_logger: logging.Logger = None,
            exchange_options: dict = None,
            queue_options: dict = None,
            producer_options: dict = None,
            publisher_options: dict = None,
    ) -> None:
        if kombu is None:
            raise RuntimeError(
                "Install 'kombu' package to use KombuDispatcher")
        super(KombuDispatcher, self).__init__(namespace, parent_logger)
        self.url: str = url
        self.exchange_options: dict = exchange_options or {}
        self.queue_options: dict = queue_options or {}
        self.producer_options: dict = producer_options or {}
        self.publisher_options: dict = publisher_options or {}
        self._publisher_connection: "kombu.Connection" | None = None
        self._listener_connection: "kombu.Connection" | None = None

    def _broker_reachable(self) -> bool:
        try:
            self.listener_connection.connect()
        except Exception as e:
            self.logger.debug(
                f"Encountered an exception while trying to reach the broker. "
                f"ERROR msg: `{e.__class__.__name__} :{e}`."
            )
            return False
        else:
            return True

    def _connection(self) -> "kombu.Connection":
        return kombu.Connection(self.url)

    @property
    def publisher_connection(self) -> "kombu.Connection":
        if self._publisher_connection is None:
            self._publisher_connection = self._connection()
        return self._publisher_connection

    @property
    def listener_connection(self) -> "kombu.Connection":
        if self._listener_connection is None:
            self._listener_connection = self._connection()
        return self._listener_connection

    def _channel(self, connection: "kombu.Connection") -> "kombu.connection.Channel":
        retry = 1
        while True:
            try:
                return connection.channel()
            except Exception as e:
                if retry == 0:
                    self.logger.error(
                        f"Could not create a channel. "
                        f"ERROR msg: `{e.__class__.__name__}: {e}`."
                    )
                    raise e
                retry -= 1

    def _exchange(self) -> "kombu.Exchange":
        options = {"durable": False}
        options.update({**self.exchange_options})
        name = options.pop("name", "dispatcher")
        return kombu.Exchange(name, **options)

    def _queue(self) -> "kombu.Queue":
        options = {**self.queue_options}
        name = options.pop("name", self.namespace)
        routing_keys = [name]
        extra_routing_keys = options.pop("extra_routing_keys", [])
        if isinstance(extra_routing_keys, str):
            extra_routing_keys = [extra_routing_keys]
        routing_keys += extra_routing_keys
        if name != self.namespace:
            routing_keys += [self.namespace]
        return kombu.Queue(
            name=name, bindings=[
                kombu.binding(self._exchange(), routing_key=key)
                for key in routing_keys
            ], **options
        )

    def _publish(
            self,
            namespace: str,
            payload: bytes,
            ttl: int | None = None,
            timeout: int | float | None = None,
    ) -> None:
        channel = self._channel(self.publisher_connection)
        try:
            with kombu.Producer(channel, exchange=self._exchange()) as producer:
                producer.publish(
                    payload, routing_key=namespace, expiration=ttl,
                    content_type='application/binary', content_encoding='binary',
                    timeout=timeout, **self.publisher_options)
        except Exception as e:
            self.logger.error(
                f"Encountered an exception while trying to publish message. "
                f"ERROR msg: `{e.__class__.__name__}: {e}`."
            )
            raise ConnectionError("Failed to publish payload")
        finally:
            channel.close()

    def _listen(self) -> Iterator[bytes]:
        self.listener_connection.connect()  # Make sure the connection is connected
        listener_queue = self._queue()
        while self.running:
            try:
                with self.listener_connection.SimpleQueue(listener_queue) as q:
                    message: kombu.Message = q.get(block=True, timeout=60)
                    message.ack()
                    yield message.body
            except queue.Empty:
                continue
            except Exception as e:  # noqa
                self.logger.error(
                    f"Encountered an exception while trying to listen to "
                    f"messages. ERROR msg: `{e.__class__.__name__}: {e}`."
                )
                raise ConnectionError("Connection to broker lost")

    def _handle_stop_signal(self, *args, **kwargs) -> None:
        super()._handle_stop_signal(args, kwargs)
        self.publisher_connection.close()
        self.listener_connection.close()
