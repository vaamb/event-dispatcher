from __future__ import annotations

import logging
from threading import Lock
import typing as t
from typing import Iterator

if t.TYPE_CHECKING:
    import redis

try:
    import redis
except ImportError:
    redis = None  # ty: ignore[invalid-assignment]

from .ABC import Dispatcher


class RedisDispatcher(Dispatcher):
    """Redis-based events dispatcher

    This class implements an event dispatcher using Redis as the message broker.
    Only kept as an example as Kombu is able to support Redis.

    :param namespace: The name of the dispatcher the events will be sent from
                      and sent to.
    :param url: The connection URL for the Redis server.
    :param parent_logger: A logging.Logger instance. The dispatcher logger
                          will be set to 'parent_logger.namespace'.
    :param redis_options: Options to pass to the Redis instance.
    """
    def __init__(
            self,
            namespace: str = "event_dispatcher",
            url: str = "redis://localhost:6379/0",
            parent_logger: logging.Logger | None = None,
            redis_options: dict | None = None,
            queue_options: dict | None = None,
            reconnection: bool = True,
            debug: bool = False,
    ) -> None:
        if redis is None:
            raise RuntimeError(
                "Install 'redis' package to use RedisDispatcher"
            )
        super().__init__(namespace, parent_logger, reconnection, debug)
        self.redis_options: dict = redis_options or {}
        self.redis_url: str = url
        self.queue_options: dict = queue_options or {}
        self._redis: redis.Redis | None = None
        self._pubsub: redis.client.PubSub | None = None
        self._connections_lock = Lock()
        # Do not close the client under a publish in progress
        self._publisher_lock = Lock()

    @property
    def redis_client(self) -> redis.Redis:
        if self._redis is None:
            self._redis = redis.Redis.from_url(self.redis_url, **self.redis_options)
        return self._redis

    @property
    def pubsub(self) -> redis_client.client.PubSub:  # ty: ignore[unresolved-attribute]
        if self._pubsub is None:
            self._pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
        return self._pubsub

    def _channels(self) -> list[str]:
        options = {**self.queue_options}
        name = options.pop("name", self.namespace)
        channels = [name]
        extra_routing_keys = options.pop("extra_routing_keys", [])
        if isinstance(extra_routing_keys, str):
            extra_routing_keys = [extra_routing_keys]
        channels += extra_routing_keys
        if name != self.namespace:
            channels += [self.namespace]
        return channels

    def _clear_connections(self) -> None:
        # Make sure each connection is closed exactly once by whichever thread
        # gets here first
        with self._connections_lock:
            pubsub = self._pubsub
            redis_client = self._redis
            self._pubsub = None
            self._redis = None
        if pubsub is not None:
            pubsub.close()
        if redis_client is not None:
            with self._publisher_lock:
                redis_client.close()

    def _broker_reachable(self) -> bool:
        # Start from fresh clients so a reconnection attempt doesn't reuse the
        # state of the connections that were just lost.
        self._clear_connections()
        try:
            self.redis_client.ping()
        except redis.RedisError as e:
            self.logger.debug(
                f"Encountered an exception while trying to reach the broker. "
                f"ERROR msg: `{e.__class__.__name__} :{e}`."
            )
            return False
        else:
            return True

    def _publish(
            self,
            namespace: str,
            payload: bytes | bytearray,
            ttl: int | None = None,
            timeout: int | float | None = None,
    ) -> None:
        with self._publisher_lock:
            try:
                # redis-py does not accept `bytearray` payloads
                self.redis_client.publish(namespace, bytes(payload))
            except Exception as e:
                self.logger.error(
                    f"Encountered an exception while trying to publish message. "
                    f"ERROR msg: `{e.__class__.__name__}: {e}`."
                )
                raise ConnectionError("Failed to publish payload")

    def _listen(self) -> Iterator[bytes]:
        pubsub = self.pubsub
        if not pubsub.subscribed:
            pubsub.subscribe(*self._channels())
        while self.running:
            try:
                # Short timeout so `stop()` is noticed even if the stop signal
                # never reaches this dispatcher. `handle_message()` returns
                # `None` on timeout and for the (ignored) subscribe confirmations.
                message = pubsub.handle_message(
                    pubsub.parse_response(block=False, timeout=1))
            except Exception as e:  # noqa
                self.logger.error(
                    f"Encountered an exception while trying to listen to "
                    f"messages. ERROR msg: `{e.__class__.__name__}: {e}`."
                )
                raise ConnectionError("Connection to broker lost")
            if message is not None:
                yield message["data"]

    def _handle_stop_signal(self, *args, **kwargs) -> None:
        super()._handle_stop_signal(*args, **kwargs)
        self._clear_connections()
