from __future__ import annotations

import asyncio
import logging
import typing as t
from typing import AsyncGenerator

from .ABC import AsyncDispatcher

if t.TYPE_CHECKING:
    from redis import asyncio as aioredis
    from redis.exceptions import RedisError

try:
    from redis import asyncio as aioredis
    from redis.exceptions import RedisError
except ImportError:
    try:
        import aioredis  # ty: ignore[unresolved-import]
        from aioredis.exceptions import RedisError  # ty: ignore[unresolved-import]
    except ImportError:
        aioredis = None  # ty: ignore[conflicting-declarations]
        RedisError = Exception  # ty: ignore[invalid-assignment]


class AsyncRedisDispatcher(AsyncDispatcher):
    """An async Redis-based events dispatcher

    This class implements an event dispatcher using Redis as the message broker.

    :param namespace: The name of the dispatcher the events will be sent from
                      and sent to.
    :param url: The connection URL for the Redis server.
    :param parent_logger: A logging.Logger instance. The dispatcher logger
                          will be set to 'parent_logger.namespace'.
    :param redis_options: Options to pass to the Redis instance.
    :param queue_options: Options to add extra routing keys.
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
        if aioredis is None:
            raise RuntimeError(
                "Install 'redis' package to use AsyncRedisDispatcher"
            )
        if not hasattr(aioredis.Redis, "from_url"):
            raise RuntimeError("Version 2 of aioredis package is required.")
        super().__init__(namespace, parent_logger, reconnection, debug)
        self.redis_options: dict = redis_options or {}
        self.redis_url: str = url
        self.queue_options: dict = queue_options or {}
        self._redis: aioredis.Redis | None = None  # ty: ignore[unresolved-attribute]
        self._pubsub: aioredis.client.PubSub | None = None  # ty: ignore[unresolved-attribute]
        # Do not close the client under a publish in progress
        self._publisher_lock = asyncio.Lock()

    @property
    def redis_client(self) -> aioredis.Redis:  # ty: ignore[unresolved-attribute]
        if self._redis is None:
            self._redis = aioredis.Redis.from_url(self.redis_url, **self.redis_options)  # ty: ignore[unresolved-attribute]
        return self._redis

    @property
    def pubsub(self) -> aioredis.client.PubSub:  # ty: ignore[unresolved-attribute]
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

    async def _clear_connections(self) -> None:
        if self._pubsub is not None:
            await self._pubsub.aclose()
            self._pubsub = None
        if self._redis is not None:
            async with self._publisher_lock:
                await self._redis.aclose()
            self._redis = None

    async def _broker_reachable(self) -> bool:
        # Start from fresh clients so a reconnection attempt doesn't reuse the
        # state of the connections that were just lost.
        await self._clear_connections()
        try:
            await self.redis_client.ping()  # ty: ignore[invalid-await]
        except RedisError as e:
            self.logger.debug(
                f"Encountered an exception while trying to reach the broker. "
                f"ERROR msg: `{e.__class__.__name__} :{e}`."
            )
            return False
        else:
            return True

    async def _publish(
            self,
            namespace: str,
            payload: bytes | bytearray,
            ttl: int | None = None,
            timeout: int | float | None = None,
    ) -> None:
        async with self._publisher_lock:
            try:
                # redis-py does not accept `bytearray` payloads
                await self.redis_client.publish(namespace, bytes(payload))
            except Exception as e:
                self.logger.error(
                    f"Encountered an exception while trying to publish message. "
                    f"ERROR msg: `{e.__class__.__name__}: {e}`."
                )
                raise ConnectionError("Failed to publish payload")

    async def _listen(self) -> AsyncGenerator[bytes, None]:
        pubsub = self.pubsub
        if not pubsub.subscribed:
            await pubsub.subscribe(*self._channels())
        while self.running:
            try:
                # Short timeout so the loop regularly checks `running`.
                # `handle_message()` returns `None` on timeout and for the
                # (ignored) subscribe confirmations.
                message = await pubsub.handle_message(
                    await pubsub.parse_response(block=False, timeout=1))
            except Exception as e:  # noqa
                self.logger.error(
                    f"Encountered an exception while trying to listen to "
                    f"messages. ERROR msg: `{e.__class__.__name__}: {e}`."
                )
                raise ConnectionError("Connection to broker lost")
            if message is not None:
                yield message["data"]

    async def _handle_stop_signal(self, *args, **kwargs) -> None:
        await self._clear_connections()
        await super()._handle_stop_signal(*args, **kwargs)
