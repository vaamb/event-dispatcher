from __future__ import annotations

import logging
import typing as t

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
        RedisError = Exception  # ty: ignore[conflicting-declarations]


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
        self.redis: aioredis.Redis | None = None  # ty: ignore[unresolved-attribute]
        self.pubsub: aioredis.client.PubSub | None = None  # ty: ignore[unresolved-attribute]

    async def _broker_reachable(self) -> bool:
        try:
            aioredis.Redis.from_url(self.redis_url, **self.redis_options)  # ty: ignore[unresolved-attribute]
        except RedisError as e:
            self.logger.debug(
                f"Encountered an exception while trying to reach the broker. "
                f"ERROR msg: `{e.__class__.__name__} :{e}`."
            )
            return False
        else:
            return True

    def _connect_to_redis(self) -> None:
        try:
            self.redis = aioredis.Redis.from_url(  # ty: ignore[unresolved-attribute]
                self.redis_url, **self.redis_options)
            self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        except RedisError as e:
            self.logger.error(
                f"Encountered an error while connecting to the server: Error msg: "
                f"`{e.__class__.__name__}: {e}`.")

    def _subscribe(self) -> None:
        assert self.pubsub is not None
        options = {**self.queue_options}
        name = options.pop("name", self.namespace)
        extra_routing_keys = options.pop("extra_routing_keys", [])
        self.pubsub.subscribe(name)
        if isinstance(extra_routing_keys, str):
            extra_routing_keys = [extra_routing_keys]
        if name != self.namespace:
            extra_routing_keys.append(name)
        for key in extra_routing_keys:
            self.pubsub.subscribe(key)

    async def _publish(
            self,
            namespace: str,
            payload: bytes | bytearray,
            ttl: int | None = None,
            timeout: int | float | None = None,
    ) -> None:
        assert self.redis is not None
        try:
            await self.redis.publish(namespace, payload)
        except Exception as e:
            self.logger.error(f"{e.__class__.__name__}: {e}")
            raise ConnectionError("Failed to publish payload")

    async def _listen(self):
        while self.running:
            try:
                if self.redis is None:
                    self._connect_to_redis()
                    self._subscribe()
                assert self.pubsub is not None
                try:
                    message = await self.pubsub.handle_message(
                        await self.pubsub.parse_response(block=False, timeout=1))
                except TimeoutError:
                    pass
                else:
                    if "data" in message:
                        yield message["data"]
            except Exception as e:  # noqa
                self.logger.error(f"{e.__class__.__name__}: {e}")
                raise ConnectionError
