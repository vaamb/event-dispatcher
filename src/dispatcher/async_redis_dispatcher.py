from __future__ import annotations

import asyncio
import logging

from .ABC import AsyncDispatcher

try:
    from redis import asyncio as aioredis
    from redis.exceptions import RedisError
except ImportError:
    try:
        import aioredis
        from aioredis.exceptions import RedisError
    except ImportError:
        aioredis = None
        RedisError = None


class AsyncRedisDispatcher(AsyncDispatcher):
    """An async Redis-based events dispatcher

        This class implements an event dispatcher using Redis as the message broker.

        :param namespace: The name of the dispatcher the events will be sent from
                          and sent to.
        :param url: The connection URL for the Redis server.
        :param parent_logger: A logging.Logger instance. The dispatcher logger
                              will be set to 'parent_logger.namespace'.
        :param redis_options: Options to pass to the Redis instance.
        """
    def __init__(
            self,
            namespace: str,
            url: str = "redis://localhost:6379/0",
            redis_options: dict = None,
            parent_logger: logging.Logger = None,
            queue_options: dict = None,
    ) -> None:
        if aioredis is None:
            raise RuntimeError(
                "Install 'redis' package to use AsyncRedisDispatcher"
            )
        if not hasattr(aioredis.Redis, "from_url"):
            raise RuntimeError("Version 2 of aioredis package is required.")
        super().__init__(namespace=namespace, parent_logger=parent_logger)
        self.redis_options = redis_options or {}
        self.redis_url = url
        self.queue_options = queue_options or {}

    async def initialize(self) -> None:
        try:
            self.redis = aioredis.Redis.from_url(self.redis_url,
                                                 **self.redis_options)
            self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        except RedisError as e:
            self.logger.error(
                f"Encountered an error while connecting to the server: Error msg: "
                f"`{e.__class__.__name__}: {e}`."
            )

    async def _publish(self, namespace: str, payload: bytes,
                       ttl: int | None = None) -> int:
        return await self.redis.publish(namespace, payload)

    async def _listen(self):
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
        while self._running.is_set():
            try:
                async for message in self.pubsub.listen():
                    if "data" in message:
                        yield message["data"]
            except Exception as e:
                self.logger.exception(
                    f"Error while reading from queue. Error msg: {e.args}"
                )
