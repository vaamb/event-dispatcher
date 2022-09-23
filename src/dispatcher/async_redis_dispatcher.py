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
            parent_logger: logging.Logger = None
    ) -> None:
        if aioredis is None:
            raise RuntimeError(
                "Install 'redis' package to use AsyncRedisDispatcher"
            )
        if not hasattr(aioredis.Redis, "from_url"):
            raise RuntimeError("Version 2 of aioredis package is required.")
        self.redis_options = redis_options or {}
        self.redis_url = url
        super().__init__(namespace=namespace, parent_logger=parent_logger)

    async def _publish(self, namespace: str, payload: bytes) -> int:
        return await self.redis.publish(namespace, payload)

    async def _listen(self):
        self.pubsub.subscribe(self.namespace)
        while self._running.is_set():
            try:
                async for message in self.pubsub.listen():
                    if "data" in message:
                        yield message["data"]
            except Exception as e:
                self.logger.exception(
                    f"Error while reading from queue. Error msg: {e.args}"
                )

    def initialize(self) -> None:
        try:
            self.redis = aioredis.Redis.from_url(self.redis_url,
                                                 **self.redis_options)
            self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        except RedisError as e:
            self.logger.error(
                f"Encountered an error while connecting to the server: Error msg: "
                f"`{e.__class__.__name__}: {e}`."
            )
        else:
            asyncio.ensure_future(self._handle_connect())