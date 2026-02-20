from __future__ import annotations

import logging

try:
    import redis
except ImportError:
    redis = None

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
            parent_logger: logging.Logger = None,
            redis_options: dict = None,
            queue_options: dict = None,
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
        self.redis: redis.Redis | None = None
        self.pubsub: redis.client.PubSub | None = None

    def _broker_reachable(self) -> bool:
        try:
            redis.Redis.from_url(self.redis_url, **self.redis_options)
        except redis.ConnectionError as e:
            self.logger.debug(
                f"Encountered an exception while trying to reach the broker. "
                f"ERROR msg: `{e.__class__.__name__} :{e}`."
            )
            return False
        else:
            return True

    def _connect_to_redis(self) -> None:
        try:
            self.redis = redis.Redis.from_url(
                self.redis_url, **self.redis_options)
            self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        except redis.ConnectionError as e:
            self.logger.error(
                f"Encountered an error while connecting to the server: Error msg: "
                f"`{e.__class__.__name__}: {e}`."
            )

    def _subscribe(self) -> None:
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

    def _publish(
            self,
            namespace: str,
            payload: bytes,
            ttl: int | None = None,
            timeout: int | float | None = None,
    ) -> int:
        try:
            return self.redis.publish(namespace, payload)
        except Exception as e:
            self.logger.error(f"{e.__class__.__name__}: {e}")
            raise ConnectionError("Failed to publish payload")

    def _listen(self):
        while self.running:
            try:
                if self.redis is None:
                    self._connect_to_redis()
                    self._subscribe()
                try:
                    message = self.pubsub.handle_message(
                        self.pubsub.parse_response(block=True, timeout=1))
                except TimeoutError:
                    pass
                else:
                    if "data" in message:
                        yield message["data"]
            except Exception as e:  # noqa
                self.logger.error(f"{e.__class__.__name__}: {e}")
                raise ConnectionError
