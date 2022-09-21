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
            namespace: str,
            url: str = "redis://localhost:6379/0",
            redis_options: dict = None,
            parent_logger: logging.Logger = None
    ) -> None:
        if redis is None:
            raise RuntimeError(
                "Install 'redis' package to use RedisDispatcher"
        )
        self.redis_options = redis_options or {}
        self.redis_url = url
        super().__init__(namespace=namespace, parent_logger=parent_logger)

    def initialize(self) -> None:
        try:
            self.redis = redis.Redis.from_url(self.redis_url, **self.redis_options)
            self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        except redis.ConnectionError as e:
            self.logger.error(
                f"Encountered an error while connecting to the server: Error msg: "
                f"`{e.__class__.__name__}: {e}`."
            )
        else:
            self._trigger_event("connect")

    def _publish(self, namespace: str, payload: bytes) -> int:
        return self.redis.publish(namespace, payload)

    def _listen(self):
        self.pubsub.subscribe(self.namespace)
        for message in self.pubsub.listen():
            if "data" in message:
                yield message["data"]
