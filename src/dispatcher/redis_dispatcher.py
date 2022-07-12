import logging
import pickle

try:
    import redis
except ImportError:
    redis = None

from .template import DispatcherTemplate


class RedisDispatcher(DispatcherTemplate):
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
        super(RedisDispatcher, self).__init__(namespace, parent_logger)

    def _connect(self):
        self.redis = redis.Redis.from_url(self.redis_url, **self.redis_options)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)

    def _parse_payload(self, payload: dict) -> dict:
        data = payload["data"]
        return pickle.loads(data)

    def _publish(self, namespace: str, payload: dict) -> int:
        message = pickle.dumps(payload)
        return self.redis.publish(namespace, message)

    def _listen(self):
        self.pubsub.subscribe(self.namespace)
        for message in self.pubsub.listen():
            yield message
