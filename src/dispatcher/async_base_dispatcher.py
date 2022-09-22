import asyncio
import logging

from ._pubsub import AsyncPubSub
from .ABC import AsyncDispatcher


class AsyncBaseDispatcher(AsyncDispatcher):
    """A simple in memory Pub Sub-based event dispatcher

    This class implements an event dispatcher using StupidPubSub as the message
    broker. While Kombu is able to support an in-memory queue, this class
    remains a lighter alternative.

    :param namespace: The name of the dispatcher the events will be sent from
                      and sent to
    :param pubsub: A pub sub having at least the methods 'listen', 'publish' and
                   'subscribe'
    :param parent_logger: A logging.Logger instance. The dispatcher logger
                          will be set to 'parent_logger.namespace'
    """
    def __init__(
            self,
            namespace: str,
            parent_logger: logging.Logger = None
    ) -> None:
        self.pubsub = AsyncPubSub()
        super().__init__(namespace, parent_logger)

    async def _publish(self, namespace: str, payload: dict) -> int:
        return await self.pubsub.publish(namespace, payload)

    async def _listen(self):
        self.pubsub.subscribe(self.namespace)
        async for message in self.pubsub.listen():
            yield message

    def initialize(self) -> None:
        asyncio.ensure_future(self._handle_connect())
