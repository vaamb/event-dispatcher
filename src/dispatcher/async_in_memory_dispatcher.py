from __future__ import annotations

import queue
import logging

from ._pubsub import AsyncPubSub
from .ABC import AsyncDispatcher


class AsyncInMemoryDispatcher(AsyncDispatcher):
    """A simple in memory Pub Sub-based event dispatcher

    This class implements an event dispatcher using StupidPubSub as the message
    broker. While Kombu is able to support an in-memory queue, this class
    remains a lighter alternative.

    :param namespace: The name of the dispatcher the events will be sent from
                      and sent to
    :param parent_logger: A logging.Logger instance. The dispatcher logger
                          will be set to 'parent_logger.namespace'
    """
    def __init__(
            self,
            namespace: str = "event_dispatcher",
            parent_logger: logging.Logger = None
    ) -> None:
        self.pubsub: AsyncPubSub = AsyncPubSub()
        super().__init__(namespace, parent_logger)

    async def _broker_reachable(self) -> bool:
        return True

    async def _publish(
            self,
            namespace: str,
            payload: dict,
            ttl: int | None = None,
            timeout: int | float | None = None,
    ) -> int:
        return await self.pubsub.publish(namespace, payload)

    async def _listen(self):
        self.pubsub.subscribe(self.namespace)
        while self.running:
            try:
                message = await self.pubsub.listen(timeout=1)
            except queue.Empty:
                pass
            except Exception as e:
                self.logger.exception(
                    f"Error while reading from queue. Error msg: {e.args}"
                )
            else:
                yield message

    async def initialize(self) -> None:
        pass
