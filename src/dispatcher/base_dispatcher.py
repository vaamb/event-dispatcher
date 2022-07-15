import logging

from .template import DispatcherTemplate
from ._pubsub import StupidPubSub


class BaseDispatcher(DispatcherTemplate):
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
            pubsub: StupidPubSub = None,
            parent_logger: logging.Logger = None
    ) -> None:
        if pubsub:
            self.pubsub = pubsub
        else:
            self.pubsub = StupidPubSub()
        super(BaseDispatcher, self).__init__(namespace, parent_logger)

    def _start(self) -> None:
        self._trigger_event("connect")

    def _parse_payload(self, payload: dict) -> dict:
        return payload

    def _publish(self, namespace: str, payload: dict) -> int:
        return self.pubsub.publish(namespace, payload)

    def _listen(self):
        self.pubsub.subscribe(self.namespace)
        for message in self.pubsub.listen():
            yield message
