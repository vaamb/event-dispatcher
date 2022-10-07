from __future__ import annotations

import typing as t
from typing import Any

from .exceptions import UnknownEvent


if t.TYPE_CHECKING:
    from .ABC import AsyncDispatcher, Dispatcher


data_type: dict | list | str | tuple | None


class EventHandler:
    asyncio_based = False

    """Base class for class-based event handler.

    A class-based event-handler is a class that contains methods to handle the
    events for a dispatcher.
    """
    def __init__(self, namespace: str = "root") -> None:
        super().__init__()
        self.namespace = namespace
        self._dispatcher: AsyncDispatcher | Dispatcher | None = None

    def __eq__(self, other) -> bool:
        return self.__dict__.keys() == other.__dict__.keys()

    def __hash__(self):
        return hash(tuple(self.__dict__.keys()))

    def _set_dispatcher(self, dispatcher: Dispatcher) -> None:
        if dispatcher.asyncio_based:
            raise RuntimeError(
                "dispatcher must be an instance of Dispatcher class"
            )
        self._dispatcher: Dispatcher = dispatcher

    def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        namespace = namespace or self.namespace
        self._dispatcher.enter_room(sid, room, namespace)

    def leave_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        namespace = namespace or self.namespace
        self._dispatcher.leave_room(sid, room, namespace)

    def session(self, sid: str, namespace: str | None = None):
        namespace = namespace or self.namespace
        return self._dispatcher.session(sid, namespace)

    def disconnect(self, sid: str, namespace: str | None = None) -> None:
        namespace = namespace or self.namespace
        self._dispatcher.disconnect(sid, namespace)

    def get_handler(self, event: str):
        handler = f"on_{event}"
        if hasattr(self, handler):
            return getattr(self, handler)
        return None

    def trigger_event(self, event: str, *args, **kwargs):
        """Dispatch an event to the correct handler method.

        :param event: The name of the event to handle.
        """
        handler = self.get_handler(event)
        if handler:
            return handler(*args, **kwargs)
        raise UnknownEvent

    def emit(
            self,
            event: str,
            data: data_type = None,
            to: dict | None = None,
            room: str | None = None,
            namespace: str | None = None,
            ttl: int | None = None,
            **kwargs
    ) -> None:
        """Emit an event to a single or multiple namespace(s)

        :param event: The event name.
        :param data: The data to send to the required dispatcher.
        :param to: The recipient of the message.
        :param room: An alias to `to`
        :param namespace: The namespace to which the event will be sent.
        :param ttl: Time to live of the message. Only available with rabbitmq
        """
        if self._dispatcher is None:
            raise RuntimeError(
                "You need to register this EventHandler in order to use it"
            )
        if isinstance(namespace, str):
            namespace = namespace.strip("/")
        namespace = namespace or self.namespace
        room = to or room
        self._dispatcher.emit(event, data, to, room, namespace, ttl, **kwargs)


class AsyncEventHandler(EventHandler):
    asyncio_based = True

    def _set_dispatcher(self, dispatcher: AsyncDispatcher) -> None:
        if not dispatcher.asyncio_based:
            raise RuntimeError(
                "dispatcher must be an instance of Dispatcher class"
            )
        self._dispatcher: AsyncDispatcher = dispatcher

    def session(self, sid: str, namespace: str | None = None):
        namespace = namespace or self.namespace
        return self._dispatcher.session(sid, namespace)

    async def disconnect(self, sid: str, namespace: str | None = None) -> None:
        namespace = namespace or self.namespace
        await self._dispatcher.disconnect(sid, namespace)

    async def trigger_event(self, event: str, *args, **kwargs):
        """Dispatch an event to the correct handler method.

        :param event: The name of the event to handle.
        """
        handler = self.get_handler(event)
        if handler:
            return await handler(*args, **kwargs)
        raise UnknownEvent

    async def emit(
            self,
            event: str,
            data: data_type = None,
            to: dict | None = None,
            room: str | None = None,
            namespace: str | None = None,
            ttl: int | None = None,
            **kwargs
    ) -> None:
        """Emit an event to a single or multiple namespace(s)

        :param event: The event name.
        :param data: The data to send to the required dispatcher.
        :param to: The recipient of the message.
        :param room: An alias to `to`
        :param namespace: The namespace to which the event will be sent.
        :param ttl: Time to live of the message. Only available with rabbitmq
        """
        if self._dispatcher is None:
            raise RuntimeError(
                "You need to register this EventHandler in order to use it"
            )
        if isinstance(namespace, str):
            namespace = namespace.strip("/")
        namespace = namespace or self.namespace
        room = to or room
        await self._dispatcher.emit(event, data, to, room, namespace, ttl, **kwargs)
