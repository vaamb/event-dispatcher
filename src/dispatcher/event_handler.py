from __future__ import annotations

from abc import ABC, abstractmethod
import inspect
from typing import Callable, Generic, Hashable, TypeVar
from uuid import UUID

from .ABC import AsyncDispatcher, BaseDispatcher, DataType, Dispatcher, EMPTY


DispatcherT = TypeVar("DispatcherT", bound=BaseDispatcher)


class BaseEventHandler(ABC, Generic[DispatcherT]):
    asyncio_based: bool

    """Base class for class-based event handler.

    A class-based event-handler is a class that contains methods to handle the
    events for a dispatcher.
    """
    def __init__(self, namespace: str = "event_dispatcher", **kwargs) -> None:
        super().__init__(**kwargs)
        namespace = namespace.strip("/")
        self.namespace = namespace
        self._dispatcher: DispatcherT | None = None
        self._handlers: dict[str, tuple[Callable, bool] | None] = {}

    @abstractmethod
    def _set_dispatcher(self, dispatcher: DispatcherT) -> None: ...

    @property
    def dispatcher(self) -> DispatcherT:
        assert self._dispatcher is not None
        return self._dispatcher

    def enter_room(self, room: str) -> None:
        self.dispatcher.enter_room(room)

    def leave_room(self, room: str) -> None:
        self.dispatcher.leave_room(room)

    def _get_handler(self, event: str) -> tuple[Callable, bool] | None:
        handler_name = f"on_{event}"
        if handler_name not in self._handlers:
            if hasattr(self, handler_name):
                event_handler = getattr(self, handler_name)
                # Check if the handler expects a 'sid' parameter
                signature = inspect.signature(event_handler)
                need_sid = "sid" in signature.parameters
                self._handlers[handler_name] = (event_handler, need_sid)
            else:
                self._handlers[handler_name] = None
        return self._handlers[handler_name]

    @abstractmethod
    def disconnect(self, sid: str | UUID) -> None: ...

    @abstractmethod
    def emit(
            self,
            event: str,
            data: DataType = EMPTY,
            to: UUID | None = None,
            room: str | None = None,
            namespace: str | None = None,
            ttl: int | None = None,
            **kwargs
    ) -> bool: ...


class EventHandler(BaseEventHandler[Dispatcher]):
    asyncio_based = False

    def _set_dispatcher(self, dispatcher: Dispatcher) -> None:
        if not isinstance(dispatcher, Dispatcher):
            raise RuntimeError(
                "dispatcher must be an instance of Dispatcher class"
            )
        self._dispatcher = dispatcher

    def session(self, sid: Hashable):
        return self.dispatcher.session(sid)

    def disconnect(self, sid: str | UUID) -> None:
        if isinstance(sid, str):
            sid = UUID(sid)
        self.dispatcher.disconnect(sid)

    def emit(
            self,
            event: str,
            data: DataType = EMPTY,
            to: UUID | None = None,
            room: str | None = None,
            namespace: str | None = None,
            ttl: int | None = None,
            **kwargs
    ) -> bool:
        """Emit an event to a single or multiple namespace(s)

        :param event: The event name.
        :param data: The data to send to the required dispatcher.
        :param to: The recipient of the message.
        :param room: An alias to `to`
        :param namespace: The namespace to which the event will be sent.
        :param ttl: Time to live of the message. Only available with rabbitmq

        :return: True for success, False for failure
        """
        if self._dispatcher is None:
            raise RuntimeError(
                "You need to register this EventHandler in order to use it"
            )
        if isinstance(namespace, str):
            namespace = namespace.strip("/")
        namespace = namespace or self.namespace
        room = to.hex if to is not None else room
        return self.dispatcher.emit(event, data, to, room, namespace, ttl, **kwargs)


class AsyncEventHandler(BaseEventHandler[AsyncDispatcher]):
    asyncio_based = True

    def _set_dispatcher(self, dispatcher: AsyncDispatcher) -> None:
        if not isinstance(dispatcher, AsyncDispatcher):
            raise RuntimeError(
                "dispatcher must be an instance of AsyncDispatcher class"
            )
        self._dispatcher = dispatcher

    def session(self, sid: Hashable):
        return self.dispatcher.session(sid)

    async def disconnect(self, sid: str | UUID) -> None:  # ty: ignore[invalid-method-override]
        if isinstance(sid, str):
            sid = UUID(sid)
        await self.dispatcher.disconnect(sid)

    async def emit(  # ty: ignore[invalid-method-override]
            self,
            event: str,
            data: DataType = EMPTY,
            to: UUID | None = None,
            room: str | None = None,
            namespace: str | None = None,
            ttl: int | None = None,
            **kwargs
    ) -> bool:
        """Emit an event to a single or multiple namespace(s)

        :param event: The event name.
        :param data: The data to send to the required dispatcher.
        :param to: The recipient of the message.
        :param room: An alias to `to`
        :param namespace: The namespace to which the event will be sent.
        :param ttl: Time to live of the message. Only available with rabbitmq

        :return: True for success, False for failure
        """
        if self._dispatcher is None:
            raise RuntimeError(
                "You need to register this EventHandler in order to use it"
            )
        if isinstance(namespace, str):
            namespace = namespace.strip("/")
        namespace = namespace or self.namespace
        room = to.hex if to is not None else room
        resp = await self.dispatcher.emit(event, data, to, room, namespace, ttl, **kwargs)
        return resp
