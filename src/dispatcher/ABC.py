from __future__ import annotations

import asyncio
from collections.abc import Callable
import inspect
import logging
from threading import Event, Thread
from typing import AsyncIterable, Iterable
import uuid

from .context_var_wrapper import ContextVarWrapper
from .event_handler import AsyncEventHandler, EventHandler
from .exceptions import StopEvent, UnknownEvent
from .serializer import Serializer


data_type: dict | list | str | tuple | None


STOP_SIGNAL = "__STOP__"

context = ContextVarWrapper()


class Dispatcher:
    asyncio_based = False

    def __init__(
            self,
            namespace: str,
            parent_logger: logging.Logger = None
    ) -> None:
        """Base class for a python-socketio inspired event dispatcher.

        :param namespace: The namespace events will be sent from and to.
        :param parent_logger: A logging.Logger instance. The dispatcher logger
                              will be set to 'parent_logger.namespace'.
        """
        logger = None
        if parent_logger:
            logger = parent_logger.getChild(namespace)
        self.namespace = namespace.strip("/")
        self.logger = logger or logging.getLogger(f"dispatcher.{namespace}")
        self.host_uid = uuid.uuid4().hex
        self.rooms = set()
        self.rooms.add(self.host_uid)
        self._running = Event()
        self.event_handlers: set[EventHandler] = set()
        self.handlers: dict[str: Callable] = {}
        self._fallback = None
        self._sessions = {}

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.namespace}, running={self.running})>"

    @property
    def running(self):
        return self._running.is_set()

    def _publish(self, namespace: str, payload: bytes,
                 ttl: int | None = None) -> None:
        """Publish the payload to the namespace."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    def _listen(self) -> Iterable:
        """Get a generator that yields payloads that will be parsed."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    def _format_data(self, data: data_type) -> list:
        if isinstance(data, tuple):
            return list(data)
        if data is None:
            return []
        return [data]

    def _thread(self) -> None:
        while self._running.is_set():
            try:
                for payload in self._listen():
                    if isinstance(payload, dict):
                        message = payload
                    else:
                        message = Serializer.loads(payload)
                    event = message["event"]
                    room = self.host_uid  # TODO: fix  message.get("room", self.host_uid)
                    if room in self.rooms:
                        sid = message["host_uid"]
                        context.sid = sid
                        data: data_type = message.get("data")
                        data: list = self._format_data(data)
                        self._trigger_event(event, sid, *data)
                        del context.sid
            except StopEvent:
                break
            except Exception as e:
                self.logger.error(
                    f"Encountered an error. Error msg: "
                    f"`{e.__class__.__name__}: {e}`"
                )

    def _stop_signal_handler(self, *args, **kwargs) -> None:
        self._running.clear()

    def _handle_connect(self):
        return self._trigger_event(
            "connect", "sid", {"REMOTE_ADDR": self.namespace}
        )

    def _handle_disconnect(self):
        return self._trigger_event("disconnect", "sid")

    def _get_event_handler(self, event: str):
        if event in self.handlers:
            return self.handlers[event]
        elif self.event_handlers:
            for e in self.event_handlers:
                event_handler = e.get_handler(event)
                if event_handler is not None:
                    return event_handler
        if self._fallback is not None:
            return self._fallback
        raise UnknownEvent(
            f"Received unknown event '{event}' and no fallback function set"
        )

    def _trigger_event(
            self,
            event: str,
            sid: str,
            *args,
    ) -> None:
        try:
            if event == STOP_SIGNAL:
                self._stop_signal_handler()
                raise StopEvent
            else:
                event_handler = self._get_event_handler(event)
                signature = inspect.signature(event_handler)
                if "sid" in signature.parameters:
                    return event_handler(sid, *args)
                return event_handler(*args)
        except StopEvent:
            raise StopEvent
        except UnknownEvent:
            if event not in {"connect", "disconnect"}:
                self.logger.warning(f"No event '{event}' configured")
        except Exception as e:
            self.logger.error(
                f"Encountered an error while handling event '{event}'. Error "
                f"msg: `{e.__class__.__name__}: {e}`"
            )

    """
    API calls
    """
    def initialize(self) -> None:
        """Method to call other methods just before starting the background thread.
        """
        pass

    def generate_payload(
            self,
            event: str,
            room: str | None = None,
            data: data_type = None,
    ) -> dict:
        payload = {"event": event, "host_uid": self.host_uid}
        if room:
            payload.update({"room": room})
        if data:
            payload.update({"data": data})
        return payload

    @property
    def fallback(self) -> Callable:
        return self._fallback

    @fallback.setter
    def fallback(self, fct: Callable = None) -> None:
        """Set the fallback function that will be called if no event handler
        is found.
        """
        self._fallback = fct

    def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        self.rooms.add(room)

    def leave_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        if room in self.rooms:
            self.rooms.remove(room)

    def session(self, sid: str, namespace: str | None = None):
        class _session_ctx_manager:
            def __init__(self, dispatcher, _sid, _namespace):
                self.dispatcher = dispatcher
                self.sid = sid
                self.namespace = namespace.strip("/")
                self.session = None

            def __enter__(self):
                self.session = self.dispatcher._sessions.get(sid, {})
                return self.session

            def __exit__(self, *args):
                self.dispatcher._sessions[sid] = self.session

        return _session_ctx_manager(self, sid, namespace)

    def disconnect(self, sid: str, namespace: str | None = None) -> None:
        pass  # TODO

    def register_event_handler(self, event_handler: EventHandler) -> None:
        """Register an event handler."""
        if event_handler.asyncio_based:
            raise RuntimeError(
                f"{self.__class__.__name__} requires a synchronous EventHandler"
            )
        event_handler._set_dispatcher(self)
        self.event_handlers.add(event_handler)

    def on(self, event: str, handler: Callable = None):
        """Register an event handler

        :param event: The event name.
        :param handler: The method that will be used to handle the event. When
                        skipped, this method acts as a decorator.

        Example:
            - As a method
            def event_handler(sender_uid, data):
                print(data)
            dispatcher.on("my_event", handler=event_handler)

            - As a decorator
            @dispatcher.on("my_event")
            def event_handler(sender_uid, data):
                print(data)

            rem: sender_uid will always be the first argument. It can be used
            to emit an event back to the sender
        """
        def set_handler(_handler: Callable):
            self.handlers[event] = _handler
            return _handler

        if handler is None:
            return set_handler
        set_handler(handler)

    def emit(
            self,
            event: str,
            data: data_type = None,
            to: str | None = None,
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
        if isinstance(namespace, str):
            namespace = namespace.strip("/")
        namespace = namespace or "root"
        room = to or room
        payload: dict = self.generate_payload(event, room, data)
        payload: bytes = Serializer.dumps(payload)
        self._publish(namespace, payload, ttl)

    def start_background_task(self, target: Callable, *args) -> Thread:
        """Override to use another threading method"""
        t = Thread(target=target, args=args)
        t.start()
        if not hasattr(self, "threads"):
            self.threads = {}
        self.threads[target.__name__] = t
        return t

    def start(self) -> None:
        """Start to dispatch events."""
        if self._running.is_set():
            return
        self.initialize()
        self._running.set()
        self._handle_connect()
        self.start_background_task(target=self._thread)

    def stop(self) -> None:
        """Stop to dispatch events."""
        self.emit(STOP_SIGNAL, room=self.host_uid, namespace=self.namespace)
        for thread in self.threads:
            self.threads[thread].join()


class AsyncDispatcher(Dispatcher):
    asyncio_based = True

    def __init__(
            self,
            namespace: str,
            parent_logger: logging.Logger = None
    ) -> None:
        super().__init__(namespace, parent_logger)
        self._running = asyncio.Event()

    async def _publish(self, namespace: str, payload: bytes,
                       ttl: int | None = None) -> None:
        """Publish the payload to the namespace."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    async def _listen(self) -> AsyncIterable:
        """Get a generator that yields payloads that will be parsed."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    async def _thread(self) -> None:
        while self._running.is_set():
            try:
                async for payload in self._listen():
                    if isinstance(payload, dict):
                        message = payload
                    else:
                        message = Serializer.loads(payload)
                    event = message["event"]
                    room = self.host_uid  # TODO: fix  message.get("room", self.host_uid)
                    if room in self.rooms:
                        sid = message["host_uid"]
                        context.sid = sid
                        data: data_type = message.get("data")
                        data: list = self._format_data(data)
                        await self._trigger_event(event, sid, *data)
                        del context.sid
            except StopEvent:
                break
            except Exception as e:
                self.logger.error(
                    f"Encountered an error. Error msg: "
                    f"`{e.__class__.__name__}: {e}`"
                )

    async def _handle_connect(self):
        return await self._trigger_event(
            "connect", "sid", {"REMOTE_ADDR": self.namespace}
        )

    async def _handle_disconnect(self):
        return await self._trigger_event("disconnect", "sid")

    async def _trigger_event(
            self,
            event: str,
            sid: str,
            *args,
    ) -> None:
        try:
            if event == STOP_SIGNAL:
                self._stop_signal_handler()
                raise StopEvent
            else:
                event_handler = self._get_event_handler(event)
                signature = inspect.signature(event_handler)
                need_sid = "sid" in signature.parameters
                if asyncio.iscoroutinefunction(event_handler) is True:
                    try:
                        if need_sid:
                            return await event_handler(sid, *args)
                        return await event_handler(*args)
                    except asyncio.CancelledError:
                        return None
                else:
                    if need_sid:
                        return event_handler(sid, *args)
                    return event_handler(*args)
        except StopEvent:
            raise StopEvent
        except UnknownEvent:
            if event not in {"connect", "disconnect"}:
                self.logger.warning(f"No event '{event}' configured")
        except Exception as e:
            self.logger.error(
                f"Encountered an error while handling event '{event}'. Error "
                f"msg: `{e.__class__.__name__}: {e}`"
            )

    async def initialize(self) -> None:
        """Method to call other methods just before starting the background thread.
        """
        pass

    def session(self, sid: str, namespace: str | None = None):
        class _session_ctx_manager:
            def __init__(self, dispatcher, _sid, _namespace):
                self.dispatcher = dispatcher
                self.sid = sid
                self.namespace = namespace.strip("/")
                self.session = None

            async def __aenter__(self):
                self.session = self.dispatcher._sessions.get(sid, {})
                return self.session

            async def __aexit__(self, *args):
                self.dispatcher._sessions[sid] = self.session

        return _session_ctx_manager(self, sid, namespace)

    async def disconnect(self, sid: str, namespace: str | None = None) -> None:
        pass  # TODO

    def register_event_handler(self, event_handler: AsyncEventHandler) -> None:
        """Register an event handler."""
        if not event_handler.asyncio_based:
            raise RuntimeError(
                f"{self.__class__.__name__} requires an AsyncEventHandler"
            )
        event_handler._set_dispatcher(self)
        self.event_handlers.add(event_handler)

    async def emit(
            self,
            event: str,
            data: data_type = None,
            to: str | None = None,
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
        if isinstance(namespace, str):
            namespace = namespace.strip("/")
        namespace = namespace or "root"
        room = to or room
        payload: dict = self.generate_payload(event, room, data)
        payload: bytes = Serializer.dumps(payload)
        await self._publish(namespace, payload, ttl)

    def start_background_task(self, target: Callable, *args, **kwargs):
        """Override to use another threading method"""
        loop = kwargs.pop("loop", None)
        return asyncio.ensure_future(target(*args, **kwargs), loop=loop)

    def start(self, loop=None) -> None:
        """Start to dispatch events."""
        if self._running.is_set():
            return

        async def inner_fct():
            await self.initialize()
            self._running.set()
            await self._handle_connect()
            self.start_background_task(self._thread, loop=loop)

        asyncio.ensure_future(inner_fct())

    def stop(self) -> None:
        """Stop to dispatch events."""
        asyncio.ensure_future(
            self.emit(STOP_SIGNAL, room=self.host_uid, namespace=self.namespace)
        )
