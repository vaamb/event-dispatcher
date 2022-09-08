from __future__ import annotations

import asyncio
from collections.abc import Callable
import logging
from threading import Event, Thread
import uuid

from .context_var_wrapper import ContextVarWrapper
from .event_handler import EventHandler
from .exceptions import StopEvent, UnknownEvent


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
        self.namespace = namespace
        self.logger = logger or logging.getLogger(f"dispatcher.{namespace}")
        self.host_uid = uuid.uuid4().hex
        self.rooms = set()
        self.rooms.add(self.host_uid)
        self._running = Event()
        self.event_handlers: set[EventHandler] = set()
        self.handlers: dict[str: Callable] = {}
        self._fallback = None

    def initialize(self) -> None:
        """Method to call other methods just before starting the background thread.
        """
        pass

    def _parse_payload(self, payload: dict) -> dict:
        """Method to parse the payload in case it was serialized before
        publishing.
        """
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    def _publish(self, namespace: str, payload: dict) -> None:
        """Publish the payload to the namespace."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    def _listen(self):
        """Get a generator that yields payloads that will be parsed."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    def generate_payload(self, event, room=None, *args, **kwargs) -> dict:
        payload = {"event": event, "host_uid": self.host_uid}
        if room:
            payload.update({"room": room})
        if args:
            payload.update({"args": args})
        if kwargs:
            payload.update({"kwargs": kwargs})
        return payload

    def _thread(self) -> None:
        while self._running.is_set():
            try:
                for payload in self._listen():
                    message = self._parse_payload(payload)
                    event = message["event"]
                    room = message.get("room", self.host_uid)
                    if room in self.rooms:
                        remote_host_uid = message["host_uid"]
                        args = message.get("args", ())
                        kwargs = message.get("kwargs", {})
                        context.sid = remote_host_uid
                        self._trigger_event(event, *args, **kwargs)
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
        raise StopEvent

    def _trigger_event(
            self,
            event: str,
            *args,
            **kwargs
    ) -> None:
        try:
            if event == STOP_SIGNAL:
                return self._stop_signal_handler()
            elif event in self.handlers:
                return self.handlers[event](*args, **kwargs)
            elif self.event_handlers:
                for e in self.event_handlers:
                    event_return = e.trigger_event(event, *args, **kwargs)
                    if event_return != "__not_triggered__":
                        return event_return
            if self._fallback:
                return self._fallback(*args, **kwargs)
            else:
                raise UnknownEvent(
                    f"Received unknown event '{event}' and no fallback function set"
                )
        except Exception as e:
            self.logger.debug(
                f"Encountered an error while handling event '{event}'. Error "
                f"msg: `{e.__class__.__name__}: {e}`"
            )

    """
    API calls
    """
    @property
    def fallback(self) -> Callable:
        return self._fallback

    @fallback.setter
    def fallback(self, fct: Callable = None) -> None:
        """Set the fallback function that will be called if no event handler
        is found.
        """
        self._fallback = fct

    def join_room(self, room: str) -> None:
        self.rooms.add(room)

    def leave_room(self, room: str) -> None:
        if room in self.rooms:
            self.rooms.remove(room)

    def register_event_handler(self, event_handler: EventHandler) -> None:
        """Register an event handler."""
        if event_handler.asyncio_based:
            raise RuntimeError(
                f"{self.__class__.__name__} requires a synchronous EventHandler"
            )
        event_handler._set_dispatcher(self)
        self.event_handlers.add(event_handler)

    def on(self, event: str, handler: Callable = None) -> None:
        """Register an event handler

        :param event: The event name.
        :param handler: The method that will be used to handle the event. When
                        skipped, the method acts as a decorator.

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
        def set_handler(handler: Callable):
            self.handlers[event] = handler
            return handler

        if handler is None:
            return set_handler
        set_handler(handler)

    def emit(
            self,
            namespace: list | str | tuple,
            event: str,
            room: str | None = None,
            *args,
            **kwargs
    ) -> None:
        """Emit an event to a single or multiple namespace(s)

        :param namespace: The namespace(s) to which the event will be sent.
        :param event: The event name.
        :param room: The room to which the event should be sent. By default it
                      will be sent to all rooms.
        :param args: Optionnal arguments to be passed to the event handler.
        :param kwargs: Optionnal key word arguments to be passed to the event
                       handler.
        """
        if isinstance(namespace, str):
            namespace = namespace.split(",")
        payload = self.generate_payload(event, room, *args, **kwargs)
        for n in namespace:
            self._publish(n, payload)

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
        self._running.set()
        self.initialize()
        self.start_background_task(target=self._thread)

    def stop(self) -> None:
        """Stop to dispatch events."""
        self.emit(self.namespace, STOP_SIGNAL)
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

    def _parse_payload(self, payload: dict) -> dict:
        """Method to parse the payload in case it was serialized before
        publishing.
        """
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    async def _publish(self, namespace: str, payload: dict) -> None:
        """Publish the payload to the namespace."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    async def _listen(self):
        """Get a generator that yields payloads that will be parsed."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    async def _thread(self) -> None:
        while self._running.is_set():
            try:
                for payload in await self._listen():
                    message = self._parse_payload(payload)
                    event = message["event"]
                    room = message.get("room", self.host_uid)
                    if room in self.rooms:
                        remote_host_uid = message["host_uid"]
                        args = message.get("args", ())
                        kwargs = message.get("kwargs", {})
                        context.sid = remote_host_uid
                        await self._trigger_event(event, *args, **kwargs)
                        del context.sid
            except StopEvent:
                break
            except Exception as e:
                self.logger.error(
                    f"Encountered an error. Error msg: "
                    f"`{e.__class__.__name__}: {e}`"
                )

    async def _trigger_event(
            self,
            event: str,
            *args,
            **kwargs
    ) -> None:
        try:
            if event == STOP_SIGNAL:
                return self._stop_signal_handler()
            elif event in self.handlers:
                return await self.handlers[event](*args, **kwargs)
            elif self.event_handlers:
                for e in self.event_handlers:
                    event_return = e.trigger_event(event, *args, **kwargs)
                    if event_return != "__not_triggered__":
                        return await event_return
            if self._fallback:
                return self._fallback(*args, **kwargs)
            else:
                raise UnknownEvent(
                    f"Received unknown event '{event}' and no fallback function set"
                )
        except Exception as e:
            self.logger.debug(
                f"Encountered an error while handling event '{event}'. Error "
                f"msg: `{e.__class__.__name__}: {e}`"
            )

    def register_event_handler(self, event_handler: EventHandler) -> None:
        """Register an event handler."""
        if not event_handler.asyncio_based:
            raise RuntimeError(
                f"{self.__class__.__name__} requires an AsyncEventHandler"
            )
        event_handler._set_dispatcher(self)
        self.event_handlers.add(event_handler)

    async def emit(
            self,
            namespace: list | str | tuple,
            event: str,
            room: str = None,
            *args,
            **kwargs
    ) -> None:
        """Emit an event to a single or multiple namespace(s)

        :param namespace: The namespace(s) to which the event will be sent.
        :param event: The event name.
        :param room: The room to which the event should be sent. By default it
                      will be sent to all rooms.
        :param args: Optionnal arguments to be passed to the event handler.
        :param kwargs: Optionnal key word arguments to be passed to the event
                       handler.
        """
        if isinstance(namespace, str):
            namespace = namespace.split(",")
        payload = self.generate_payload(event, room, *args, **kwargs)
        for n in namespace:
            await self._publish(n, payload)

    async def start_background_task(self, target: Callable, *args, **kwargs):
        """Override to use another threading method"""
        return asyncio.ensure_future(target(*args, **kwargs))

    async def start(self) -> None:
        """Start to dispatch events."""
        if self._running.is_set():
            return
        self._running.set()
        self.initialize()
        await self.start_background_task(self._thread)
        loop = asyncio.get_event_loop()
        if not loop.is_running():
            loop.run_forever()

    async def stop(self) -> None:
        """Stop to dispatch events."""
        await self.emit(self.namespace, STOP_SIGNAL)
