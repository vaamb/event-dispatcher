from __future__ import annotations

import asyncio
from asyncio import Task
from collections.abc import Callable
from functools import cached_property
import inspect
import logging
from threading import Event, Thread
import time
from typing import AsyncIterator, Iterator, TypedDict
import uuid

from .context_var_wrapper import ContextVarWrapper
from .event_handler import AsyncEventHandler, EventHandler
from .exceptions import StopEvent, UnknownEvent
from .serializer import Serializer


DataType: bytes | dict | list | str | tuple | None


class PayloadDict(TypedDict):
    host_uid: str
    event: str
    room: str | None
    data: dict | list | str | tuple | None


STOP_SIGNAL = "__STOP__"

context = ContextVarWrapper()


class Dispatcher:
    asyncio_based = False
    _PAYLOAD_SEPARATOR = b"|"
    _DATA_OBJECT = b"\x31"  # 1
    _DATA_BINARY = b"\x32"  # 2

    def __init__(
            self,
            namespace: str,
            parent_logger: logging.Logger = None,
            reconnection: bool = True,
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
        self.reconnection = reconnection
        self.host_uid: str = str(uuid.uuid4())
        self.rooms: set[str] = set()
        self.rooms.add(self.host_uid)
        self._running = Event()
        self._connected = Event()
        self._reconnecting = Event()
        self.event_handlers: set[EventHandler] = set()
        self.handlers: dict[str: Callable] = {}
        self._fallback = None
        self._sessions = {}
        self._threads: dict[str, Thread] | None = {}

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}({self.namespace}, "
            f"running={self.running}, connected={self.connected})>"
        )

    @property
    def threads(self) -> dict[str, Thread]:
        if not self.asyncio_based:
            return self._threads
        raise AttributeError("AsyncDispatcher do not have threads")

    # Methods to implement based on broker used
    def _broker_reachable(self) -> bool:
        """Check if it is possible to connect to the broker."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    def _publish(self, namespace: str, payload: bytes,
                 ttl: int | None = None) -> None:
        """Publish the payload to the namespace."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    def _listen(self) -> Iterator[bytes]:
        """Get a generator that yields payloads that will be parsed."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    # Handling of broker-connection related events
    def _handle_broker_connect(self) -> None:
        if not self.connected:
            self._trigger_connect_event()
        self._connected.set()

    def _handle_broker_disconnect(self) -> None:
        time.sleep(0)
        if self.connected:
            self._trigger_disconnect_event()
        self._connected.clear()

    def _handle_stop_signal(self, *args, **kwargs) -> None:
        self._handle_broker_disconnect()
        self._running.clear()
        self._connected.clear()
        self._reconnecting.clear()
        for thread in [*self._threads.values()]:
            try:
                thread.join(timeout=0)
            except RuntimeError:  # Trying to close current thread
                pass

    # Payload-related methods
    @cached_property
    def _DATA_OBJECT_SEPARATOR(self) -> bytes:
        return self._PAYLOAD_SEPARATOR + self._DATA_OBJECT

    @cached_property
    def _DATA_BINARY_SEPARATOR(self) -> bytes:
        return self._PAYLOAD_SEPARATOR + self._DATA_BINARY

    def _encode_data(self, data: DataType) -> bytes:
        if type(data) == type(bytes):
            return self._DATA_BINARY_SEPARATOR + data
        else:
            return self._DATA_OBJECT_SEPARATOR + Serializer.dumps(data)

    def _generate_payload(
            self,
            event: str,
            room: str | None = None,
            data: DataType = None,
    ) -> bytes:
        return (
            Serializer.dumps({
                "host_uid": self.host_uid,
                "event": event,
                "room": room
            }) +
            self._encode_data(data)
        )

    @classmethod
    def _decode_data(cls, data: bytes) -> DataType:
        data_type = data[:1]
        if data_type == cls._DATA_OBJECT:
            return Serializer.loads(data[1:])
        elif data_type == cls._DATA_BINARY:
            return data[1:]
        else:
            raise ValueError("Unknown type of data")

    def _parse_payload(self, payload: bytes) -> PayloadDict:
        base_info, base_data = payload.split(self._PAYLOAD_SEPARATOR, maxsplit=2)
        info = Serializer.loads(base_info)
        return PayloadDict(
            host_uid=info["host_uid"],
            event=info["event"],
            room=(info["room"] if info["room"] is not None else self.host_uid),
            data=self._decode_data(base_data)
        )

    def _data_as_list(self, data: DataType) -> list:
        if isinstance(data, tuple):
            return list(data)
        if data is None:
            return []
        return [data]

    # Events triggering
    def _trigger_connect_event(self) -> None:
        return self._trigger_event(
            "connect", "sid", {"REMOTE_ADDR": self.namespace})

    def _trigger_disconnect_event(self) -> None:
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
            f"Received unknown event '{event}' and no fallback function set")

    def _trigger_event(
            self,
            event: str,
            sid: str,
            *args,
    ) -> None:
        try:
            if event == STOP_SIGNAL:
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

    # Loops running once `run()` is called
    def _reconnection_loop(self) -> None:
        self._connected.clear()
        self._reconnecting.set()
        retry_sleep = 1
        self.logger.info("Starting the reconnection loop in 1 sec")
        time.sleep(retry_sleep)
        while self.running and self.reconnecting:
            self.logger.info(
                    f"Attempting to reconnect to the message broker")
            connected = self._broker_reachable()
            if connected:
                self.logger.info(f"Reconnection successful")
                self._handle_broker_connect()
                self._reconnecting.clear()
                break
            else:
                self.logger.info(
                    f"Reconnection attempt failed. Retrying in {retry_sleep} s")
                time.sleep(retry_sleep)
                retry_sleep *= 2
                if retry_sleep > 60:
                    retry_sleep = 60

    def _listen_loop(self) -> None:
        self.logger.info("Starting the listening loop")
        while self.running and self.connected:
            try:
                self.logger.info("Waiting for messages")
                for payload in self._listen():
                    message: PayloadDict = self._parse_payload(payload)
                    event: str = message["event"]
                    self.logger.debug(f"Received event '{event}'")
                    room: str = message["room"]
                    if room in self.rooms:
                        sid: str = message["host_uid"]
                        context.sid = sid
                        data: DataType = message["data"]
                        data: list = self._data_as_list(data)
                        # User-defined functions should not crash the whole listening loop
                        try:
                            self._trigger_event(event, sid, *data)
                        except StopEvent:
                            self.emit(
                                STOP_SIGNAL, room=room, namespace=self.namespace,
                                ttl=5)
                            raise
                        except Exception as e:
                            self.logger.error(
                                f"Encountered an error when trying to trigger "
                                f"event {event}. Error msg: "
                                f"`{e.__class__.__name__}: {e}`")
                        finally:
                            del context.sid
                    time.sleep(0)
            except StopEvent:
                raise
            except ConnectionError:
                self._handle_broker_disconnect()
                raise

    def _master_loop(self) -> None:
        while self.running:
            try:
                self._listen_loop()
            except ConnectionError:
                # Try to reconnect if needed
                if self.reconnection:
                    self.logger.warning("Connection lost, will try to reconnect")
                    self._reconnection_loop()
                else:
                    self.logger.warning("Connection lost, stopping")
                    raise StopEvent
            except StopEvent:
                self._handle_stop_signal()
                break

    """
    API calls
    """
    @property
    def running(self):
        return self._running.is_set()

    @property
    def connected(self):
        return self._connected.is_set()

    @property
    def reconnecting(self):
        return not self.connected and self._reconnecting.is_set()

    def initialize(self) -> None:
        """Method to call other methods just before starting the background thread.
        """
        pass

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
            data: DataType = None,
            to: str | None = None,
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
        if isinstance(namespace, str):
            namespace = namespace.strip("/")
        namespace = namespace or "root"
        room = to or room
        payload: bytes = self._generate_payload(event, room, data)
        try:
            self._publish(namespace, payload, ttl)
            return True
        except ConnectionError:
            return False

    def start_background_task(
            self,
            target: Callable,
            *args,
            task_name: str | None = None,
            **kwargs
    ) -> Thread:
        """Override to use another threading method"""
        task_name = task_name or f"dispatcher-{target.__name__}"
        task = self._threads.get(task_name)
        if task:
            if task.is_alive():
                raise ValueError(
                    f"A background task named {task_name} is already running")
        t = Thread(
            target=target,
            args=args,
            name=task_name)
        t.start()
        self._threads[task_name] = t
        return t

    def connect(self, retry: bool = False, wait: bool = True):
        """Connect to the event dispatcher broker.

        :param retry: Retry to connect if the initial connection attempt failed.
        :param wait: In case the dispatcher tries to reconnect after a failed
                     initial attempt, block until the connection is made.
        """
        if self.connected:
            raise RuntimeError("Already connected")
        self.initialize()
        connected = self._broker_reachable()
        if connected:
            self._handle_broker_connect()
        else:
            if retry:
                if wait:
                    self._reconnection_loop()
                else:
                    self.start_background_task(target=self._reconnection_loop)
            else:
                raise ConnectionError("Cannot connect to the broker")

    def wait(self) -> None:
        """Wait until the connection is lost and reconnection is not attempted
        or the process is explicitly stopped with `stop()`."""
        while self.running:
            time.sleep(1)

    def run(self, block: bool = False) -> None:
        """Run the main loop that listens to new messages coming from the
         broker and triggers the registered events."""
        if self.running:
            raise RuntimeError("Already running")
        self._running.set()
        if block:
            return self._master_loop()
        else:
            self.start_background_task(target=self._master_loop)

    def start(self, retry: bool = False, block: bool = True) -> None:
        """Start to dispatch and receive events."""
        def wrap():
            self.connect(retry=retry, wait=True)
            self.run(block=True)

        if block:
            wrap()
        else:
            self.start_background_task(target=wrap)

    def stop(self) -> None:
        """Stop to dispatch events."""
        self.emit(
            STOP_SIGNAL, room=str(self.host_uid), namespace=self.namespace,
            ttl=15)


class AsyncDispatcher(Dispatcher):
    asyncio_based = True

    def __init__(
            self,
            namespace: str,
            parent_logger: logging.Logger = None,
            reconnection: bool = True,
    ) -> None:
        super().__init__(namespace, parent_logger, reconnection)
        self._running = asyncio.Event()
        self._connected = asyncio.Event()
        self._reconnecting = asyncio.Event()
        self._tasks: dict[str, Task] | None = {}

    async def _broker_reachable(self) -> bool:
        """Check if it is possible to connect to the broker."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    async def _publish(self, namespace: str, payload: bytes,
                       ttl: int | None = None) -> None:
        """Publish the payload to the namespace."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    async def _listen(self) -> AsyncIterator[bytes]:
        """Get a generator that yields payloads that will be parsed."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    async def _handle_broker_connect(self) -> None:
        if not self.connected:
            await self._trigger_connect_event()
        self._connected.set()

    async def _handle_broker_disconnect(self) -> None:
        await asyncio.sleep(0)
        if self.connected:
            await self._trigger_disconnect_event()
        self._connected.clear()

    async def _handle_stop_signal(self, *args, **kwargs) -> None:
        await self._handle_broker_disconnect()
        self._running.clear()
        self._connected.clear()
        self._reconnecting.clear()
        for task in [*self._tasks.values()]:
            try:
                task.cancel()
            except RuntimeError:
                pass

    # Events triggering
    async def _trigger_connect_event(self) -> None:
        return await self._trigger_event(
            "connect", "sid", {"REMOTE_ADDR": self.namespace})

    async def _trigger_disconnect_event(self) -> None:
        return await self._trigger_event("disconnect", "sid")

    async def _trigger_event(
            self,
            event: str,
            sid: str,
            *args,
    ) -> None:
        try:
            if event == STOP_SIGNAL:
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

    # Tasks running once `run()` is called
    async def _reconnection_loop(self) -> None:
        self._connected.clear()
        self._reconnecting.set()
        retry_sleep = 1
        self.logger.info("Starting the reconnection loop in 1 sec")
        await asyncio.sleep(retry_sleep)
        while self.running and self.reconnecting:
            self.logger.info(
                    f"Attempting to reconnect to the message broker")
            connected = await self._broker_reachable()
            if connected:
                self.logger.info(f"Reconnection successful")
                await self._handle_broker_connect()
                self._reconnecting.clear()
                break
            else:
                self.logger.info(
                    f"Reconnection attempt failed. Retrying in {retry_sleep} s")
                await asyncio.sleep(retry_sleep)
                retry_sleep *= 2
                if retry_sleep > 60:
                    retry_sleep = 60

    async def _listen_loop(self) -> None:
        self.logger.info("Starting the listening loop")
        while self.running and self.connected:
            try:
                self.logger.info("Waiting for messages")
                async for payload in self._listen():
                    message: PayloadDict = self._parse_payload(payload)
                    event: str = message["event"]
                    self.logger.debug(f"Received event '{event}'")
                    room: str = message["room"]
                    if room in self.rooms:
                        sid: str = message["host_uid"]
                        context.sid = sid
                        data: DataType = message["data"]
                        data: list = self._data_as_list(data)
                        # User-defined functions should not crash the whole listening loop
                        try:
                            await self._trigger_event(event, sid, *data)
                        except StopEvent:
                            await self.emit(
                                STOP_SIGNAL, room=room, namespace=self.namespace,
                                ttl=5)
                            raise
                        except Exception as e:
                            self.logger.error(
                                f"Encountered an error when trying to trigger "
                                f"event {event}. Error msg: "
                                f"`{e.__class__.__name__}: {e}`")
                        finally:
                            del context.sid
                    await asyncio.sleep(0)
            except StopEvent:
                raise
            except ConnectionError:
                await self._handle_broker_disconnect()
                raise

    async def _master_loop(self) -> None:
        while self.running:
            try:
                await self._listen_loop()
            except ConnectionError:
                # Try to reconnect if needed
                if self.reconnection:
                    self.logger.warning("Connection lost, will try to reconnect")
                    await self._reconnection_loop()
                else:
                    self.logger.warning("Connection lost, stopping")
                    raise StopEvent
            except StopEvent:
                await self._handle_stop_signal()
                break

    """
    API
    """
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
            data: DataType = None,
            to: str | None = None,
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
        if isinstance(namespace, str):
            namespace = namespace.strip("/")
        namespace = namespace or "root"
        room = to or room
        payload: bytes = self._generate_payload(event, room, data)
        try:
            await self._publish(namespace, payload, ttl)
            return True
        except ConnectionError:
            return False

    def start_background_task(
            self,
            target: Callable,
            *args,
            task_name: str | None = None,
            **kwargs
    ) -> Task:
        """Override to use another concurrency method"""
        task_name = task_name or f"dispatcher-{target.__name__}"
        task = self._tasks.get(task_name)
        if task:
            if not task.done():
                raise ValueError(
                    f"A background task named {task_name} is already running")
        t = Task(
            target(*args),
            name=task_name)
        self._tasks[task_name] = t
        return t

    async def connect(self, retry: bool = False, wait: bool = True):
        """Connect to the event dispatcher broker.

        :param retry: Retry to connect if the initial connection attempt failed.
        :param wait: In case the dispatcher tries to reconnect after a failed
                     initial attempt, block until the connection is made.
        """
        await self.initialize()
        connected = await self._broker_reachable()
        if connected:
            await self._handle_broker_connect()
        else:
            if retry:
                if wait:
                    await self._reconnection_loop()
                else:
                    self.start_background_task(target=self._reconnection_loop)
            else:
                raise ConnectionError("Cannot connect to the broker")

    async def wait(self) -> None:
        """Wait until the connection is lost and reconnection is not attempted
        or the process is explicitly stopped with `stop()`."""
        while self.running:
            await asyncio.sleep(1)

    async def run(self, block: bool = True) -> None:
        """Run the main loop that listens to new messages coming from the
         broker and triggers the registered events."""
        if self.running:
            raise RuntimeError("Already running")
        self._running.set()
        self.start_background_task(target=self._master_loop)
        if block:
            await self.wait()

    async def start(self, retry: bool = False, block: bool = True) -> None:
        """Start to dispatch and receive events."""
        async def wrap():
            await self.connect(retry=retry, wait=True)
            await self.run(block=True)

        if block:
            await wrap()
        else:
            self.start_background_task(target=wrap)

    async def stop(self) -> None:
        """Stop to dispatch events."""
        await self.emit(
            STOP_SIGNAL, room=str(self.host_uid), namespace=self.namespace,
            ttl=15)
