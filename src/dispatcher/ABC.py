from __future__ import annotations

import asyncio
from asyncio import Task
from collections.abc import Callable
import inspect
import logging
from threading import Event, RLock, Thread
import time
from typing import AsyncIterator, Iterator, TypedDict
import uuid
from uuid import UUID

from .context_var_wrapper import ContextVarWrapper
from .event_handler import AsyncEventHandler, EventHandler
from .exceptions import StopEvent, UnknownEvent
from .serializer import Serializer


DataType: bytes | bytearray | dict | list | str | tuple | None


class PayloadDict(TypedDict):
    host_uid: UUID
    event: str
    room: str | None
    data: dict | list | str | tuple | None


STOP_SIGNAL = "__STOP__"

context = ContextVarWrapper()


class BaseDispatcher:
    asyncio_based: bool
    _PAYLOAD_SEPARATOR: bytes = b"\x1d\x1d"
    _DATA_OBJECT: bytes = b"\x31"  # 1
    _DATA_BINARY: bytes = b"\x32"  # 2

    serializer = Serializer

    def __init__(
            self,
            namespace: str = "event_dispatcher",
            parent_logger: logging.Logger = None,
            reconnection: bool = True,
    ) -> None:
        """Base class for a python-socketio inspired event dispatcher.

        :param namespace: The namespace events will be sent from and to.
        :param parent_logger: A logging.Logger instance. The dispatcher logger
                              will be set to 'parent_logger.namespace'.
        :param reconnection: Whether to attempt reconnection on connection loss.
        """
        logger = None
        if parent_logger:
            logger = parent_logger.getChild(namespace)
        self.namespace: str = namespace.strip("/")
        self.logger: logging.Logger = logger or logging.getLogger(f"dispatcher.{namespace}")
        self.reconnection: bool = reconnection
        self.host_uid: UUID = uuid.uuid4()
        self.rooms: set[str] = {self.host_uid.hex}

        # Event handling
        self.event_handlers: set[EventHandler] = set()
        self.handlers: dict[str, Callable] = {}
        self._fallback: Callable | None = None
        self._sessions: dict = {}

        # State
        self._running: Event | asyncio.Event = Event()
        self._connected: Event | asyncio.Event = Event()
        self._reconnecting: Event | asyncio.Event = Event()
        self._shutdown_event: Event | asyncio.Event = Event()

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}({self.namespace}, "
            f"running={self.running}, connected={self.connected})>"
        )

    @property
    def running(self):
        return self._running.is_set()

    @property
    def connected(self):
        return self._connected.is_set()

    @property
    def reconnecting(self):
        return not self.connected and self._reconnecting.is_set()

    @property
    def stopped(self):
        return self._shutdown_event.is_set()

    # Payload-related methods
    def _encode_data(self, data: DataType) -> bytearray:
        if isinstance(data, (bytes, bytearray)):
            rv = bytearray()
            rv += self._DATA_BINARY
            rv += data
            return rv
        else:
            rv = bytearray()
            rv += self._DATA_OBJECT
            rv += self.serializer.dumps(data)
            return rv

    def _generate_payload(
            self,
            event: str,
            room: str | UUID | None = None,
            data: DataType = None,
    ) -> bytearray:
        if isinstance(room, UUID):
            room = room.hex
        rv = bytearray()
        rv += self.serializer.dumps(
            {
                "host_uid": self.host_uid.hex,
                "event": event,
                "room": room,
            }
        )
        rv += self._PAYLOAD_SEPARATOR
        rv += self._encode_data(data)
        return rv

    def _decode_data(self, data: bytes) -> DataType:
        data_type = data[:1]
        if data_type == self._DATA_OBJECT:
            return self.serializer.loads(data[1:])
        elif data_type == self._DATA_BINARY:
            return data[1:]
        else:
            raise ValueError("Unknown type of data")

    def _parse_payload(self, payload: bytes) -> PayloadDict:
        base_info, base_data = payload.split(self._PAYLOAD_SEPARATOR, maxsplit=1)
        info = self.serializer.loads(base_info)
        return PayloadDict(
            host_uid=UUID(info["host_uid"]),
            event=info["event"],
            room=(info["room"] if info["room"] is not None else self.host_uid.hex),
            data=self._decode_data(base_data)
        )

    def _data_as_list(self, data: DataType) -> list:
        if isinstance(data, tuple):
            return list(data)
        if data is None:
            return []
        return [data]

    # Event handling
    def _get_event_handlers(self) -> set[EventHandler]:
        raise NotImplementedError

    def _get_event_handler(self, event: str) -> Callable:
        """Get the appropriate handler for the given event.

        Args:
            event: The event name to get the handler for.

        Returns:
            The event handler callable.

        Raises:
            UnknownEvent: If no handler is found and no fallback is set.
        """
        # First check direct handlers
        if event in self.handlers:
            return self.handlers[event]

        # Then check event handlers:
        for handler in self._get_event_handlers():
            event_handler = handler.get_handler(event)
            if event_handler is not None:
                return event_handler

        # Finally, use fallback if available
        if self._fallback is not None:
            return self._fallback

        raise UnknownEvent(f"No handler found for event '{event}'")

    @property
    def fallback(self) -> Callable:
        return self._fallback

    @fallback.setter
    def fallback(self, fct: Callable | None = None) -> None:
        """Set the fallback function that will be called if no event handler
        is found.
        """
        self._fallback = fct

    # Rooms management
    def enter_room(self, room: str) -> None:
        self.rooms.add(room)

    def leave_room(self, room: str) -> None:
        if room in self.rooms:
            self.rooms.remove(room)


class Dispatcher(BaseDispatcher):
    asyncio_based: bool = False

    def __init__(
            self,
            namespace: str = "event_dispatcher",
            parent_logger: logging.Logger | None = None,
            reconnection: bool = True,
    ) -> None:
        """Base class for a python-socketio inspired event dispatcher.

        :param namespace: The namespace events will be sent from and to.
        :param parent_logger: A logging.Logger instance. The dispatcher logger
                              will be set to 'parent_logger.namespace'.
        :param reconnection: Whether to attempt reconnection on connection loss.
        """
        super().__init__(namespace, parent_logger, reconnection)

        # Thread management
        self._threads: dict[str, Thread] = {}
        self._threads_lock = RLock()
        self._event_handlers_lock = RLock()
        self._sessions_lock = RLock()

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

    def _publish(
            self,
            namespace: str,
            payload: bytes | bytearray,
            ttl: int | None = None,
            timeout: int | float | None = None,
    ) -> None:
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
        self._shutdown_event.set()
        self._running.clear()
        self._connected.clear()
        self._reconnecting.clear()
        self._stop_threads()

    # Events triggering
    def _get_event_handlers(self) -> set[EventHandler]:
        with self._event_handlers_lock:
            return self.event_handlers

    def _trigger_event(
            self,
            event: str,
            sid: UUID,
            *args,
    ) -> None:
        """Trigger an event with the given arguments.

        Args:
            event: The name of the event to trigger.
            sid: The session ID of the sender.
            *args: Arguments to pass to the event handler.

        Returns:
            The result of the event handler, or None if the event was not handled.

        Raises:
            StopEvent: If the event handler raises StopEvent.
        """
        if event == STOP_SIGNAL:
            raise StopEvent("Received stop signal")

        try:
            event_handler = self._get_event_handler(event)
            signature = inspect.signature(event_handler)

            # Check if the handler expects a 'sid' parameter
            need_sid = "sid" in signature.parameters

            # Prepare arguments
            handler_args = (sid, *args) if need_sid else args

            # Call the handler
            event_handler(*handler_args)

        except UnknownEvent as e:
            if event not in {"connect", "disconnect"}:
                self.logger.warning(f"No handler for event '{event}': {e}")

        except StopEvent:
            # Re-raise StopEvent to propagate it up the call stack
            raise

        except Exception as e:
            error_msg = f"Error in event handler for '{event}': {e}"
            self.logger.error(error_msg, exc_info=True)

    def _trigger_connect_event(self) -> None:
        return self._trigger_event(
            "connect", "sid", {"REMOTE_ADDR": self.namespace})

    def _trigger_disconnect_event(self) -> None:
        return self._trigger_event("disconnect", "sid")

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
                # Stop the loop
                return
            else:
                self.logger.info(
                    f"Reconnection attempt failed. Retrying in {retry_sleep} s")
                time.sleep(retry_sleep)
                retry_sleep *= 2
                if retry_sleep > 60:
                    retry_sleep = 60
        # Should not try to reconnect or be running -> Stop
        raise StopEvent

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
                    if room is None or room in self.rooms:
                        sid: uuid = message["host_uid"]
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
    API
    """
    def initialize(self) -> None:
        """Method to call other methods just before starting the background thread.
        """
        pass

    def session(self, sid: UUID | str):
        class _session_ctx_manager:
            def __init__(self, dispatcher, sid_):
                self.dispatcher: Dispatcher = dispatcher
                self.sid: UUID = sid_
                self.session: dict | None = None

            def __enter__(self):
                self.session = self.dispatcher._sessions.get(sid, {})
                return self.session

            def __exit__(self, *args):
                self.dispatcher._sessions[sid] = self.session

        if isinstance(sid, str):
            sid = UUID(sid)

        return _session_ctx_manager(self, sid)

    def disconnect(self, sid: UUID, namespace: str | None = None) -> None:
        pass  # TODO

    def register_event_handler(self, event_handler: EventHandler) -> None:
        """Register an event handler."""
        if event_handler.asyncio_based:
            raise RuntimeError(
                f"{self.__class__.__name__} requires a synchronous EventHandler"
            )
        event_handler._set_dispatcher(self)
        with self._event_handlers_lock:
            self.event_handlers.add(event_handler)

    def on(self, event: str, handler: Callable = None) -> None:
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
            with self._event_handlers_lock:
                self.handlers[event] = _handler
            return _handler

        if handler is None:
            return set_handler
        set_handler(handler)

    def emit(
            self,
            event: str,
            data: DataType = None,
            to:  UUID | None = None,
            room: str | None = None,
            namespace: str | None = None,
            ttl: int | None = None,
            timeout: int | float | None = None,
            **kwargs
    ) -> bool:
        """Emit an event to a single or multiple namespace(s)

        :param event: The event name.
        :param data: The data to send to the required dispatcher.
        :param to: The recipient of the message.
        :param room: An alias to `to`
        :param namespace: The namespace to which the event will be sent.
        :param ttl: Time to live of the message. Only available with rabbitmq.
        :param timeout: Timeout to deliver the message. Only available with rabbitmq.

        :return: True for success, False for failure
        """
        if isinstance(namespace, str):
            namespace = namespace.strip("/")
        namespace = namespace or self.namespace
        room = to or room
        payload: bytearray = self._generate_payload(event, room, data)
        try:
            self._publish(namespace, payload, ttl, timeout)
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
        """Start a background task in a managed thread.

        Args:
            target: The target function to run in the background.
            *args: Positional arguments to pass to the target function.
            task_name: Optional name for the task. If not provided, a name will be generated.
            **kwargs: Keyword arguments to pass to the target function.

        Returns:
            Thread: The thread object that was started.

        Raises:
            ValueError: If a task with the same name is already running.
            RuntimeError: If the dispatcher is not running.
        """
        #if not self.running or self._shutdown_event.is_set():
        #    raise RuntimeError("Cannot start background task: dispatcher is not running")

        task_name = task_name or f"dispatcher-{target.__name__}"

        # Create a wrapper function
        def wrapped_target():
            try:
                target(*args, **kwargs)
            except Exception as e:
                self.logger.error(
                    f"Background task '{task_name}' failed: {e}",
                    exc_info=True
                )
            finally:
                with self._threads_lock:
                    if task_name in self._threads:
                        del self._threads[task_name]

        with self._threads_lock:
            # Clean up completed threads
            self._cleanup_threads()

            # Check if a task with the same name is already running
            if task_name in self._threads:
                raise ValueError(
                    f"A background task named '{task_name}' is already running")

            # Create and store the thread
            thread = Thread(
                target=wrapped_target,
                name=task_name,
                daemon=True,
            )
            self._threads[task_name] = thread
            thread.start()
            return thread

    def _stop_threads(self, timeout: float | None = 0.1) -> None:
        with self._threads_lock:
            for thread in self._threads.values():
                thread.join(timeout=timeout)
            self._threads.clear()

    def _cleanup_threads(self) -> None:
        """Clean up finished threads and optionally wait for running ones.

        Args:
            timeout: Maximum time to wait for threads to finish (None to wait forever).
        """
        with self._threads_lock:
            if not self._threads:
                return

            # Clean up finished threads
            completed_threads = [
                name for name, threads in self._threads.items() if not threads.is_alive()]

            for name in completed_threads:
                self._threads[name].join(timeout=0.1)
                del self._threads[name]

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
            self._master_loop()
        else:
            self.start_background_task(
                target=self._master_loop, task_name="dispatcher-main_loop")

    def start(self, retry: bool = False, block: bool = True) -> None:
        """Start to dispatch and receive events."""
        self.logger.info("Starting dispatcher...")
        def wrap():
            try:
                if not self.connected:
                    self.connect(retry=retry, wait=True)
                self.run(block=True)
            except StopEvent:
                self.logger.info("Caught a stop event")

        if block:
            wrap()
        else:
            self.start_background_task(target=wrap, task_name="dispatcher-main_loop")

    def stop(self) -> None:
        """Stop the dispatcher and clean up resources."""
        if not self.running:
            raise RuntimeError("Not running")

        self.logger.info("Stopping dispatcher...")

        # Set shutdown flag to prevent new tasks
        self._shutdown_event.set()

        try:
            # Send stop signal to all rooms
            self.emit(
                STOP_SIGNAL,
                to=self.host_uid,
                namespace=self.namespace,
                ttl=15,
            )

            # Handle broker disconnect, will clean up threads
            self._handle_stop_signal()

            # Clear all handlers and sessions
            with self._event_handlers_lock:
                self.event_handlers.clear()
                self.handlers.clear()

            with self._sessions_lock:
                self._sessions.clear()

            self.logger.info("Dispatcher stopped successfully")

        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}", exc_info=True)
        finally:
            # Ensure all flags are cleared
            self._running.clear()
            self._connected.clear()
            self._reconnecting.clear()
            self._shutdown_event.clear()


class AsyncDispatcher(BaseDispatcher):
    asyncio_based = True

    def __init__(
            self,
            namespace: str = "event_dispatcher",
            parent_logger: logging.Logger = None,
            reconnection: bool = True,
    ) -> None:
        """Base class for a python-socketio inspired async event dispatcher.

        :param namespace: The namespace events will be sent from and to.
        :param parent_logger: A logging.Logger instance. The dispatcher logger
                              will be set to 'parent_logger.namespace'.
        :param reconnection: Whether to attempt reconnection on connection loss.
        """
        super().__init__(namespace, parent_logger, reconnection)

        # State
        self._running = asyncio.Event()
        self._connected = asyncio.Event()
        self._reconnecting = asyncio.Event()
        self._shutdown_event = asyncio.Event()

        # Task management
        self._tasks: dict[str, Task] = {}

    async def _broker_reachable(self) -> bool:
        """Check if it is possible to connect to the broker."""
        raise NotImplementedError(
            "This method needs to be implemented in a subclass"
        )

    async def _publish(
            self,
            namespace: str,
            payload: bytes,
            ttl: int | None = None,
            timeout: int | float | None = None,
    ) -> None:
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
        self._shutdown_event.set()
        self._running.clear()
        self._connected.clear()
        self._reconnecting.clear()
        await self._stop_tasks()

    # Events triggering
    def _get_event_handlers(self) -> set[EventHandler]:
        return self.event_handlers

    async def _trigger_event(
            self,
            event: str,
            sid: UUID,
            *args,
    ) -> None:
        """Trigger an event with the given arguments.

        Args:
            event: The name of the event to trigger.
            sid: The session ID of the sender.
            *args: Arguments to pass to the event handler.

        Returns:
            The result of the event handler, or None if the event was not handled.

        Raises:
            StopEvent: If the event handler raises StopEvent.
        """
        if event == STOP_SIGNAL:
            raise StopEvent("Received stop signal")

        try:
            event_handler = self._get_event_handler(event)
            signature = inspect.signature(event_handler)

            # Check if the handler expects a 'sid' parameter
            need_sid = "sid" in signature.parameters

            # Prepare arguments
            handler_args = (sid, *args) if need_sid else args

            # Call the handler (supports both sync and async handlers)
            if asyncio.iscoroutinefunction(event_handler):
                try:
                    await event_handler(*handler_args)
                except asyncio.CancelledError:
                    # Don't log cancelled tasks as errors
                    raise
            else:
                # Might be blocking, the user needs to handle this case
                event_handler(*handler_args)

        except UnknownEvent as e:
            if event not in {"connect", "disconnect"}:
                self.logger.warning(f"No handler for event '{event}': {e}")

        except StopEvent:
            # Re-raise StopEvent to propagate it up the call stack
            raise

        except Exception as e:
            error_msg = f"Error in async event handler for '{event}': {e}"
            self.logger.error(error_msg, exc_info=True)

    async def _trigger_connect_event(self) -> None:
        return await self._trigger_event(
            "connect", "sid", {"REMOTE_ADDR": self.namespace})

    async def _trigger_disconnect_event(self) -> None:
        return await self._trigger_event("disconnect", "sid")

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
                # Stop the loop
                return
            else:
                self.logger.info(
                    f"Reconnection attempt failed. Retrying in {retry_sleep} s")
                await asyncio.sleep(retry_sleep)
                retry_sleep *= 2
                if retry_sleep > 60:
                    retry_sleep = 60
        # Should not try to reconnect or be running -> Stop
        raise StopEvent

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
                    if room is None or room in self.rooms:
                        sid: UUID = message["host_uid"]
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

    def session(self, sid: UUID | str):
        class _session_ctx_manager:
            def __init__(self, dispatcher, sid_):
                self.dispatcher: Dispatcher = dispatcher
                self.sid: UUID = sid_
                self.session: dict | None = None

            async def __aenter__(self):
                self.session = self.dispatcher._sessions.get(sid, {})
                return self.session

            async def __aexit__(self, *args):
                self.dispatcher._sessions[sid] = self.session

        if isinstance(sid, str):
            sid = UUID(sid)

        return _session_ctx_manager(self, sid)

    async def disconnect(self, sid: UUID, namespace: str | None = None) -> None:
        pass  # TODO

    async def register_event_handler(self, event_handler: AsyncEventHandler) -> None:
        """Register an event handler."""
        if not event_handler.asyncio_based:
            raise RuntimeError(
                f"{self.__class__.__name__} requires an AsyncEventHandler"
            )
        event_handler._set_dispatcher(self)
        self.event_handlers.add(event_handler)

    def on(self, event: str, handler: Callable = None) -> None:
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

    async def emit(
            self,
            event: str,
            data: DataType = None,
            to:  UUID | None = None,
            room: str | None = None,
            namespace: str | None = None,
            ttl: int | None = None,
            timeout: int | float | None = None,
            **kwargs
    ) -> bool:
        """Emit an event to a single or multiple namespace(s)

        :param event: The event name.
        :param data: The data to send to the required dispatcher.
        :param to: The recipient of the message.
        :param room: An alias to `to`
        :param namespace: The namespace to which the event will be sent.
        :param ttl: Time to live of the message. Only available with rabbitmq.
        :param timeout: Timeout to deliver the message. Only available with rabbitmq.

        :return: True for success, False for failure
        """
        if isinstance(namespace, str):
            namespace = namespace.strip("/")
        namespace = namespace or self.namespace
        room = to or room
        payload: bytearray = self._generate_payload(event, room, data)
        try:
            await self._publish(namespace, payload, ttl, timeout)
            return True
        except ConnectionError:
            return False

    async def start_background_task(
            self,
            target: Callable,
            *args,
            task_name: str | None = None,
            **kwargs
    ) -> Task:
        """Start a background task in an asyncio task.

        Args:
            target: The target coroutine function to run in the background.
            *args: Positional arguments to pass to the target function.
            task_name: Optional name for the task. If not provided, a name will be generated.
            **kwargs: Keyword arguments to pass to the target function.

        Returns:
            asyncio.Task: The task object that was created.

        Raises:
            ValueError: If a task with the same name is already running.
            RuntimeError: If the dispatcher is not running.
        """
        #if not self.running or self._shutdown_event.is_set():
        #    raise RuntimeError("Cannot start background task: dispatcher is not running")

        task_name = task_name or f"dispatcher-{target.__name__}"

        # Create a wrapper coroutine
        async def wrapped_target():
            try:
                await target(*args, **kwargs)
            except asyncio.CancelledError:
                # Task was cancelled, no need to log
                raise
            except Exception as e:
                self.logger.error(
                    f"Background task '{task_name}' failed: {e}",
                    exc_info=True
                )
            finally:
                # Clean up the task reference when done
                if task_name in self._tasks:
                    del self._tasks[task_name]

        # Clean up completed tasks
        await self._cleanup_tasks()

        # Check if a task with the same name is already running
        if task_name in self._tasks:
            raise ValueError(
                f"A background task named '{task_name}' is already running")

        # Create and store the task
        task = asyncio.create_task(wrapped_target(), name=task_name)
        self._tasks[task_name] = task
        return task

    async def _stop_tasks(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        # Wait for the tasks to be cancelled
        await asyncio.sleep(0.1)
        await self._cleanup_tasks()

    async def _cleanup_tasks(self) -> None:
        """Clean up completed tasks from the tasks dictionary."""
        if not self._tasks:
            return

        completed_tasks = [
            name for name, task in self._tasks.items()
            if task.done() or task.cancelled()
        ]
        for name in completed_tasks:
            del self._tasks[name]

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
                    await self.start_background_task(target=self._reconnection_loop)
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
        if block:
            await self._master_loop()
        else:
            await self.start_background_task(
                target=self._master_loop, task_name="dispatcher-main_loop")

    async def start(self, retry: bool = False, block: bool = True) -> None:
        """Start to dispatch and receive events."""
        self.logger.info("Starting dispatcher...")
        async def wrap():
            try:
                if not self.connected:
                    await self.connect(retry=retry, wait=True)
                await self.run(block=True)
            except StopEvent:
                self.logger.info("Caught a stop event")

        if block:
            await wrap()
        else:
            await self.start_background_task(target=wrap, task_name="dispatcher-main_loop")

    async def stop(self) -> None:
        """Stop the dispatcher and clean up resources."""
        if not self.running:
            raise RuntimeError("Not running")

        self.logger.info("Stopping dispatcher...")

        # Set shutdown flag to prevent new tasks
        self._shutdown_event.set()

        try:
            # Send stop signal to all rooms
            await self.emit(
                STOP_SIGNAL,
                to=self.host_uid,
                namespace=self.namespace,
                ttl=15,
            )

            # Handle broker disconnect, will clean up threads
            await self._handle_stop_signal()

            # Clear all handlers and sessions
            self.event_handlers.clear()
            self.handlers.clear()

            self._sessions.clear()

            self.logger.info("Dispatcher stopped successfully")

        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}", exc_info=True)
        finally:
            # Ensure all flags are cleared
            self._running.clear()
            self._connected.clear()
            self._reconnecting.clear()
            self._shutdown_event.clear()
