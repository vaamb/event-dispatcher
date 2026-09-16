import asyncio
import sys
import time
from unittest import TestCase
from unittest.mock import ANY, AsyncMock, Mock, patch
import uuid

import pytest

from dispatcher.ABC import AsyncDispatcher, BaseDispatcher, Dispatcher, EMPTY
from dispatcher.async_in_memory_dispatcher import AsyncInMemoryDispatcher
from dispatcher.exceptions import StopEvent
from dispatcher.in_memory_dispatcher import InMemoryDispatcher

from conftest import (
    async_running, async_wait_until, running, wait_until)
from handlers import AsyncPingPong, PingPong


class MockBaseDispatcher(BaseDispatcher):
    _get_event_handlers = Mock()


class MockDispatcher(Dispatcher, MockBaseDispatcher):
    _broker_reachable = Mock(return_value=True)
    _publish = Mock()
    _listen = Mock()
    disconnect = Mock()


class MockAsyncDispatcher(AsyncDispatcher, MockBaseDispatcher):
    _broker_reachable = AsyncMock(return_value=True)
    _publish = AsyncMock()
    _listen = AsyncMock()
    disconnect = AsyncMock()


class TestBaseDispatcher(TestCase):
    def setUp(self):
        MockBaseDispatcher._get_event_handlers.reset_mock()

    def test_custom_namespace(self):
        """Test initialization with custom namespace."""
        dispatcher = MockBaseDispatcher(namespace="test_namespace")
        assert dispatcher.namespace == "test_namespace"


    def test_encode_decode_data(self):
        """Test encoding and decoding of different data types."""
        dispatcher = MockBaseDispatcher()

        # Test with dict
        test_dict = {"key": "value"}
        encoded = dispatcher._encode_data(test_dict)
        assert isinstance(encoded, bytearray)
        assert dispatcher._DATA_OBJECT in encoded
        decoded = dispatcher._decode_data(encoded)
        assert isinstance(decoded, dict)
        assert decoded == test_dict

        # Test with tuple
        test_tuple = (1, 2, 3)
        encoded = dispatcher._encode_data(test_tuple)
        assert isinstance(encoded, bytearray)
        assert dispatcher._DATA_OBJECT in encoded
        decoded = dispatcher._decode_data(encoded)
        assert isinstance(decoded, list)  # Tuple is converted to list
        assert decoded == [*test_tuple]  # Ditto

        # Test with list
        test_list = [1, 2, 3]
        encoded = dispatcher._encode_data(test_list)
        assert isinstance(encoded, bytearray)
        assert dispatcher._DATA_OBJECT in encoded
        decoded = dispatcher._decode_data(encoded)
        assert isinstance(decoded, list)
        assert decoded == test_list

        # Test with str
        test_str = "test string"
        encoded = dispatcher._encode_data(test_str)
        assert isinstance(encoded, bytearray)
        assert dispatcher._DATA_OBJECT in encoded
        decoded = dispatcher._decode_data(encoded)
        assert isinstance(decoded, str)
        assert decoded == test_str

        # Test with None
        encoded = dispatcher._encode_data(None)
        assert isinstance(encoded, bytearray)
        assert dispatcher._DATA_OBJECT in encoded
        decoded = dispatcher._decode_data(encoded)
        assert decoded is None

        # Test with bytes
        test_bytes = b"test bytes"
        encoded = dispatcher._encode_data(test_bytes)
        assert isinstance(encoded, bytearray)
        assert dispatcher._DATA_BINARY in encoded
        decoded = dispatcher._decode_data(encoded)
        assert isinstance(decoded, bytearray)  # Bytes type is converted to bytearray
        assert decoded == bytearray(test_bytes)  # Ditto


    def test_generate_parse_payload(self):
        """Test payload generation and parsing."""
        dispatcher = MockBaseDispatcher()
        test_event = "test_event"
        test_room = "test_room"
        test_data = {"key": "value"}

        # Generate payload
        payload = dispatcher._generate_payload(test_event, test_room, test_data)
        assert isinstance(payload, bytearray)

        # Parse payload
        parsed = dispatcher._parse_payload(payload)
        assert isinstance(parsed, dict)

        # Verify parsed payload
        assert parsed["event"] == test_event
        assert parsed["room"] == test_room
        assert parsed["data"] == test_data
        assert parsed["host_uid"] == dispatcher.host_uid


    def test_data_as_list(self):
        """Test conversion of data to list."""
        dispatcher = MockBaseDispatcher()

        assert dispatcher._data_as_list(None) == [None]
        assert dispatcher._data_as_list("test") == ["test"]
        assert dispatcher._data_as_list((1, 2, 3)) == [1, 2, 3]
        assert dispatcher._data_as_list([1, 2, 3]) == [[1, 2, 3]]
        assert dispatcher._data_as_list({"key": "value"}) == [{"key": "value"}]
        assert dispatcher._data_as_list(b"test") == [b"test"]
        assert dispatcher._data_as_list(EMPTY) == []


class TestDispatcher(TestCase):
    def setUp(self):
        MockDispatcher._broker_reachable.reset_mock()
        MockDispatcher._publish.reset_mock()
        MockDispatcher._listen.reset_mock()
        MockDispatcher.disconnect.reset_mock()

    def test_initialization(self):
        """Test that Dispatcher initializes with default values."""
        dispatcher = MockDispatcher()
        assert dispatcher.namespace == "event_dispatcher"
        assert isinstance(dispatcher.host_uid, uuid.UUID)
        assert dispatcher.host_uid.hex in dispatcher.rooms
        assert not dispatcher.running
        assert not dispatcher.connected
        assert not dispatcher.reconnecting

    def test_session(self):
        dispatcher = MockDispatcher()
        with dispatcher.session("session_1") as session:
            assert session == {}
            session["test"] = True

        with dispatcher.session("session_2") as session:
            assert session == {}

        with dispatcher.session("session_1") as session:
            assert session["test"] is True

    def test_emit(self):
        """Test emitting an event."""
        dispatcher = MockDispatcher()
        test_event = "test_event"
        test_data = {"key": "value"}
        test_room = "test_room"
        test_to = uuid.uuid4()

        # Test emit with room
        result = dispatcher.emit(test_event, data=test_data, room=test_room)
        assert result is True
        dispatcher._publish.assert_called_once()
        dispatcher._publish.reset_mock()

        # Test emit with to
        result = dispatcher.emit(test_event, data=test_data, to=test_to)
        assert result is True
        dispatcher._publish.assert_called_once()
        dispatcher._publish.reset_mock()

        # Events go to the dispatcher's own namespace unless told otherwise
        dispatcher.emit(test_event)
        dispatcher._publish.assert_called_once_with(
            dispatcher.namespace, ANY, None, None)
        dispatcher._publish.reset_mock()

        dispatcher.emit(test_event, namespace="/other_namespace/")
        dispatcher._publish.assert_called_once_with(
            "other_namespace", ANY, None, None)

    def test_lifecycle(self):
        """Test connect and disconnect flow."""
        dispatcher = MockDispatcher()

        # Test connect
        dispatcher.connect()
        assert dispatcher.connected is True

        # Test running
        dispatcher.run(block=False)
        assert dispatcher.running is True

        with pytest.raises(RuntimeError):
            dispatcher.run(block=False)

        # Test disconnect
        dispatcher.stop()
        dispatcher._publish.assert_called_once()

        with pytest.raises(RuntimeError):
            dispatcher.stop()

        assert dispatcher.connected is False
        assert dispatcher.running is False

    def test_event_handling(self):
        """Test event handler registration and triggering."""
        mock_handler = Mock()
        test_event = "test_event"
        test_data = {"key": "test_value"}

        dispatcher = MockDispatcher()

        # Register event handler
        dispatcher.on(test_event, mock_handler)

        # Trigger event
        dispatcher._trigger_event(test_event, dispatcher.host_uid, test_data)

        # Verify handler was called with correct arguments
        mock_handler.assert_called_once_with(test_data)

        # Reset mock
        mock_handler.reset_mock()

        # Trigger nonexistent event
        with patch.object(dispatcher.logger, "warning") as warning_logger:
            dispatcher._trigger_event("nonexistent_event", dispatcher.host_uid, test_data)
            # Verify logger.warning was called with correct arguments
            warning_logger.assert_called_once_with(
                "No handler for event 'nonexistent_event': No handler found for event 'nonexistent_event'")

        # Set a default handler
        dispatcher.fallback = mock_handler

        # Trigger nonexistent event
        dispatcher._trigger_event("nonexistent_event", dispatcher.host_uid, test_data)

        # Verify default handler was called with correct arguments
        mock_handler.assert_called_once_with(test_data)

    def test_background_tasks(self):
        """Test background tasks."""
        called = False

        def call():
            nonlocal called
            time.sleep(0.1)
            called = True

        dispatcher = MockDispatcher()
        dispatcher.start_background_task(call)

        time.sleep(0.2)

        assert called


@pytest.mark.asyncio
class TestAsyncDispatcher:
    def setup_method(self):
        MockAsyncDispatcher._broker_reachable.reset_mock()
        MockAsyncDispatcher._publish.reset_mock()
        MockAsyncDispatcher._listen.reset_mock()
        MockAsyncDispatcher.disconnect.reset_mock()

    async def test_initialization(self):
        """Test that AsyncDispatcher initializes with asyncio-based flags."""
        dispatcher = MockAsyncDispatcher()
        assert dispatcher.asyncio_based is True
        assert isinstance(dispatcher._running, asyncio.Event)
        assert isinstance(dispatcher._connected, asyncio.Event)
        assert isinstance(dispatcher._reconnecting, asyncio.Event)

    async def test_session(self):
        dispatcher = MockAsyncDispatcher()
        async with dispatcher.session("session_1") as session:
            assert session == {}
            session["test"] = True

        async with dispatcher.session("session_2") as session:
            assert session == {}

        async with dispatcher.session("session_1") as session:
            assert session["test"] is True

    async def test_emit(self):
        """Test async emit functionality."""
        dispatcher = MockAsyncDispatcher()
        test_event = "test_async_event"
        test_data = {"key": "async_value"}

        # Test async emit
        result = await dispatcher.emit(test_event, data=test_data)
        assert result is True
        dispatcher._publish.assert_awaited_once_with(
            dispatcher.namespace, ANY, None, None)
        dispatcher._publish.reset_mock()

        # Events go to another namespace when told to
        await dispatcher.emit(test_event, namespace="/other_namespace/")
        dispatcher._publish.assert_awaited_once_with(
            "other_namespace", ANY, None, None)

    async def test_lifecycle(self):
        """Test async connect and disconnect flow."""
        dispatcher = MockAsyncDispatcher()

        # Test connect
        await dispatcher.connect()
        assert dispatcher.connected is True

        # Test running
        await dispatcher.run(block=False)
        assert dispatcher.running is True

        with pytest.raises(RuntimeError):
            await dispatcher.run(block=False)

        # Test disconnect
        await dispatcher.stop()
        dispatcher._publish.assert_called_once()
        dispatcher._publish.reset_mock()

        with pytest.raises(RuntimeError):
            await dispatcher.stop()

        assert dispatcher.connected is False
        assert dispatcher.running is False

    async def test_event_handling(self):
        """Test async event handler registration and triggering."""
        test_data = {"key": "test_value"}
        dispatcher = MockAsyncDispatcher()

        # Test sync event handler registration and triggering
        mock_handler = Mock()
        test_event = "test_event"
        dispatcher.on(test_event, mock_handler)

        await dispatcher._trigger_event(test_event, dispatcher.host_uid, test_data)
        mock_handler.assert_called_once_with(test_data)

        # Test async event handler registration and triggering
        async_mock_handler = AsyncMock()
        async_test_event = "async_test_event"
        dispatcher.on(async_test_event, async_mock_handler)

        version = sys.version_info
        if (
                version.minor == 10 and version.micro >= 6
                or version.minor == 11 and version.micro <=2
        ):
            # There is an issue with inspect.signature(AsyncMock()) on Python 3.10.6 and 3.11.2
            return

        await dispatcher._trigger_event(async_test_event, dispatcher.host_uid, test_data)
        async_mock_handler.assert_awaited_once_with(test_data)

        # Reset mock
        mock_handler.reset_mock()

        # Trigger nonexistent event
        with patch.object(dispatcher.logger, "warning") as warning_logger:
            await dispatcher._trigger_event("nonexistent_event", dispatcher.host_uid, test_data)
            # Verify logger.warning was called with correct arguments
            warning_logger.assert_called_once_with(
                "No handler for event 'nonexistent_event': No handler found for event 'nonexistent_event'")

        # Set a default handler
        dispatcher.fallback = mock_handler

        # Trigger nonexistent event
        await dispatcher._trigger_event("nonexistent_event", dispatcher.host_uid, test_data)

        # Verify default handler was called with correct arguments
        mock_handler.assert_called_once_with(test_data)

    async def test_background_tasks(self):
        """Test background tasks."""
        called = False

        async def call():
            nonlocal called
            await asyncio.sleep(0.1)
            called = True

        dispatcher = MockAsyncDispatcher()
        await dispatcher.start_background_task(call)

        await asyncio.sleep(0.2)

        assert called


# The loops driving a dispatcher (`_listen_loop` and friends) need a working
# `_listen()`/`_publish()` pair, so they are exercised through the in-memory
# dispatchers, the simplest concrete implementations of the ABCs.

def make_dispatcher(namespace: str, handler: PingPong) -> InMemoryDispatcher:
    dispatcher = InMemoryDispatcher(namespace)
    dispatcher.register_event_handler(handler)
    return dispatcher


class TestListenLoop:
    def test_event_delivery(self, namespace):
        """Handlers receive the emitter's uid and the decoded data."""
        handler = PingPong()
        dispatcher = make_dispatcher(namespace, handler)

        with running(dispatcher):
            dispatcher.emit("pong", {"key": "value"})

            sid, data = handler.expect("pong")
            assert sid == dispatcher.host_uid
            assert data == {"key": "value"}

    def test_stop_event_stops_dispatcher(self, namespace):
        dispatcher = InMemoryDispatcher(namespace)

        @dispatcher.on("stop")
        def on_stop() -> None:
            raise StopEvent("Stop on first event")

        with running(dispatcher):
            dispatcher.emit("stop")

            wait_until(lambda: not dispatcher.running)
            assert dispatcher.stopped is True
            assert dispatcher.connected is False

    def test_multiple_events(self, namespace):
        """A burst of events is delivered in full and in order."""
        handler = PingPong()
        dispatcher = make_dispatcher(namespace, handler)
        event_count = 5

        with running(dispatcher):
            for i in range(event_count):
                dispatcher.emit("pong", i)

            received = [handler.expect("pong")[1] for _ in range(event_count)]
            assert received == list(range(event_count))

    def test_rooms(self, namespace):
        """Events are only delivered to dispatchers that are in the room."""
        handler1 = PingPong()
        handler2 = PingPong()
        dispatcher1 = make_dispatcher(namespace, handler1)
        dispatcher2 = make_dispatcher(namespace, handler2)

        def deliveries(**kwargs) -> tuple[bool, bool]:
            """Emit a targeted `pong` from dispatcher1, then a broadcast marker,
            and tell which dispatchers received the targeted one.

            In-memory delivery is FIFO, so a handler seeing the marker first
            has filtered the targeted event out.
            """
            dispatcher1.emit("pong", "targeted", **kwargs)
            dispatcher1.emit("pong", "marker")
            results = []
            for handler in (handler1, handler2):
                _, data = handler.expect("pong")
                if data == "targeted":
                    handler.expect("pong")  # Consume the marker
                results.append(data == "targeted")
            return tuple(results)

        with running(dispatcher1), running(dispatcher2):
            # Dispatchers enter a room named after their uid as soon as they start
            assert dispatcher1.rooms == {dispatcher1.host_uid.hex}
            assert dispatcher2.rooms == {dispatcher2.host_uid.hex}

            # Broadcast
            assert deliveries() == (True, True)

            # Empty room
            assert deliveries(room="room1") == (False, False)

            # Dispatcher1 joins room1
            dispatcher1.enter_room("room1")
            assert deliveries(room="room1") == (True, False)

            # Both dispatchers join room2
            dispatcher1.enter_room("room2")
            dispatcher2.enter_room("room2")
            assert deliveries(room="room2") == (True, True)
            assert dispatcher1.rooms == {dispatcher1.host_uid.hex, "room1", "room2"}
            assert dispatcher2.rooms == {dispatcher2.host_uid.hex, "room2"}

            # Dispatcher1 leaves room1
            dispatcher1.leave_room("room1")
            assert deliveries(room="room1") == (False, False)
            assert dispatcher1.rooms == {dispatcher1.host_uid.hex, "room2"}

            # Direct message to dispatcher2
            assert deliveries(to=dispatcher2.host_uid) == (False, True)


def make_async_dispatcher(
        namespace: str, handler: AsyncPingPong) -> AsyncInMemoryDispatcher:
    dispatcher = AsyncInMemoryDispatcher(namespace)
    dispatcher.register_event_handler(handler)
    return dispatcher


@pytest.mark.asyncio
class TestAsyncListenLoop:
    async def test_event_delivery(self, namespace):
        """Handlers receive the emitter's uid and the decoded data."""
        handler = AsyncPingPong()
        dispatcher = make_async_dispatcher(namespace, handler)

        async with async_running(dispatcher):
            await dispatcher.emit("pong", {"key": "value"})

            sid, data = await handler.expect("pong")
            assert sid == dispatcher.host_uid
            assert data == {"key": "value"}

    async def test_stop_event_stops_dispatcher(self, namespace):
        dispatcher = AsyncInMemoryDispatcher(namespace)

        @dispatcher.on("stop")
        async def on_stop() -> None:
            raise StopEvent("Stop on first event")

        async with async_running(dispatcher):
            await dispatcher.emit("stop")

            await async_wait_until(lambda: not dispatcher.running)
            assert dispatcher.stopped is True
            assert dispatcher.connected is False

    async def test_multiple_events(self, namespace):
        """A burst of events is delivered in full and in order."""
        handler = AsyncPingPong()
        dispatcher = make_async_dispatcher(namespace, handler)
        event_count = 5

        async with async_running(dispatcher):
            for i in range(event_count):
                await dispatcher.emit("pong", i)

            received = [
                (await handler.expect("pong"))[1] for _ in range(event_count)]
            assert received == list(range(event_count))

    async def test_rooms(self, namespace):
        """Events are only delivered to dispatchers that are in the room."""
        handler1 = AsyncPingPong()
        handler2 = AsyncPingPong()
        dispatcher1 = make_async_dispatcher(namespace, handler1)
        dispatcher2 = make_async_dispatcher(namespace, handler2)

        async def deliveries(**kwargs) -> tuple[bool, bool]:
            """Emit a targeted `pong` from dispatcher1, then a broadcast marker,
            and tell which dispatchers received the targeted one.

            In-memory delivery is FIFO, so a handler seeing the marker first
            has filtered the targeted event out.
            """
            await dispatcher1.emit("pong", "targeted", **kwargs)
            await dispatcher1.emit("pong", "marker")
            results = []
            for handler in (handler1, handler2):
                _, data = await handler.expect("pong")
                if data == "targeted":
                    await handler.expect("pong")  # Consume the marker
                results.append(data == "targeted")
            return tuple(results)

        async with async_running(dispatcher1), async_running(dispatcher2):
            # Dispatchers enter a room named after their uid as soon as they start
            assert dispatcher1.rooms == {dispatcher1.host_uid.hex}
            assert dispatcher2.rooms == {dispatcher2.host_uid.hex}

            # Broadcast
            assert await deliveries() == (True, True)

            # Empty room
            assert await deliveries(room="room1") == (False, False)

            # Dispatcher1 joins room1
            dispatcher1.enter_room("room1")
            assert await deliveries(room="room1") == (True, False)

            # Both dispatchers join room2
            dispatcher1.enter_room("room2")
            dispatcher2.enter_room("room2")
            assert await deliveries(room="room2") == (True, True)
            assert dispatcher1.rooms == {dispatcher1.host_uid.hex, "room1", "room2"}
            assert dispatcher2.rooms == {dispatcher2.host_uid.hex, "room2"}

            # Dispatcher1 leaves room1
            dispatcher1.leave_room("room1")
            assert await deliveries(room="room1") == (False, False)
            assert dispatcher1.rooms == {dispatcher1.host_uid.hex, "room2"}

            # Direct message to dispatcher2
            assert await deliveries(to=dispatcher2.host_uid) == (False, True)
