import asyncio
import sys
import time
from unittest.mock import AsyncMock, Mock, patch
import uuid

import pytest

from dispatcher.ABC import BaseDispatcher, Dispatcher, AsyncDispatcher


class TestBaseDispatcher:
    def test_custom_namespace(self):
        """Test initialization with custom namespace."""
        dispatcher = BaseDispatcher(namespace="test_namespace")
        assert dispatcher.namespace == "test_namespace"


    def test_encode_decode_data(self):
        """Test encoding and decoding of different data types."""
        dispatcher = BaseDispatcher()

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
        dispatcher = BaseDispatcher()
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
        dispatcher = BaseDispatcher()

        assert dispatcher._data_as_list(None) == []
        assert dispatcher._data_as_list("test") == ["test"]
        assert dispatcher._data_as_list((1, 2, 3)) == [1, 2, 3]
        assert dispatcher._data_as_list([1, 2, 3]) == [[1, 2, 3]]


class TestDispatcher:
    def test_initialization(self):
        """Test that Dispatcher initializes with default values."""
        dispatcher = Dispatcher()
        assert dispatcher.namespace == "event_dispatcher"
        assert isinstance(dispatcher.host_uid, uuid.UUID)
        assert dispatcher.host_uid.hex in dispatcher.rooms
        assert not dispatcher.running
        assert not dispatcher.connected
        assert not dispatcher.reconnecting

    def test_emit(self):
        """Test emitting an event."""
        dispatcher = Dispatcher()
        test_event = "test_event"
        test_data = {"key": "value"}
        test_room = "test_room"
        test_to = uuid.uuid4()

        # Test emit with room
        with patch.object(dispatcher, "_publish") as mock_publish:
            result = dispatcher.emit(test_event, data=test_data, room=test_room)
            assert result is True
            mock_publish.assert_called_once()

        # Test emit with to
        with patch.object(dispatcher, "_publish") as mock_publish:
            result = dispatcher.emit(test_event, data=test_data, to=test_to)
            assert result is True
            mock_publish.assert_called_once()

    def test_lifecycle(self):
        """Test connect and disconnect flow."""
        dispatcher = Dispatcher()

        with (
            patch.object(dispatcher, "_broker_reachable", Mock(return_value=True)),
            patch.object(dispatcher, "_listen"),
            patch.object(dispatcher, "_publish") as mock_publish,
        ):

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
            mock_publish.assert_called_once()

            with pytest.raises(RuntimeError):
                dispatcher.stop()

            assert dispatcher.connected is False
            assert dispatcher.running is False

    def test_event_handling(self):
        """Test event handler registration and triggering."""
        mock_handler = Mock()
        test_event = "test_event"
        test_data = {"key": "test_value"}

        dispatcher = Dispatcher()

        # Register event handler
        dispatcher.on(test_event, mock_handler)

        # Trigger event
        dispatcher._trigger_event(test_event, dispatcher.host_uid, test_data)

        # Verify handler was called with correct arguments
        mock_handler.assert_called_once_with(test_data)

    def test_background_tasks(self):
        """Test background tasks."""
        called = False

        def call():
            nonlocal called
            time.sleep(0.1)
            called = True

        dispatcher = Dispatcher()
        dispatcher.start_background_task(call)

        time.sleep(0.2)

        assert called


@pytest.mark.asyncio
class TestAsyncDispatcher:
    async def test_initialization(self):
        """Test that AsyncDispatcher initializes with asyncio-based flags."""
        dispatcher = AsyncDispatcher()
        assert dispatcher.asyncio_based is True
        assert isinstance(dispatcher._running, asyncio.Event)
        assert isinstance(dispatcher._connected, asyncio.Event)
        assert isinstance(dispatcher._reconnecting, asyncio.Event)

    async def test_emit(self):
        """Test async emit functionality."""
        dispatcher = AsyncDispatcher()
        test_event = "test_async_event"
        test_data = {"key": "async_value"}

        # Test async emit
        with patch('dispatcher.ABC.AsyncDispatcher._publish', AsyncMock()) as mock_publish:
            result = await dispatcher.emit(test_event, data=test_data)
            assert result is True
            mock_publish.assert_awaited_once()

    async def test_lifecycle(self):
        """Test async connect and disconnect flow."""
        dispatcher = AsyncDispatcher()

        with (
            patch.object(dispatcher, "_broker_reachable", AsyncMock(return_value=True)),
            patch.object(dispatcher, "_listen"),
            patch.object(dispatcher, "_publish") as mock_publish,
        ):

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
            mock_publish.assert_called_once()

            with pytest.raises(RuntimeError):
                await dispatcher.stop()

            assert dispatcher.connected is False
            assert dispatcher.running is False

    async def test_async_event_handling(self):
        """Test async event handler registration and triggering."""
        test_data = {"key": "test_value"}
        dispatcher = AsyncDispatcher()

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

    async def test_background_tasks(self):
        """Test background tasks."""
        called = False

        async def call():
            nonlocal called
            await asyncio.sleep(0.1)
            called = True

        dispatcher = AsyncDispatcher()
        await dispatcher.start_background_task(call)

        await asyncio.sleep(0.2)

        assert called
