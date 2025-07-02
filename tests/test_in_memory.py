import asyncio
from threading import Event
import time

import pytest

from dispatcher.async_in_memory_dispatcher import AsyncInMemoryDispatcher
from dispatcher.exceptions import StopEvent
from dispatcher.in_memory_dispatcher import InMemoryDispatcher


# Test data
TEST_EVENT = "test_event"
TEST_DATA = {"key": "value"}
TEST_NAMESPACE = "test_namespace"


class TestInMemoryDispatcher:
    def test_initialization(self):
        """Test that the dispatcher initializes correctly."""
        dispatcher = InMemoryDispatcher(namespace=TEST_NAMESPACE)
        assert dispatcher.namespace == TEST_NAMESPACE
        assert not dispatcher.running
        assert not dispatcher.connected

    def test_lifecycle(self):
        """Test connecting and disconnecting the dispatcher."""
        dispatcher = InMemoryDispatcher()

        # Connect
        dispatcher.connect()
        assert dispatcher.connected is True

        # Run
        dispatcher.run(block=False)
        assert dispatcher.running is True

        # Disconnect
        dispatcher.stop()
        assert dispatcher.connected is False
        assert dispatcher.running is False

    def test_event_handling(self):
        """Test basic event handling functionality."""
        received_events = []

        def event_handler(sid, data):
            received_events.append((sid, data))
            raise StopEvent("Stop after first event")

        dispatcher = InMemoryDispatcher()
        dispatcher.on(TEST_EVENT, event_handler)

        # Start the dispatcher in a separate thread
        dispatcher.start(block=False)
        # Give it some time to start and subscribe to the broker
        time.sleep(0.1)

        # Send an event
        dispatcher.emit(TEST_EVENT, TEST_DATA)

        # Give it some time to process
        time.sleep(0.1)

        # Check the event was received
        assert len(received_events) == 1
        sid, data = received_events[0]
        assert sid == dispatcher.host_uid
        assert data == TEST_DATA

        # Make sure the dispatcher processed the stop event
        assert dispatcher.stopped is True
        assert dispatcher.connected is False
        assert dispatcher.running is False

    def test_namespace_isolation(self):
        """Test that different namespaces don't interfere with each other."""
        events1 = []
        events2 = []

        def handler1(sid, data):
            events1.append((sid, data))
            #raise StopEvent("Stop after first event")

        def handler2(sid, data):
            events2.append((sid, data))
            #raise StopEvent("Stop after first event")

        # Create two dispatchers with different namespaces
        dispatcher1 = InMemoryDispatcher(namespace="ns1")
        dispatcher2 = InMemoryDispatcher(namespace="ns2")
        dispatcher3 = InMemoryDispatcher(namespace="ns1")

        dispatcher1.on(TEST_EVENT, handler1)
        dispatcher2.on(TEST_EVENT, handler2)

        dispatcher1.start(block=False)
        dispatcher2.start(block=False)
        # Give them some time to start and subscribe to the broker
        time.sleep(0.1)

        try:
            # Send events to both namespaces
            dispatcher1.emit(TEST_EVENT, {"test": "data1"})
            dispatcher2.emit(TEST_EVENT, {"test": "data2"})
            dispatcher3.emit(TEST_EVENT, {"test": "data3"})

            # Give them some time to process
            time.sleep(0.2)

            # Handlers should only receive events from their own namespace
            assert len(events1) == 2
            assert len(events2) == 1

            assert events1[0][0] == dispatcher1.host_uid
            assert events1[0][1]["test"] == "data1"
            assert events1[1][0] == dispatcher3.host_uid
            assert events1[1][1]["test"] == "data3"
            assert events2[0][0] == dispatcher2.host_uid
            assert events2[0][1]["test"] == "data2"

        finally:
            dispatcher1.stop()
            dispatcher2.stop()

    def test_concurrent_events(self):
        """Test that the dispatcher can handle multiple events concurrently."""
        received_events = []
        event_count = 5
        event_received = Event()

        def event_handler(data):
            received_events.append(data)
            if len(received_events) >= event_count:
                event_received.set()
            if len(received_events) == 1:
                # Add a small delay to test concurrency
                time.sleep(0.1)

        dispatcher = InMemoryDispatcher()
        dispatcher.on(TEST_EVENT, event_handler)

        # Start the dispatcher
        dispatcher.start(block=False)
        # Give it some time to start and subscribe to the broker
        time.sleep(0.1)

        try:
            # Send multiple events
            for i in range(event_count):
                dispatcher.emit(TEST_EVENT, {"index": i})

            # Wait for all events to be processed
            event_received.wait(timeout=2.0)

            # Check all events were received
            assert len(received_events) == event_count
            assert sorted(e["index"] for e in received_events) == list(range(event_count))

        finally:
            dispatcher.stop()

    def test_rooms(self):
        """Test adding and removing rooms."""
        counter1 = 0
        counter2 = 0

        dispatcher1 = InMemoryDispatcher()
        dispatcher2 = InMemoryDispatcher()

        @dispatcher1.on(TEST_EVENT)
        def handler1(sid, data):
            nonlocal counter1
            counter1 += 1

        @dispatcher2.on(TEST_EVENT)
        def handler2(sid, data):
            nonlocal counter2
            counter2 += 1

        dispatcher1.start(block=False)
        dispatcher2.start(block=False)

        # Dispatcher enter a room with their uuid as soon as they start
        assert len(dispatcher1.rooms) == 1
        assert len(dispatcher2.rooms) == 1

        # Send events to all dispatchers
        dispatcher1.emit(TEST_EVENT, {"test": "data1"})
        time.sleep(0.1)  # Give them some time to process
        assert counter1 == 1
        assert len(dispatcher1.rooms) == 1
        assert counter2 == 1
        assert len(dispatcher2.rooms) == 1

        # Send events to empty room1
        dispatcher1.emit(TEST_EVENT, {"test": "data2"}, room="room1")
        time.sleep(0.1)  # Give them some time to process
        assert counter1 == 1
        assert len(dispatcher1.rooms) == 1
        assert counter2 == 1
        assert len(dispatcher2.rooms) == 1

        # Dispatcher1 joins room1
        dispatcher1.enter_room("room1")
        dispatcher1.emit(TEST_EVENT, {"test": "data2"}, room="room1")
        time.sleep(0.1)  # Give them some time to process
        assert counter1 == 2
        assert len(dispatcher1.rooms) == 2
        assert counter2 == 1
        assert len(dispatcher2.rooms) == 1

        # Both dispatchers enter room2
        dispatcher1.enter_room("room2")
        dispatcher2.enter_room("room2")
        dispatcher1.emit(TEST_EVENT, {"test": "data3"}, room="room2")
        time.sleep(0.1)  # Give them some time to process
        assert counter1 == 3
        assert len(dispatcher1.rooms) == 3
        assert counter2 == 2
        assert len(dispatcher2.rooms) == 2

        # Dispatcher1 leaves room 1
        dispatcher1.leave_room("room1")
        dispatcher1.emit(TEST_EVENT, {"test": "data4"}, room="room1")
        time.sleep(0.1)  # Give them some time to process
        assert counter1 == 3
        assert len(dispatcher1.rooms) == 2
        assert counter2 == 2
        assert len(dispatcher2.rooms) == 2

        # Send to dispatcher2 only
        dispatcher1.emit(TEST_EVENT, {"test": "data5"}, to=dispatcher2.host_uid)
        time.sleep(0.2)  # Give them some time to process
        assert counter1 == 3
        assert len(dispatcher1.rooms) == 2
        assert counter2 == 3
        assert len(dispatcher2.rooms) == 2

@pytest.mark.asyncio
class TestAsyncInMemoryDispatcher:
    async def test_initialization(self):
        """Test that the async dispatcher initializes correctly."""
        dispatcher = AsyncInMemoryDispatcher(namespace=TEST_NAMESPACE)
        assert dispatcher.namespace == TEST_NAMESPACE
        assert not dispatcher.running
        assert not dispatcher.connected

    async def test_lifecycle(self):
        """Test connecting and disconnecting the async dispatcher."""
        dispatcher = AsyncInMemoryDispatcher()

        # Connect
        await dispatcher.connect()
        assert dispatcher.connected is True

        # Run
        await dispatcher.run(block=False)
        assert dispatcher.running is True

        # Disconnect
        await dispatcher.stop()
        assert dispatcher.connected is False
        assert dispatcher.running is False

    async def test_event_handling(self):
        """Test basic async event handling functionality."""
        """Test basic event handling functionality."""
        received_events = []

        async def event_handler(sid, data):
            received_events.append((sid, data))
            raise StopEvent("Stop after first event")

        dispatcher = AsyncInMemoryDispatcher()
        dispatcher.on(TEST_EVENT, event_handler)

        # Start the dispatcher in a separate thread
        await dispatcher.start(block=False)
        # Give it some time to start and subscribe to the broker
        await asyncio.sleep(0.1)

        # Send an event
        await dispatcher.emit(TEST_EVENT, TEST_DATA)

        # Give it some time to process
        await asyncio.sleep(0.1)

        # Check the event was received
        assert len(received_events) == 1
        sid, data = received_events[0]
        assert sid == dispatcher.host_uid
        assert data == TEST_DATA

        # Make sure the dispatcher processed the stop event
        assert dispatcher.stopped is True
        assert dispatcher.connected is False
        assert dispatcher.running is False

    async def test_namespace_isolation(self):
        """Test that different namespaces don't interfere with each other."""
        events1 = []
        events2 = []

        def handler1(sid, data):
            events1.append((sid, data))
            #raise StopEvent("Stop after first event")

        def handler2(sid, data):
            events2.append((sid, data))
            #raise StopEvent("Stop after first event")

        # Create two dispatchers with different namespaces
        dispatcher1 = AsyncInMemoryDispatcher(namespace="ns1")
        dispatcher2 = AsyncInMemoryDispatcher(namespace="ns2")
        dispatcher3 = AsyncInMemoryDispatcher(namespace="ns1")

        dispatcher1.on(TEST_EVENT, handler1)
        dispatcher2.on(TEST_EVENT, handler2)

        await dispatcher1.start(block=False)
        await dispatcher2.start(block=False)
        # Give them some time to start and subscribe to the broker
        await asyncio.sleep(0.1)

        try:
            # Send events to both namespaces
            await dispatcher1.emit(TEST_EVENT, {"test": "data1"})
            await dispatcher2.emit(TEST_EVENT, {"test": "data2"})
            await dispatcher3.emit(TEST_EVENT, {"test": "data3"})

            # Give it some time to process
            await asyncio.sleep(0.2)

            # Handlers should only receive events from their own namespace
            assert len(events1) == 2
            assert len(events2) == 1

            assert events1[0][0] == dispatcher1.host_uid
            assert events1[0][1]["test"] == "data1"
            assert events1[1][0] == dispatcher3.host_uid
            assert events1[1][1]["test"] == "data3"
            assert events2[0][0] == dispatcher2.host_uid
            assert events2[0][1]["test"] == "data2"

        finally:
            await dispatcher1.stop()
            await dispatcher2.stop()

    async def test_concurrent_events(self):
        """Test that the dispatcher can handle multiple events concurrently."""
        received_events = []
        event_count = 5
        event_received = asyncio.Event()

        async def event_handler(data):
            received_events.append(data)
            if len(received_events) >= event_count:
                event_received.set()
            if len(received_events) == 1:
                # Add a small delay to test concurrency
                await asyncio.sleep(0.1)

        dispatcher = AsyncInMemoryDispatcher()
        dispatcher.on(TEST_EVENT, event_handler)

        # Start the dispatcher
        await dispatcher.start(block=False)
        # Give it some time to start and subscribe to the broker
        await asyncio.sleep(0.1)

        try:
            # Send multiple events
            for i in range(event_count):
                await dispatcher.emit(TEST_EVENT, {"index": i})

            # Wait for all events to be processed
            await asyncio.wait_for(event_received.wait(), timeout=2.0)

            # Check all events were received
            assert len(received_events) == event_count
            assert sorted(e["index"] for e in received_events) == list(range(event_count))

        finally:
            await dispatcher.stop()

    async def test_rooms(self):
        """Test adding and removing rooms."""
        counter1 = 0
        counter2 = 0

        dispatcher1 = AsyncInMemoryDispatcher()
        dispatcher2 = AsyncInMemoryDispatcher()

        @dispatcher1.on(TEST_EVENT)
        def handler1(sid, data):
            nonlocal counter1
            counter1 += 1

        @dispatcher2.on(TEST_EVENT)
        async def handler2(sid, data):
            nonlocal counter2
            counter2 += 1

        await dispatcher1.start(block=False)
        await dispatcher2.start(block=False)
        # Give it some time to start and subscribe to the broker
        await asyncio.sleep(0.1)

        # Dispatcher enter a room with their uuid as soon as they start
        assert len(dispatcher1.rooms) == 1
        assert len(dispatcher2.rooms) == 1

        # Send events to all dispatchers
        await dispatcher1.emit(TEST_EVENT, {"test": "data1"})
        await asyncio.sleep(0.1)  # Give them some time to process
        assert counter1 == 1
        assert len(dispatcher1.rooms) == 1
        assert counter2 == 1
        assert len(dispatcher2.rooms) == 1

        # Send events to empty room1
        await dispatcher1.emit(TEST_EVENT, {"test": "data2"}, room="room1")
        await asyncio.sleep(0.1)  # Give them some time to process
        assert counter1 == 1
        assert len(dispatcher1.rooms) == 1
        assert counter2 == 1
        assert len(dispatcher2.rooms) == 1

        # Dispatcher1 joins room1
        dispatcher1.enter_room("room1")
        await dispatcher1.emit(TEST_EVENT, {"test": "data2"}, room="room1")
        await asyncio.sleep(0.1)  # Give them some time to process
        assert counter1 == 2
        assert len(dispatcher1.rooms) == 2
        assert counter2 == 1
        assert len(dispatcher2.rooms) == 1

        # Both dispatchers enter room2
        dispatcher1.enter_room("room2")
        dispatcher2.enter_room("room2")
        await dispatcher1.emit(TEST_EVENT, {"test": "data3"}, room="room2")
        await asyncio.sleep(0.1)  # Give them some time to process
        assert counter1 == 3
        assert len(dispatcher1.rooms) == 3
        assert counter2 == 2
        assert len(dispatcher2.rooms) == 2

        # Dispatcher1 leaves room 1
        dispatcher1.leave_room("room1")
        await dispatcher1.emit(TEST_EVENT, {"test": "data4"}, room="room1")
        await asyncio.sleep(0.1)  # Give them some time to process
        assert counter1 == 3
        assert len(dispatcher1.rooms) == 2
        assert counter2 == 2
        assert len(dispatcher2.rooms) == 2

        # Send to dispatcher2 only
        await dispatcher1.emit(TEST_EVENT, {"test": "data5"}, room=dispatcher2.host_uid)
        await asyncio.sleep(0.2)  # Give them some time to process
        assert counter1 == 3
        assert len(dispatcher1.rooms) == 2
        assert counter2 == 3
        assert len(dispatcher2.rooms) == 2