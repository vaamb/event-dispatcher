"""Tests for the async dispatchers against their message broker.

Async counterpart of `test_brokers.py`, see its docstring.
"""
import asyncio

import pytest

from dispatcher import (
    AsyncAMQPDispatcher, AsyncInMemoryDispatcher, AsyncRedisDispatcher)

from conftest import (
    RABBITMQ_URL, REDIS_URL, require_broker, async_running as running,
    async_wait_until as wait_until,
    async_wait_until_listening as wait_until_listening)
from handlers import AsyncPingPong


pytestmark = pytest.mark.asyncio


class AMQPBackend:
    url = RABBITMQ_URL
    unreachable_url = "amqp://guest:guest@localhost:1//"

    @staticmethod
    def make_dispatcher(
            namespace: str,
            url: str,
            handler: AsyncPingPong | None = None,
            **kwargs,
    ) -> AsyncAMQPDispatcher:
        # `auto_delete` removes the queue from the broker once its last consumer
        # is gone, so tests do not leave queues (and pending messages) behind
        queue_options = {"auto_delete": True, **kwargs.pop("queue_options", {})}
        dispatcher = AsyncAMQPDispatcher(
            namespace, url=url, queue_options=queue_options, **kwargs)
        if handler is not None:
            dispatcher.register_event_handler(handler)
        return dispatcher

    @staticmethod
    async def drop_listener_connection(dispatcher: AsyncAMQPDispatcher) -> None:
        # Closing the listener connection fires the `close_callbacks` that
        # `_listen()` relies on to notice a broker disconnect
        await dispatcher.listener_connection.close()

    @staticmethod
    def assert_connections_open(dispatcher: AsyncAMQPDispatcher) -> None:
        assert dispatcher._listener_connection is not None

    @staticmethod
    def assert_connections_released(dispatcher: AsyncAMQPDispatcher) -> None:
        assert dispatcher._publisher_connection is None
        assert dispatcher._listener_connection is None


class RedisBackend:
    url = REDIS_URL
    unreachable_url = "redis://localhost:1/0"

    @staticmethod
    def make_dispatcher(
            namespace: str,
            url: str,
            handler: AsyncPingPong | None = None,
            **kwargs,
    ) -> AsyncRedisDispatcher:
        # Redis pub/sub keeps no state on the server, so nothing to clean up
        dispatcher = AsyncRedisDispatcher(namespace, url=url, **kwargs)
        if handler is not None:
            dispatcher.register_event_handler(handler)
        return dispatcher

    @staticmethod
    async def drop_listener_connection(dispatcher: AsyncRedisDispatcher) -> None:
        # Closing the transport makes the pending read fail with a connection
        # error, as a network drop would
        dispatcher.pubsub.connection._writer.close()

    @staticmethod
    def assert_connections_open(dispatcher: AsyncRedisDispatcher) -> None:
        assert dispatcher._redis is not None

    @staticmethod
    def assert_connections_released(dispatcher: AsyncRedisDispatcher) -> None:
        assert dispatcher._pubsub is None
        assert dispatcher._redis is None


class InMemoryBackend:
    url = None  # No broker to reach

    @staticmethod
    def make_dispatcher(
            namespace: str,
            url: None,
            handler: AsyncPingPong | None = None,
            **kwargs,
    ) -> AsyncInMemoryDispatcher:
        dispatcher = AsyncInMemoryDispatcher(namespace, **kwargs)
        if handler is not None:
            dispatcher.register_event_handler(handler)
        return dispatcher

    @staticmethod
    def assert_connections_open(dispatcher: AsyncInMemoryDispatcher) -> None:
        assert dispatcher.pubsub in dispatcher.pubsub.broker.clients

    @staticmethod
    def assert_connections_released(dispatcher: AsyncInMemoryDispatcher) -> None:
        # `stop()` unlinks the pubsub from its broker
        assert dispatcher.pubsub not in dispatcher.pubsub.broker.clients


REAL_BROKERS = [AMQPBackend, RedisBackend]
Backend = type[AMQPBackend | RedisBackend | InMemoryBackend]


@pytest.fixture(
    params=[*REAL_BROKERS, InMemoryBackend], ids=["amqp", "redis", "in_memory"])
def backend(request) -> Backend:
    return request.param


@pytest.fixture
def url(backend) -> str | None:
    """The backend's broker URL, skipping the test when it is not running."""
    if backend.url is not None:
        require_broker(backend.url)
    return backend.url


real_brokers_only = pytest.mark.parametrize(
    "backend", REAL_BROKERS, ids=["amqp", "redis"], indirect=True)


@real_brokers_only
class TestBrokerConnection:
    async def test_unreachable_broker(self, backend, namespace):
        dispatcher = backend.make_dispatcher(namespace, backend.unreachable_url)

        assert await dispatcher._broker_reachable() is False
        with pytest.raises(ConnectionError):
            await dispatcher.connect()
        assert dispatcher.connected is False

    async def test_reachable_broker(self, backend, namespace, url):
        dispatcher = backend.make_dispatcher(namespace, url)

        assert await dispatcher._broker_reachable() is True
        await dispatcher.connect()
        assert dispatcher.connected is True

        await dispatcher._clear_connections()

    async def test_emit_without_broker_returns_false(self, backend, namespace):
        dispatcher = backend.make_dispatcher(namespace, backend.unreachable_url)

        assert await dispatcher.emit("ping", {"key": "value"}) is False


class TestDispatcher:
    async def test_lifecycle(self, backend, namespace, url):
        handler = AsyncPingPong()
        dispatcher = backend.make_dispatcher(namespace, url, handler)

        await dispatcher.start(block=False)
        await wait_until(lambda: dispatcher.running)
        assert dispatcher.connected is True
        backend.assert_connections_open(dispatcher)
        sid, data = await handler.expect("connect")
        assert data == {"REMOTE_ADDR": namespace}

        await dispatcher.stop()
        assert dispatcher.running is False
        assert dispatcher.connected is False
        await handler.expect("disconnect")
        # The broker connections are released, not just the flags
        backend.assert_connections_released(dispatcher)

    async def test_ping_pong(self, backend, url):
        client_handler = AsyncPingPong(namespace="server")
        server_handler = AsyncPingPong(namespace="client")
        client = backend.make_dispatcher("client", url, client_handler)
        server = backend.make_dispatcher("server", url, server_handler)

        async with running(server), running(client):
            await client_handler.emit("ping", {"key": "value"})

            sid, data = await server_handler.expect("ping")
            assert sid == client.host_uid
            assert data == {"key": "value"}

            sid, data = await client_handler.expect("pong")
            assert sid == server.host_uid
            assert data == {"key": "value"}

    async def test_namespaces_are_isolated(self, backend, namespace, url):
        handler = AsyncPingPong()
        dispatcher = backend.make_dispatcher(namespace, url, handler)
        emitter = backend.make_dispatcher("emitter", url)

        async with running(dispatcher):
            await emitter.emit(
                "pong", "other namespace", namespace=f"{namespace}-other")
            # The marker is sent second, so seeing it first means the event
            # sent to the other namespace was not delivered
            await emitter.emit("pong", "marker", namespace=namespace)

            sid, data = await handler.expect("pong")
            assert data == "marker"


@real_brokers_only
class TestConnectionLoss:
    async def test_reconnects_after_connection_loss(self, backend, namespace, url):
        handler = AsyncPingPong()
        dispatcher = backend.make_dispatcher(namespace, url, handler)

        async with running(dispatcher):
            await handler.expect("connect")

            await backend.drop_listener_connection(dispatcher)

            await handler.expect("disconnect")
            await wait_until(lambda: dispatcher.reconnecting)
            assert dispatcher.running is True

            # The reconnection loop waits 1 s before its first attempt
            await handler.expect("connect", timeout=3.0)
            assert dispatcher.connected is True
            assert dispatcher.reconnecting is False

            await wait_until_listening(dispatcher)

    async def test_stops_after_connection_loss_without_reconnection(
            self, backend, namespace, url):
        handler = AsyncPingPong()
        dispatcher = backend.make_dispatcher(
            namespace, url, handler, reconnection=False)

        async with running(dispatcher):
            await handler.expect("connect")

            await backend.drop_listener_connection(dispatcher)

            await handler.expect("disconnect")
            await wait_until(lambda: not dispatcher.running)

            assert dispatcher.stopped is True
            assert dispatcher.connected is False


@pytest.mark.parametrize("backend", [AMQPBackend], ids=["amqp"], indirect=True)
class TestAMQPQueues:
    async def test_failed_publish_drops_the_connection(self, backend, namespace):
        dispatcher = backend.make_dispatcher(namespace, backend.unreachable_url)

        assert await dispatcher.emit("ping", {"key": "value"}) is False
        # `_publish` drops the failed connection so the next attempt starts clean
        assert dispatcher._publisher_connection is None

    async def test_same_namespace_shares_a_queue(self, backend, namespace, url):
        """Two dispatchers on one namespace consume the same RabbitMQ queue, so
        each message is delivered to only one of them (work-queue semantics)."""
        handler1 = AsyncPingPong()
        handler2 = AsyncPingPong()
        dispatcher1 = backend.make_dispatcher(namespace, url, handler1)
        dispatcher2 = backend.make_dispatcher(namespace, url, handler2)
        emitter = backend.make_dispatcher("emitter", url)

        async with running(dispatcher1), running(dispatcher2):
            for i in range(4):
                await emitter.emit("pong", i, namespace=namespace)
            # Give the broker time to deliver everything before counting
            await asyncio.sleep(0.2)

        received = handler1.events.qsize() + handler2.events.qsize()
        # Each dispatcher also recorded its own `connect` and `disconnect`
        assert received == 4 + 2 * 2

    async def test_same_namespace_distinct_queues_fan_out(
            self, backend, namespace, url):
        """Giving each dispatcher its own queue name binds both queues to the
        namespace routing key, so every dispatcher receives every message."""
        handler1 = AsyncPingPong()
        handler2 = AsyncPingPong()
        dispatcher1 = backend.make_dispatcher(
            namespace, url, handler1, queue_options={"name": f"{namespace}-1"})
        dispatcher2 = backend.make_dispatcher(
            namespace, url, handler2, queue_options={"name": f"{namespace}-2"})
        emitter = backend.make_dispatcher("emitter", url)

        async with running(dispatcher1), running(dispatcher2):
            await emitter.emit("pong", {"key": "value"}, namespace=namespace)

            sid, data = await handler1.expect("pong")
            assert sid == emitter.host_uid
            sid, data = await handler2.expect("pong")
            assert sid == emitter.host_uid


@pytest.mark.parametrize(
    "backend", [RedisBackend, InMemoryBackend], ids=["redis", "in_memory"],
    indirect=True)
class TestPubSubChannels:
    async def test_same_namespace_fans_out(self, backend, namespace, url):
        """Two dispatchers on one namespace subscribe to the same channel, so
        every message is delivered to both of them (pub/sub semantics)."""
        handler1 = AsyncPingPong()
        handler2 = AsyncPingPong()
        dispatcher1 = backend.make_dispatcher(namespace, url, handler1)
        dispatcher2 = backend.make_dispatcher(namespace, url, handler2)
        emitter = backend.make_dispatcher("emitter", url)

        async with running(dispatcher1), running(dispatcher2):
            for i in range(4):
                await emitter.emit("pong", i, namespace=namespace)
            # Give the broker time to deliver everything before counting
            await asyncio.sleep(0.2)

        received = handler1.events.qsize() + handler2.events.qsize()
        # Each dispatcher also recorded its own `connect` and `disconnect`
        assert received == 2 * 4 + 2 * 2


@pytest.mark.parametrize("backend", [RedisBackend], ids=["redis"], indirect=True)
class TestRedisChannels:
    async def test_custom_name_subscribes_to_both_channels(
            self, backend, namespace, url):
        """A dispatcher given a queue name listens on that channel on top of
        its namespace, like a named queue is also bound to the namespace."""
        handler = AsyncPingPong()
        dispatcher = backend.make_dispatcher(
            namespace, url, handler, queue_options={"name": f"{namespace}-1"})
        emitter = backend.make_dispatcher("emitter", url)

        async with running(dispatcher):
            await emitter.emit("pong", "to namespace", namespace=namespace)
            sid, data = await handler.expect("pong")
            assert data == "to namespace"

            await emitter.emit("pong", "to name", namespace=f"{namespace}-1")
            sid, data = await handler.expect("pong")
            assert data == "to name"
