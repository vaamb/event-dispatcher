"""Tests for the sync dispatchers backed by a real message broker.

The contract tests run against every backend (see `backend`), each one
providing the few pieces that differ: how to build a dispatcher, how to
simulate a dropped connection and which internals show connections were
released. Delivery semantics differ between a RabbitMQ queue and a Redis
channel, so those tests are backend-specific (see the end of the module).
"""
from contextlib import contextmanager
import socket
from threading import Event
import time

import pytest

from dispatcher import KombuDispatcher, RedisDispatcher

from conftest import RABBITMQ_URL, REDIS_URL, require_broker
from handlers import PingPong


class KombuBackend:
    url = RABBITMQ_URL
    unreachable_url = "amqp://guest:guest@localhost:1//"

    @staticmethod
    def make_dispatcher(
            namespace: str,
            url: str,
            handler: PingPong | None = None,
            **kwargs,
    ) -> KombuDispatcher:
        # `auto_delete` removes the queue from the broker once its last consumer
        # is gone, so tests do not leave queues (and pending messages) behind
        queue_options = {"auto_delete": True, **kwargs.pop("queue_options", {})}
        dispatcher = KombuDispatcher(
            namespace, url=url, queue_options=queue_options, **kwargs)
        if handler is not None:
            dispatcher.register_event_handler(handler)
        return dispatcher

    @staticmethod
    def drop_listener_connection(dispatcher: KombuDispatcher) -> None:
        """Simulate a network drop: kill the socket under the listener thread.

        A regular `close()` from another thread would run the AMQP close
        handshake on a connection the listener thread is reading, which is racy.
        """
        dispatcher.listener_connection.connection.sock.shutdown(socket.SHUT_RDWR)

    @staticmethod
    def assert_connections_open(dispatcher: KombuDispatcher) -> None:
        assert dispatcher.listener_connection.connected is True

    @staticmethod
    def assert_connections_released(dispatcher: KombuDispatcher) -> None:
        assert dispatcher.publisher_connection.connected is False
        assert dispatcher.listener_connection.connected is False


class RedisBackend:
    url = REDIS_URL
    unreachable_url = "redis://localhost:1/0"

    @staticmethod
    def make_dispatcher(
            namespace: str,
            url: str,
            handler: PingPong | None = None,
            **kwargs,
    ) -> RedisDispatcher:
        # Redis pub/sub keeps no state on the server, so nothing to clean up
        dispatcher = RedisDispatcher(namespace, url=url, **kwargs)
        if handler is not None:
            dispatcher.register_event_handler(handler)
        return dispatcher

    @staticmethod
    def drop_listener_connection(dispatcher: RedisDispatcher) -> None:
        """Simulate a network drop: kill the socket under the listener thread.

        `connection.disconnect()` would also drop the socket reference the
        listener thread is reading on.
        """
        dispatcher.pubsub.connection._sock.shutdown(socket.SHUT_RDWR)

    @staticmethod
    def assert_connections_open(dispatcher: RedisDispatcher) -> None:
        assert dispatcher._redis is not None

    @staticmethod
    def assert_connections_released(dispatcher: RedisDispatcher) -> None:
        assert dispatcher._pubsub is None
        assert dispatcher._redis is None


@pytest.fixture(params=[KombuBackend, RedisBackend], ids=["kombu", "redis"])
def backend(request) -> type[KombuBackend | RedisBackend]:
    return request.param


@pytest.fixture
def url(backend) -> str:
    """The backend's broker URL, skipping the test when it is not running."""
    require_broker(backend.url)
    return backend.url


def wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError("Condition not met in time")
        time.sleep(0.01)


def wait_until_listening(dispatcher: KombuDispatcher | RedisDispatcher) -> None:
    """Block until the dispatcher's queue is bound and consumed.

    `start(block=False)` returns before `_listen()` has declared and bound the
    queue (or subscribed to the channel), and messages sent to a namespace no
    one listens to yet are silently dropped. So probe the dispatcher with
    messages addressed to itself until one comes back.
    """
    wait_until(lambda: dispatcher.connected)
    probe_received = Event()

    @dispatcher.on("probe")
    def on_probe() -> None:
        probe_received.set()

    deadline = time.monotonic() + 2.0
    while not probe_received.is_set():
        if time.monotonic() > deadline:
            raise TimeoutError("Dispatcher did not start listening in time")
        dispatcher.emit("probe", to=dispatcher.host_uid)
        time.sleep(0.05)


@contextmanager
def running(dispatcher: KombuDispatcher | RedisDispatcher):
    dispatcher.start(block=False)
    wait_until_listening(dispatcher)
    try:
        yield dispatcher
    finally:
        if dispatcher.running:
            dispatcher.stop()


class TestBrokerConnection:
    def test_unreachable_broker(self, backend, namespace):
        dispatcher = backend.make_dispatcher(namespace, backend.unreachable_url)

        assert dispatcher._broker_reachable() is False
        with pytest.raises(ConnectionError):
            dispatcher.connect()
        assert dispatcher.connected is False

    def test_reachable_broker(self, backend, namespace, url):
        dispatcher = backend.make_dispatcher(namespace, url)

        assert dispatcher._broker_reachable() is True
        dispatcher.connect()
        assert dispatcher.connected is True

        dispatcher._clear_connections()

    def test_lifecycle(self, backend, namespace, url):
        handler = PingPong()
        dispatcher = backend.make_dispatcher(namespace, url, handler)

        dispatcher.start(block=False)
        wait_until(lambda: dispatcher.running)
        assert dispatcher.connected is True
        backend.assert_connections_open(dispatcher)
        sid, data = handler.expect("connect")
        assert data == {"REMOTE_ADDR": namespace}

        dispatcher.stop()
        assert dispatcher.running is False
        assert dispatcher.connected is False
        handler.expect("disconnect")
        # The broker connections are released, not just the flags
        backend.assert_connections_released(dispatcher)

    def test_emit_without_broker_returns_false(self, backend, namespace):
        dispatcher = backend.make_dispatcher(namespace, backend.unreachable_url)

        assert dispatcher.emit("ping", {"key": "value"}) is False


class TestMessaging:
    def test_ping_pong(self, backend, url):
        client_handler = PingPong(namespace="server")
        server_handler = PingPong(namespace="client")
        client = backend.make_dispatcher("client", url, client_handler)
        server = backend.make_dispatcher("server", url, server_handler)

        with running(server), running(client):
            client_handler.emit("ping", {"key": "value"})

            sid, data = server_handler.expect("ping")
            assert sid == client.host_uid
            assert data == {"key": "value"}

            sid, data = client_handler.expect("pong")
            assert sid == server.host_uid
            assert data == {"key": "value"}


class TestConnectionLoss:
    def test_reconnects_after_connection_loss(self, backend, namespace, url):
        handler = PingPong()
        dispatcher = backend.make_dispatcher(namespace, url, handler)

        with running(dispatcher):
            handler.expect("connect")

            backend.drop_listener_connection(dispatcher)

            handler.expect("disconnect")
            wait_until(lambda: dispatcher.reconnecting)
            assert dispatcher.running is True

            # The reconnection loop waits 1 s before its first attempt
            handler.expect("connect", timeout=3.0)
            assert dispatcher.connected is True
            assert dispatcher.reconnecting is False

            wait_until_listening(dispatcher)

    def test_stops_after_connection_loss_without_reconnection(
            self, backend, namespace, url):
        handler = PingPong()
        dispatcher = backend.make_dispatcher(
            namespace, url, handler, reconnection=False)

        with running(dispatcher):
            handler.expect("connect")

            backend.drop_listener_connection(dispatcher)

            handler.expect("disconnect")
            wait_until(lambda: not dispatcher.running)
            assert dispatcher.stopped is True
            assert dispatcher.connected is False


@pytest.mark.parametrize("backend", [KombuBackend], ids=["kombu"], indirect=True)
class TestKombuQueues:
    def test_same_namespace_shares_a_queue(self, backend, namespace, url):
        """Two dispatchers on one namespace consume the same RabbitMQ queue, so
        each message is delivered to only one of them (work-queue semantics)."""
        handler1 = PingPong()
        handler2 = PingPong()
        dispatcher1 = backend.make_dispatcher(namespace, url, handler1)
        dispatcher2 = backend.make_dispatcher(namespace, url, handler2)
        emitter = backend.make_dispatcher("emitter", url)

        with running(dispatcher1), running(dispatcher2):
            for i in range(4):
                emitter.emit("pong", i, namespace=namespace)
            # Give the broker time to deliver everything before counting
            time.sleep(0.2)

        received = handler1.events.qsize() + handler2.events.qsize()
        # Each dispatcher also recorded its own `connect` and `disconnect`
        assert received == 4 + 2 * 2

    def test_same_namespace_distinct_queues_fan_out(self, backend, namespace, url):
        """Giving each dispatcher its own queue name binds both queues to the
        namespace routing key, so every dispatcher receives every message."""
        handler1 = PingPong()
        handler2 = PingPong()
        dispatcher1 = backend.make_dispatcher(
            namespace, url, handler1, queue_options={"name": f"{namespace}-1"})
        dispatcher2 = backend.make_dispatcher(
            namespace, url, handler2, queue_options={"name": f"{namespace}-2"})
        emitter = backend.make_dispatcher("emitter", url)

        with running(dispatcher1), running(dispatcher2):
            emitter.emit("pong", {"key": "value"}, namespace=namespace)

            sid, data = handler1.expect("pong")
            assert sid == emitter.host_uid
            sid, data = handler2.expect("pong")
            assert sid == emitter.host_uid


@pytest.mark.parametrize("backend", [RedisBackend], ids=["redis"], indirect=True)
class TestRedisChannels:
    def test_same_namespace_fans_out(self, backend, namespace, url):
        """Two dispatchers on one namespace subscribe to the same Redis channel,
        so every message is delivered to both of them (pub/sub semantics)."""
        handler1 = PingPong()
        handler2 = PingPong()
        dispatcher1 = backend.make_dispatcher(namespace, url, handler1)
        dispatcher2 = backend.make_dispatcher(namespace, url, handler2)
        emitter = backend.make_dispatcher("emitter", url)

        with running(dispatcher1), running(dispatcher2):
            for i in range(4):
                emitter.emit("pong", i, namespace=namespace)
            # Give the broker time to deliver everything before counting
            time.sleep(0.2)

        received = handler1.events.qsize() + handler2.events.qsize()
        # Each dispatcher also recorded its own `connect` and `disconnect`
        assert received == 2 * 4 + 2 * 2

    def test_custom_name_subscribes_to_both_channels(self, backend, namespace, url):
        """A dispatcher given a queue name listens on that channel on top of
        its namespace, like a named queue is also bound to the namespace."""
        handler = PingPong()
        dispatcher = backend.make_dispatcher(
            namespace, url, handler, queue_options={"name": f"{namespace}-1"})
        emitter = backend.make_dispatcher("emitter", url)

        with running(dispatcher):
            emitter.emit("pong", "to namespace", namespace=namespace)
            sid, data = handler.expect("pong")
            assert data == "to namespace"

            emitter.emit("pong", "to name", namespace=f"{namespace}-1")
            sid, data = handler.expect("pong")
            assert data == "to name"
