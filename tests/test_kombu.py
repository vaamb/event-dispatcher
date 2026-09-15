from contextlib import contextmanager
import socket
from threading import Event
import time

import pytest

from dispatcher import KombuDispatcher

from handlers import PingPong


UNREACHABLE_URL = "amqp://guest:guest@localhost:1//"


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


def wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError("Condition not met in time")
        time.sleep(0.01)


def wait_until_listening(dispatcher: KombuDispatcher) -> None:
    """Block until the dispatcher's queue is bound and consumed.

    `start(block=False)` returns before `_listen()` has declared and bound the
    queue, and the exchange silently drops messages routed to a namespace no
    queue is bound to yet. So probe the dispatcher with messages addressed to
    itself until one comes back.
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


def drop_listener_connection(dispatcher: KombuDispatcher) -> None:
    """Simulate a network drop: kill the socket under the listener thread.

    A regular `close()` from another thread would run the AMQP close
    handshake on a connection the listener thread is reading, which is racy.
    """
    dispatcher.listener_connection.connection.sock.shutdown(socket.SHUT_RDWR)


@contextmanager
def running(dispatcher: KombuDispatcher):
    dispatcher.start(block=False)
    wait_until_listening(dispatcher)
    try:
        yield dispatcher
    finally:
        if dispatcher.running:
            dispatcher.stop()


class TestBrokerConnection:
    def test_unreachable_broker(self, namespace):
        dispatcher = make_dispatcher(namespace, UNREACHABLE_URL)

        assert dispatcher._broker_reachable() is False
        with pytest.raises(ConnectionError):
            dispatcher.connect()
        assert dispatcher.connected is False

    def test_reachable_broker(self, namespace, rabbitmq_url):
        dispatcher = make_dispatcher(namespace, rabbitmq_url)

        assert dispatcher._broker_reachable() is True
        dispatcher.connect()
        assert dispatcher.connected is True

        dispatcher.listener_connection.close()

    def test_lifecycle(self, namespace, rabbitmq_url):
        handler = PingPong()
        dispatcher = make_dispatcher(namespace, rabbitmq_url, handler)

        dispatcher.start(block=False)
        wait_until(lambda: dispatcher.running)
        assert dispatcher.connected is True
        assert dispatcher.listener_connection.connected is True
        sid, data = handler.expect("connect")
        assert data == {"REMOTE_ADDR": namespace}

        dispatcher.stop()
        assert dispatcher.running is False
        assert dispatcher.connected is False
        handler.expect("disconnect")
        # The broker connections are released, not just the flags
        assert dispatcher.publisher_connection.connected is False
        assert dispatcher.listener_connection.connected is False

    def test_emit_without_broker_returns_false(self, namespace):
        dispatcher = make_dispatcher(namespace, UNREACHABLE_URL)

        assert dispatcher.emit("ping", {"key": "value"}) is False


class TestMessaging:
    def test_ping_pong(self, rabbitmq_url):
        client_handler = PingPong(namespace="server")
        server_handler = PingPong(namespace="client")
        client = make_dispatcher("client", rabbitmq_url, client_handler)
        server = make_dispatcher("server", rabbitmq_url, server_handler)

        with running(server), running(client):
            client_handler.emit("ping", {"key": "value"})

            sid, data = server_handler.expect("ping")
            assert sid == client.host_uid
            assert data == {"key": "value"}

            sid, data = client_handler.expect("pong")
            assert sid == server.host_uid
            assert data == {"key": "value"}

    def test_same_namespace_shares_a_queue(self, namespace, rabbitmq_url):
        """Two dispatchers on one namespace consume the same RabbitMQ queue, so
        each message is delivered to only one of them (work-queue semantics)."""
        handler1 = PingPong()
        handler2 = PingPong()
        dispatcher1 = make_dispatcher(namespace, rabbitmq_url, handler1)
        dispatcher2 = make_dispatcher(namespace, rabbitmq_url, handler2)
        emitter = make_dispatcher("emitter", rabbitmq_url)

        with running(dispatcher1), running(dispatcher2):
            for i in range(4):
                emitter.emit("pong", i, namespace=namespace)
            # Give the broker time to deliver everything before counting
            time.sleep(0.2)

        received = handler1.events.qsize() + handler2.events.qsize()
        # Each dispatcher also recorded its own `connect` and `disconnect`
        assert received == 4 + 2 * 2

    def test_same_namespace_distinct_queues_fan_out(self, namespace, rabbitmq_url):
        """Giving each dispatcher its own queue name binds both queues to the
        namespace routing key, so every dispatcher receives every message."""
        handler1 = PingPong()
        handler2 = PingPong()
        dispatcher1 = make_dispatcher(
            namespace, rabbitmq_url, handler1,
            queue_options={"name": f"{namespace}-1"})
        dispatcher2 = make_dispatcher(
            namespace, rabbitmq_url, handler2,
            queue_options={"name": f"{namespace}-2"})
        emitter = make_dispatcher("emitter", rabbitmq_url)

        with running(dispatcher1), running(dispatcher2):
            emitter.emit("pong", {"key": "value"}, namespace=namespace)

            sid, data = handler1.expect("pong")
            assert sid == emitter.host_uid
            sid, data = handler2.expect("pong")
            assert sid == emitter.host_uid


class TestConnectionLoss:
    def test_reconnects_after_connection_loss(self, namespace, rabbitmq_url):
        handler = PingPong()
        dispatcher = make_dispatcher(namespace, rabbitmq_url, handler)

        with running(dispatcher):
            handler.expect("connect")

            drop_listener_connection(dispatcher)

            handler.expect("disconnect")
            wait_until(lambda: dispatcher.reconnecting)
            assert dispatcher.running is True

            # The reconnection loop waits 1 s before its first attempt
            handler.expect("connect", timeout=3.0)
            assert dispatcher.connected is True
            assert dispatcher.reconnecting is False

            wait_until_listening(dispatcher)

    def test_stops_after_connection_loss_without_reconnection(
            self, namespace, rabbitmq_url):
        handler = PingPong()
        dispatcher = make_dispatcher(
            namespace, rabbitmq_url, handler, reconnection=False)

        with running(dispatcher):
            handler.expect("connect")

            drop_listener_connection(dispatcher)

            handler.expect("disconnect")
            wait_until(lambda: not dispatcher.running)
            assert dispatcher.stopped is True
            assert dispatcher.connected is False
