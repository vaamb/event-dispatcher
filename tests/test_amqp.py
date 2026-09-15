import asyncio
from contextlib import asynccontextmanager

import pytest

from dispatcher import AsyncAMQPDispatcher

from handlers import AsyncPingPong


pytestmark = pytest.mark.asyncio

UNREACHABLE_URL = "amqp://guest:guest@localhost:1//"


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


async def wait_until(predicate, timeout: float = 2.0) -> None:
    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_poll(), timeout)


async def wait_until_listening(dispatcher: AsyncAMQPDispatcher) -> None:
    """Block until the dispatcher's queue is bound and consumed.

    `start(block=False)` returns before `_listen()` has declared and bound the
    queue, and the exchange silently drops messages routed to a namespace no
    queue is bound to yet. So probe the dispatcher with messages addressed to
    itself until one comes back.
    """
    # `_broker_reachable()` resets both connections, so emitting before
    # `connect()` is done would close the listener connection under it
    await wait_until(lambda: dispatcher.connected)
    probe_received = asyncio.Event()

    @dispatcher.on("probe")
    async def on_probe() -> None:
        probe_received.set()

    async def _probe() -> None:
        while not probe_received.is_set():
            await dispatcher.emit("probe", to=dispatcher.host_uid)
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_probe(), 2.0)


@asynccontextmanager
async def running(dispatcher: AsyncAMQPDispatcher):
    await dispatcher.start(block=False)
    await wait_until_listening(dispatcher)
    try:
        yield dispatcher
    finally:
        if dispatcher.running:
            await dispatcher.stop()


class TestBrokerConnection:
    async def test_unreachable_broker(self, namespace):
        dispatcher = make_dispatcher(namespace, UNREACHABLE_URL)

        assert await dispatcher._broker_reachable() is False
        with pytest.raises(ConnectionError):
            await dispatcher.connect()
        assert dispatcher.connected is False

    async def test_reachable_broker(self, namespace, rabbitmq_url):
        dispatcher = make_dispatcher(namespace, rabbitmq_url)

        assert await dispatcher._broker_reachable() is True
        await dispatcher.connect()
        assert dispatcher.connected is True

        await dispatcher._clear_connections()

    async def test_lifecycle(self, namespace, rabbitmq_url):
        handler = AsyncPingPong()
        dispatcher = make_dispatcher(namespace, rabbitmq_url, handler)

        await dispatcher.start(block=False)
        await wait_until(lambda: dispatcher.running)
        assert dispatcher.connected is True
        assert dispatcher._listener_connection is not None
        sid, data = await handler.expect("connect")
        assert data == {"REMOTE_ADDR": namespace}

        await dispatcher.stop()
        assert dispatcher.running is False
        assert dispatcher.connected is False
        await handler.expect("disconnect")
        # The AMQP connections are released, not just the flags
        assert dispatcher._publisher_connection is None
        assert dispatcher._listener_connection is None

    async def test_emit_without_broker_returns_false(self, namespace):
        dispatcher = make_dispatcher(namespace, UNREACHABLE_URL)

        assert await dispatcher.emit("ping", {"key": "value"}) is False
        # `_publish` drops the failed connection so the next attempt starts clean
        assert dispatcher._publisher_connection is None


class TestMessaging:
    async def test_ping_pong(self, rabbitmq_url):
        client_handler = AsyncPingPong(namespace="server")
        server_handler = AsyncPingPong(namespace="client")
        client = make_dispatcher("client", rabbitmq_url, client_handler)
        server = make_dispatcher("server", rabbitmq_url, server_handler)

        async with running(server), running(client):
            await client_handler.emit("ping", {"key": "value"})

            sid, data = await server_handler.expect("ping")
            assert sid == client.host_uid
            assert data == {"key": "value"}

            sid, data = await client_handler.expect("pong")
            assert sid == server.host_uid
            assert data == {"key": "value"}

    async def test_same_namespace_shares_a_queue(self, namespace, rabbitmq_url):
        """Two dispatchers on one namespace consume the same RabbitMQ queue, so
        each message is delivered to only one of them (work-queue semantics)."""
        handler1 = AsyncPingPong()
        handler2 = AsyncPingPong()
        dispatcher1 = make_dispatcher(namespace, rabbitmq_url, handler1)
        dispatcher2 = make_dispatcher(namespace, rabbitmq_url, handler2)
        emitter = make_dispatcher("emitter", rabbitmq_url)

        async with running(dispatcher1), running(dispatcher2):
            for i in range(4):
                await emitter.emit("pong", i, namespace=namespace)
            # Give the broker time to deliver everything before counting
            await asyncio.sleep(0.2)

        received = handler1.events.qsize() + handler2.events.qsize()
        # Each dispatcher also recorded its own `connect` and `disconnect`
        assert received == 4 + 2 * 2

    async def test_same_namespace_distinct_queues_fan_out(
            self, namespace, rabbitmq_url):
        """Giving each dispatcher its own queue name binds both queues to the
        namespace routing key, so every dispatcher receives every message."""
        handler1 = AsyncPingPong()
        handler2 = AsyncPingPong()
        dispatcher1 = make_dispatcher(
            namespace, rabbitmq_url, handler1,
            queue_options={"name": f"{namespace}-1"})
        dispatcher2 = make_dispatcher(
            namespace, rabbitmq_url, handler2,
            queue_options={"name": f"{namespace}-2"})
        emitter = make_dispatcher("emitter", rabbitmq_url)

        async with running(dispatcher1), running(dispatcher2):
            await emitter.emit("pong", {"key": "value"}, namespace=namespace)

            sid, data = await handler1.expect("pong")
            assert sid == emitter.host_uid
            sid, data = await handler2.expect("pong")
            assert sid == emitter.host_uid


class TestConnectionLoss:
    async def test_reconnects_after_connection_loss(self, namespace, rabbitmq_url):
        handler = AsyncPingPong()
        dispatcher = make_dispatcher(namespace, rabbitmq_url, handler)

        async with running(dispatcher):
            await handler.expect("connect")

            # Closing the listener connection fires the `close_callbacks`
            # that `_listen()` relies on to notice a broker disconnect
            await dispatcher.listener_connection.close()

            await handler.expect("disconnect")
            await wait_until(lambda: dispatcher.reconnecting)
            assert dispatcher.running is True

            # The reconnection loop waits 1 s before its first attempt
            await handler.expect("connect", timeout=3.0)
            assert dispatcher.connected is True
            assert dispatcher.reconnecting is False

            await wait_until_listening(dispatcher)

    async def test_stops_after_connection_loss_without_reconnection(
            self, namespace, rabbitmq_url):
        handler = AsyncPingPong()
        dispatcher = make_dispatcher(
            namespace, rabbitmq_url, handler, reconnection=False)

        async with running(dispatcher):
            await handler.expect("connect")

            await dispatcher.listener_connection.close()

            await handler.expect("disconnect")
            await wait_until(lambda: not dispatcher.running)

            assert dispatcher.stopped is True
            assert dispatcher.connected is False
