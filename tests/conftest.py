import asyncio
from contextlib import asynccontextmanager, contextmanager
from functools import cache
import socket
from threading import Event
import time
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from dispatcher.ABC import AsyncDispatcher, Dispatcher


RABBITMQ_URL = "amqp://guest:guest@localhost:5672//"
REDIS_URL = "redis://localhost:6379/0"


@cache
def _is_listening(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def require_broker(url: str) -> None:
    """Skip the current test when no broker is listening at `url`."""
    parts = urlsplit(url)
    assert parts.hostname is not None and parts.port is not None
    if not _is_listening(parts.hostname, parts.port):
        pytest.skip(f"No broker reachable at {parts.hostname}:{parts.port}")


@pytest.fixture
def namespace() -> str:
    """A namespace unique to the test so runs never share broker queues."""
    return f"test-{uuid4().hex[:8]}"


# Sync helpers

def wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError("Condition not met in time")
        time.sleep(0.01)


def wait_until_listening(dispatcher: Dispatcher) -> None:
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
def running(dispatcher: Dispatcher):
    dispatcher.start(block=False)
    wait_until_listening(dispatcher)
    try:
        yield dispatcher
    finally:
        if dispatcher.running:
            dispatcher.stop()


# Async helpers

async def async_wait_until(predicate, timeout: float = 2.0) -> None:
    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_poll(), timeout)


async def async_wait_until_listening(dispatcher: AsyncDispatcher) -> None:
    """Async counterpart of `wait_until_listening`, see its docstring."""
    # `_broker_reachable()` resets both connections, so emitting before
    # `connect()` is done would close the listener connection under it
    await async_wait_until(lambda: dispatcher.connected)
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
async def async_running(dispatcher: AsyncDispatcher):
    await dispatcher.start(block=False)
    await async_wait_until_listening(dispatcher)
    try:
        yield dispatcher
    finally:
        if dispatcher.running:
            await dispatcher.stop()
