from functools import cache
import socket
from urllib.parse import urlsplit
from uuid import uuid4

import pytest


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
