import socket
from uuid import uuid4

import pytest


RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_URL = f"amqp://guest:guest@{RABBITMQ_HOST}:{RABBITMQ_PORT}//"


@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    """Skip the requesting tests when no RabbitMQ broker is listening locally."""
    try:
        with socket.create_connection((RABBITMQ_HOST, RABBITMQ_PORT), timeout=1):
            pass
    except OSError:
        pytest.skip(f"No RabbitMQ broker reachable at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    return RABBITMQ_URL


@pytest.fixture
def namespace() -> str:
    """A namespace unique to the test so runs never share broker queues."""
    return f"test-{uuid4().hex[:8]}"
