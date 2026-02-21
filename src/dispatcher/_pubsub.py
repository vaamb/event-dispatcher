"""A simple in-memory pub_sub message broker to be used by the dispatcher."""

from __future__ import annotations

from abc import ABC
import asyncio
from queue import Queue, Empty
from typing import Generic, Self, TypeVar


PubSubT = TypeVar("PubSubT")

class NoMessage(Exception):
    pass


class BasePubSub(ABC):
    __slots__ = ("channels",)

    def __init__(self) -> None:
        self.channels: set[str] = set()

    def subscribe(self, channel: str) -> None:
        self.channels.add(channel)

    def unsubscribe(self, channel: str | None = None) -> None:
        if channel:
            self.channels.discard(channel)
        else:
            self.channels.clear()

    @property
    def subscribed(self) -> bool:
        return bool(self.channels)


class StupidPubSub(BasePubSub):
    __slots__ = ("broker", "messages")

    def __init__(self, namespace: str, queue_length: int = 0) -> None:
        super().__init__()
        self.broker: Broker = Broker.get_broker(namespace)
        self.broker.link(self)
        self.messages = Queue(maxsize=queue_length)

    def publish(self, namespace: str, message: dict | bytes) -> int:
        broker = Broker._brokers.get(namespace)
        if broker is None:
            return 0
        payload = {"namespace": namespace, "data": message}
        published = broker.push(payload)
        return published

    def listen(self, timeout: float | None = None) -> dict | bytes:
        try:
            return self.messages.get(block=True, timeout=timeout)
        except Empty:
            raise NoMessage()


class AsyncPubSub(BasePubSub):
    __slots__ = ("broker", "messages")

    def __init__(self, namespace: str, queue_length: int = 0) -> None:
        super().__init__()
        self.broker: AsyncBroker = AsyncBroker.get_broker(namespace)
        self.broker.link(self)
        self.messages = asyncio.Queue(maxsize=queue_length)

    async def publish(self, namespace: str, message: dict | bytes) -> int:
        broker = AsyncBroker._brokers.get(namespace)
        if broker is None:
            return 0
        payload = {"namespace": namespace, "data": message}
        published = await broker.push(payload)
        return published

    async def listen(self, timeout: float | None = None) -> dict | bytes:
        try:
            return await asyncio.wait_for(self.messages.get(), timeout)
        except asyncio.TimeoutError:
            raise NoMessage()


class BaseBroker(Generic[PubSubT]):
    _brokers: dict[str, Self] = {}

    __slots__ = ("clients",)

    def __init__(self) -> None:
        self.clients: set[PubSubT] = set()

    @classmethod
    def get_broker(cls, namespace: str) -> Self:
        try:
            return cls._brokers[namespace]
        except KeyError:
            broker = cls()
            cls._brokers[namespace] = broker
            return broker

    def link(self, client: PubSubT) -> None:
        self.clients.add(client)

    def unlink(self, client: PubSubT) -> None:
        self.clients.discard(client)
        if not self.clients:
            self.__class__._brokers = {
                k: v for k, v in self.__class__._brokers.items() if v is not self
            }


class Broker(BaseBroker[StupidPubSub]):
    _brokers: dict[str, Self] = {}

    __slots__ = ()

    def push(self, payload: dict | bytes) -> int:
        pushed = 0
        for client in self.clients:
            if payload["namespace"] in client.channels:
                data = payload["data"]
                client.messages.put(data)
                pushed += 1
        return pushed


class AsyncBroker(BaseBroker[AsyncPubSub]):
    _brokers: dict[str, Self] = {}

    __slots__ = ()

    async def push(self, payload: dict | bytes) -> int:
        pushed = 0
        for client in self.clients:
            if payload["namespace"] in client.channels:
                data = payload["data"]
                await client.messages.put(data)
                pushed += 1
        return pushed
