"""A simple in-memory pub_sub message broker to be used by the dispatcher."""

from __future__ import annotations

import asyncio
from queue import Queue, Empty


class Broker:
    __slots__ = ["clients"]

    def __init__(self) -> None:
        self.clients: set["StupidPubSub"] = set()

    def link(self, client: "StupidPubSub") -> None:
        self.clients.add(client)

    def push(self, payload: dict | bytes) -> int:
        pushed = 0
        for client in self.clients:
            if payload["namespace"] in client.channels:
                data = payload["data"]
                client.messages.put(data)
                pushed += 1
        return pushed


class AsyncBroker(Broker):
    async def push(self, payload: dict | bytes) -> int:
        pushed = 0
        self.clients: set["AsyncPubSub"]
        for client in self.clients:
            if payload["namespace"] in client.channels:
                data = payload["data"]
                await client.messages.put(data)
                pushed += 1
        return pushed


_brokers: dict[str, Broker ] = {}
_async_brokers: dict[str, AsyncBroker] = {}


def _get_broker(namespace: str) -> Broker:
    if namespace not in _brokers:
        _brokers[namespace] = Broker()
    return _brokers[namespace]


def _get_async_broker(namespace: str) -> AsyncBroker:
    if namespace not in _async_brokers:
        _async_brokers[namespace] = AsyncBroker()
    return _async_brokers[namespace]


class StupidPubSub:
    __slots__ = ["broker", "channels", "messages"]

    def __init__(self, namespace: str) -> None:
        self._init(namespace)

    def _init(self, namespace: str):
        self.broker: Broker = _get_broker(namespace)
        self.broker.link(self)
        self.channels: set[str] = set()
        self.messages = Queue()

    def subscribe(self, channel: str) -> None:
        self.channels.add(channel)

    def unsubscribe(self, channel: str = None) -> None:
        if channel:
            self.channels.remove(channel)
        else:
            self.channels.clear()

    def publish(self, channel: str, message: dict | bytes) -> int:
        payload = {"namespace": channel, "data": message}
        published = self.broker.push(payload)
        return published

    def listen(self, timeout: float | None = None) -> dict | bytes:
        return self.messages.get(block=True, timeout=timeout)

    @property
    def subscribed(self) -> bool:
        return bool(self.channels)


class AsyncPubSub(StupidPubSub):
    def _init(self, namespace: str):
        self.broker: AsyncBroker = _get_async_broker(namespace)
        self.broker.link(self)
        self.channels: set[str] = set()
        self.messages = asyncio.Queue()

    async def publish(self, channel: str, message: dict | bytes) -> int:
        payload = {"namespace": channel, "data": message}
        published = await self.broker.push(payload)
        return published

    async def listen(self, timeout: float | None = None) -> dict | bytes:
        try:
            return await asyncio.wait_for(self.messages.get(), timeout)
        except TimeoutError:
            raise Empty
