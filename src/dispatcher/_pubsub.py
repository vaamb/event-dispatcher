"""A simple in-memory pub_sub message broker to be used by the dispatcher."""

from __future__ import annotations

import asyncio
from queue import Queue
from typing import Iterable, AsyncIterable


class Broker:
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


_broker = Broker()
_async_broker = AsyncBroker()


class StupidPubSub:
    def __init__(self, broker: Broker = None) -> None:
        if not broker:
            self.broker = _broker
        else:
            if isinstance(broker, Broker):
                self.broker = broker
            else:
                raise TypeError("broker needs to be an instance of Broker()")
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

    def listen(self) -> Iterable[dict]:
        while self.subscribed:
            response = self.messages.get(block=False)
            if response is not None:
                yield response

    @property
    def subscribed(self) -> bool:
        return bool(self.channels)


class AsyncPubSub(StupidPubSub):
    def __init__(self, broker: AsyncBroker = None):
        if not broker:
            broker = _async_broker
        super().__init__(broker)
        self.messages = asyncio.Queue()

    async def publish(self, channel: str, message: dict | bytes) -> int:
        payload = {"namespace": channel, "data": message}
        published = await self.broker.push(payload)
        return published

    async def listen(self) -> AsyncIterable[dict]:
        while self.subscribed:
            response = await self.messages.get()
            if response is not None:
                yield response
