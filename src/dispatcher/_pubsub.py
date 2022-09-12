"""A simple in-memory pub_sub message broker to be used by the dispatcher."""

from queue import Queue
from typing import Iterable, AsyncIterable


class Broker:
    def __init__(self) -> None:
        self.clients: set["StupidPubSub"] = set()

    def link(self, client: "StupidPubSub") -> None:
        self.clients.add(client)

    def push(self, payload: dict) -> int:
        pushed = 0
        for client in self.clients:
            if payload["namespace"] in client.channels:
                data = payload["data"]
                client.messages.put(data)
                pushed += 1
        return pushed


_broker = Broker()


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

    def publish(self, channel: str, message: dict) -> int:
        payload = {"namespace": channel, "data": message}
        published = self.broker.push(payload)
        return published

    def listen(self) -> Iterable[dict]:
        while self.subscribed:
            response = self.messages.get()
            if response is not None:
                yield response

    @property
    def subscribed(self) -> bool:
        return bool(self.channels)


class AsyncPubSub(StupidPubSub):
    async def publish(self, channel: str, message: dict) -> int:
        payload = {"namespace": channel, "data": message}
        published = self.broker.push(payload)
        return published

    async def listen(self) -> AsyncIterable[dict]:
        while self.subscribed:
            response = self.messages.get()
            print(1)
            if response is not None:
                yield response
