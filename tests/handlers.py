import asyncio
import queue
import time
from uuid import UUID

from dispatcher import AsyncEventHandler, EventHandler


class PingPong(EventHandler):
    """Records every event it receives and answers `ping` with `pong`.

    `namespace` is the namespace the *replies* are sent to, so a PingPong
    registered on a dispatcher listening on "server" should be created with
    `namespace="client"` (the same way gaia's handler emits to the aggregator
    namespace while its dispatcher listens on its own).
    """
    def __init__(self, namespace: str = "event_dispatcher") -> None:
        super().__init__(namespace)
        self.events: queue.Queue[tuple[str, UUID, object]] = queue.Queue()

    def expect(self, event: str, timeout: float = 2.0) -> tuple[UUID, object]:
        """Wait for `event`, discarding anything received before it.

        Raises `TimeoutError` if the event does not show up in time.
        """
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            try:
                name, sid, data = self.events.get(timeout=max(remaining, 0))
            except queue.Empty:
                raise TimeoutError(f"No '{event}' event within {timeout} s")
            if name == event:
                return sid, data

    # Lifecycle events are triggered by the dispatcher itself
    def on_connect(self, sid: UUID, data: dict) -> None:
        self.events.put(("connect", sid, data))

    def on_disconnect(self, sid: UUID) -> None:
        self.events.put(("disconnect", sid, None))

    def on_ping(self, sid: UUID, data: object) -> None:
        self.events.put(("ping", sid, data))
        self.emit("pong", data, to=sid)

    def on_pong(self, sid: UUID, data: object) -> None:
        self.events.put(("pong", sid, data))


class AsyncPingPong(AsyncEventHandler):
    """Async counterpart of `PingPong`, see its docstring."""
    def __init__(self, namespace: str = "event_dispatcher") -> None:
        super().__init__(namespace)
        self.events: asyncio.Queue[tuple[str, UUID, object]] = asyncio.Queue()

    async def expect(self, event: str, timeout: float = 2.0) -> tuple[UUID, object]:
        """Wait for `event`, discarding anything received before it.

        Raises `TimeoutError` if the event does not show up in time.
        """
        async def _drain_until() -> tuple[UUID, object]:
            while True:
                name, sid, data = await self.events.get()
                if name == event:
                    return sid, data

        return await asyncio.wait_for(_drain_until(), timeout)

    # Lifecycle events are triggered by the dispatcher itself
    async def on_connect(self, sid: UUID, data: dict) -> None:
        await self.events.put(("connect", sid, data))

    async def on_disconnect(self, sid: UUID) -> None:
        await self.events.put(("disconnect", sid, None))

    async def on_ping(self, sid: UUID, data: object) -> None:
        await self.events.put(("ping", sid, data))
        await self.emit("pong", data, to=sid)

    async def on_pong(self, sid: UUID, data: object) -> None:
        await self.events.put(("pong", sid, data))
