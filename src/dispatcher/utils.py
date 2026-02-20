from __future__ import annotations

from .ABC import AsyncDispatcher, Dispatcher


try:
    import kombu
except ImportError:
    kombu = None


class RegisterEventMixin:
    def register_dispatcher_events(self, dispatcher: AsyncDispatcher | Dispatcher) -> None:
        """Register the methods starting by "dispatch_" as an event handler"""
        for key in dir(self):
            if key.startswith("dispatch_"):
                event = key.replace("dispatch_", "")
                callback = getattr(self, key)
                dispatcher.on(event, callback)
