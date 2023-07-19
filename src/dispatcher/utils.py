from __future__ import annotations

import inspect
import logging
from typing import Type, Union

from .async_in_memory_dispatcher import AsyncInMemoryDispatcher
from .async_amqp_dispatcher import AsyncAMQPDispatcher
from .async_redis_dispatcher import AsyncRedisDispatcher
from .in_memory_dispatcher import InMemoryDispatcher
from .kombu_dispatcher import KombuDispatcher
from .ABC import AsyncDispatcher, Dispatcher


try:
    import kombu
except ImportError:
    kombu = None


class RegisterEventMixin:
    def register_dispatcher_events(self, dispatcher: Dispatcher) -> None:
        """Register the methods starting by "dispatch_" as an event handler"""
        for key in dir(self):
            if key.startswith("dispatch_"):
                event = key.replace("dispatch_", "")
                callback = getattr(self, key)
                dispatcher.on(event, callback)
