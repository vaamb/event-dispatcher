from __future__ import annotations

import inspect
import logging
from typing import Type, Union

from .async_base_dispatcher import AsyncBaseDispatcher
from .async_amqp_dispatcher import AsyncAMQPDispatcher
from .async_redis_dispatcher import AsyncRedisDispatcher
from .base_dispatcher import BaseDispatcher
from .kombu_dispatcher import KombuDispatcher
from .ABC import AsyncDispatcher, Dispatcher


try:
    import kombu
except ImportError:
    kombu = None


_BROKER_CREATED = False
_BROKER_REACHABLE = 2
MESSAGE_BROKER_URL = "memory://"

KOMBU_SUPPORTED = (
    "amqp", "amqps", "pyamqp", "librabbitmq", "memory", "redis", "rediss",
    "SQS", "sqs", "mongodb", "zookeeper", "sqlalchemy", "sqla", "SLMQ", "slmq",
    "filesystem", "qpid", "sentinel", "consul", "etcd", "azurestoragequeues",
    "azureservicebus", "pyro"
)

default_logger = logging.getLogger("dispatcher")


def _get_url(config: Union[dict, None, Type]) -> str:
    if config is None:
        return MESSAGE_BROKER_URL
    elif isinstance(config, dict):
        return config.get("MESSAGE_BROKER_URL", MESSAGE_BROKER_URL)
    elif inspect.isclass(config):
        return vars(config).get("MESSAGE_BROKER_URL", MESSAGE_BROKER_URL)
    else:
        raise ValueError("config must either be a class or a dict")


def configure_dispatcher(
        config: Union[dict, Type],
        silent: bool = True,
) -> None:
    """Set dispatcher configuration

    :param config: A class or dict containing at least the 'MESSAGE_BROKER_URL'
                   field
    :param silent: Log the warning when a dispatcher was already created
    """

    global _BROKER_CREATED, _BROKER_REACHABLE,  MESSAGE_BROKER_URL
    if not _BROKER_CREATED:
        MESSAGE_BROKER_URL = _get_url(config)
    elif _BROKER_CREATED and not silent:
        default_logger.warning(
            "It is not recommended to configure dispatchers once a dispatcher "
            "has been created. If you want to override this behavior, use "
            "'override=True'."
        )


def get_dispatcher(
        namespace: str,
        config:  Union[dict, None, Type] = None,
        logger: logging.Logger = False,
        async_based: bool = False,
) -> AsyncDispatcher | Dispatcher:
    """Get the required dispatcher

    In case the dispatcher relies on a backend server (on Redis for example),
    this method checks if the server is reachable before returning the required
    dispatcher. If it is not reachable, it will return a basic in memory dispatcher.

    :param namespace: The name of the namespace the events will be sent from
                      and sent to
    :param config: A class or dict containing at least the 'MESSAGE_BROKER_URL'
                   field
    :param logger: A logging.Logger instance
    :param async_based: Whether to return an async version of the dispatcher
    :return: A dispatcher instance
    """
    global _BROKER_CREATED, _BROKER_REACHABLE
    if isinstance(logger, logging.Logger):
        logger = logger
    else:
        logger = default_logger
    url = _get_url(config)
    server = url[:url.index("://")]
    if server == "memory":
        if not async_based:
            return BaseDispatcher(namespace, parent_logger=logger)
        else:
            return AsyncBaseDispatcher(namespace, parent_logger=logger)
    elif server == "kombuMemory":
        if kombu is None:
            raise RuntimeError(
                "Install 'kombu' package to use 'kombuMemory' as message broker"
            )
        return KombuDispatcher(
            namespace, "memory://", parent_logger=logger,
            exchange_opt={"name": "gaia"}
        )
    elif server == "redis" and async_based:
        return AsyncRedisDispatcher(namespace, url, parent_logger=logger)
    elif server == "amqp" and async_based:
        return AsyncAMQPDispatcher(namespace, url, parent_logger=logger)
    elif server in KOMBU_SUPPORTED:
        return KombuDispatcher(
            namespace, url, parent_logger=logger,
            exchange_opt={"name": "gaia"}
        )
    else:
        logger.warning(
            f"{server} is not supported. Either use a message broker supported "
            f"by kombu or use 'memory://'. Falling back to in-memory dispatcher"
        )
        return BaseDispatcher(namespace, parent_logger=logger)


class RegisterEventMixin:
    def register_dispatcher_events(self, dispatcher: Dispatcher) -> None:
        """Register the methods starting by "dispatch_" as an event handler"""
        for key in dir(self):
            if key.startswith("dispatch_"):
                event = key.replace("dispatch_", "")
                callback = getattr(self, key)
                dispatcher.on(event, callback)
