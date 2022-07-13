import inspect
import logging
from typing import Type, Union

try:
    import kombu
except ImportError:
    kombu = None

from .base_dispatcher import BaseDispatcher
from .kombu_dispatcher import KombuDispatcher
from .template import DispatcherTemplate


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
        override: bool = False,
        silent: bool = True,
) -> None:
    """Set dispatcher configuration

    :param config: A class or dict containing at least the 'MESSAGE_BROKER_URL'
                   field
    :param override: Override the configuration in case a dispatcher was already
                     created
    :param silent: Log the warning when a dispatcher was already created
    """

    global _BROKER_CREATED, _BROKER_REACHABLE,  MESSAGE_BROKER_URL
    if not _BROKER_CREATED or override:
        MESSAGE_BROKER_URL = _get_url(config)
        _BROKER_REACHABLE = 2
    elif _BROKER_CREATED and not silent:
        logger.warning(
            "It is not recommended to configure dispatchers once a dispatcher "
            "has been created. If you want to override this behavior, use "
            "'override=True'."
        )


def get_dispatcher(
        namespace: str,
        config:  Union[dict, None, Type] = None,
        logger: logging.Logger = False,
) -> DispatcherTemplate:
    """Get the required dispatcher

    In case the dispatcher relies on a backend server (on Redis for example),
    this method checks if the server is reachable before returning the required
    dispatcher. If it is not reachable, it will return a basic in memory dispatcher.

    :param namespace: The name of the namespace the events will be sent from
                      and sent to
    :param config: A class or dict containing at least the 'MESSAGE_BROKER_URL'
                   field
    :param logger: A logging.Logger instance
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
        return BaseDispatcher(namespace, parent_logger=logger)
    elif server == "kombuMemory":
        if kombu is None:
            raise RuntimeError(
                "Install 'kombu' package to use 'kombuMemory' as message broker"
            )
        return KombuDispatcher(
            namespace, "memory://", parent_logger=logger,
            exchange_opt={"name": "gaia"}
        )
    elif server in KOMBU_SUPPORTED:
        if _BROKER_REACHABLE == 2:
            if kombu is None:
                raise RuntimeError(
                    f"Install 'kombu' package to use '{server}' as message broker"
                )
            conn = kombu.Connection(url)
            try:
                conn.connection()
                _BROKER_REACHABLE = 1
            except Exception as e:
                logger.warning(
                    f"Cannot connect to {server} server, using base dispatcher "
                    f"instead. Error msg: {e.__class__.__name__}: {e}"
                )
                _BROKER_REACHABLE = 0
        if _BROKER_REACHABLE is True:
            return KombuDispatcher(
                namespace, url, parent_logger=logger,
                exchange_opt={"name": "gaia"}
            )
        return BaseDispatcher(namespace, parent_logger=logger)
    else:
        logger.warning(
            f"{server} is not supported. Either use a message broker supported "
            f"by kombu or use 'memory://'. Falling back to in-memory dispatcher"
        )
        return BaseDispatcher(namespace, parent_logger=logger)


class RegisterEventMixin:
    def register_dispatcher_events(self, dispatcher: DispatcherTemplate) -> None:
        """Register the methods starting by "dispatch_" as an event handler"""
        for key in dir(self):
            if key.startswith("dispatch_"):
                event = key.replace("dispatch_", "")
                callback = getattr(self, key)
                dispatcher.on(event, callback)
