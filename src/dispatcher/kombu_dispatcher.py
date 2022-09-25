import logging

try:
    import kombu
except ImportError:
    kombu = None

from .ABC import Dispatcher


class KombuDispatcher(Dispatcher):
    """An event dispatcher that uses Kombu as message broker

    This class implements an event dispatcher backend for event sharing across
    multiple processes, using RabbitMQ, Redis or any other messaging mechanism
    supported by 'kombu'.

    :param namespace: The name of the dispatcher the events will be sent from
                      and sent to.
    :param url: The connection URL for the message broker. For example,
                'amqp://guest:guest@localhost:5672//' is used for RabbitMQ
                and 'redis://localhost:6379/' for Redis.
    :param parent_logger: A logging.Logger instance. The dispatcher logger
                          will be set to 'parent_logger.namespace'.
    :param exchange_opt: Options for the kombu exchange.
    """
    def __init__(
            self,
            namespace: str,
            url: str = "memory://",
            parent_logger: logging.Logger = None,
            exchange_options: dict = None,
            queue_options: dict = None,
    ) -> None:
        if kombu is None:
            raise RuntimeError(
                "Install 'kombu' package to use KombuDispatcher"
            )
        super(KombuDispatcher, self).__init__(namespace, parent_logger)
        self.url = url
        self.exchange_options = exchange_options or {}
        self.queue_options = queue_options or {}
        self.producer = self._producer()

    def _connection(self) -> "kombu.Connection":
        return kombu.Connection(self.url)

    def _channel(self, connection):
        return connection.channel()

    def _exchange(self) -> "kombu.Exchange":
        options = {"durable": False}
        options.update({**self.exchange_options})
        name = options.pop("name", "dispatcher")
        return kombu.Exchange(name, **options)

    def _queue(self, exchange) -> "kombu.Queue":
        options = {**self.queue_options}
        name = options.pop("name", self.namespace)
        routing_keys = [name]
        extra_routing_keys = options.pop("extra_routing_keys", [])
        if isinstance(extra_routing_keys, str):
            extra_routing_keys = [extra_routing_keys]
        routing_keys += extra_routing_keys
        if name != self.namespace:
            routing_keys += [self.namespace]
        return kombu.Queue(
            name=name, bindings=[
                kombu.binding(exchange, routing_key=key)
                for key in routing_keys
            ], **options
        )

    def _producer(self) -> "kombu.Producer":
        return self._connection().Producer(exchange=self._exchange())

    def initialize(self):
        try:
            self._connection().connect()
        except Exception as e:
            self.logger.error(
                f"Encountered an error while connecting to the server: Error msg: "
                f"`{e.__class__.__name__}: {e}`."
            )
        else:
            self._handle_connect()

    def _error_callback(self, exception, interval):
        self.logger.exception(f"Sleeping {interval}s")

    def _publish(self, namespace: str, payload: bytes):
        connection = self._connection()
        publish = connection.ensure(
            self.producer, self.producer.publish, errback=self._error_callback
        )
        publish(payload, routing_key=namespace)

    def _listen(self):
        reader_queue = self._queue(self._exchange())
        connection = self._connection().ensure_connection(
            errback=self._error_callback
        )
        while self._running.is_set():
            try:
                with connection.SimpleQueue(reader_queue) as queue:
                    while True:
                        message = queue.get(block=True)
                        message.ack()
                        yield message.payload
            except Exception as e:
                self.logger.exception(
                    f"Error while reading from queue. Error msg: {e.args}"
                )
