import logging
import pickle

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
            exchange_opt: dict = None
    ) -> None:
        if kombu is None:
            raise RuntimeError(
                "Install 'kombu' package to use KombuDispatcher"
            )
        super(KombuDispatcher, self).__init__(namespace, parent_logger)
        self.url = url
        self.exchange_opt = exchange_opt or {}

    def _exchange(self):
        opts = {"type": "direct", "durable": False}
        opts.update(self.exchange_opt)
        return kombu.Exchange(**opts)

    def _queue(self):
        return kombu.Queue(
            name=self.namespace, exchange=self._exchange(),
            routing_key=self.namespace, durable=False
        )

    def _connection(self):
        return kombu.Connection(self.url)

    def _producer(self):
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
            self._trigger_event("connect")

    def _parse_payload(self, payload: bytes) -> dict:
        return pickle.loads(payload)

    def _error_callback(self, exception, interval):
        self.logger.exception(f"Sleeping {interval}s")

    def _publish(self, namespace: str, payload: dict):
        message = pickle.dumps(payload)
        connection = self._connection()
        producer = self._producer()
        publish = connection.ensure(
            producer, producer.publish, errback=self._error_callback
        )
        publish(
            message, routing_key=namespace, declare=[self._queue()]
        )

    def _listen(self):
        while True:
            reader_queue = self._queue()
            connection = self._connection().ensure_connection(
                errback=self._error_callback
            )
            try:
                with connection.SimpleQueue(reader_queue) as queue:
                    while True:
                        message = queue.get(block=True)
                        message.ack()
                        yield message.payload
            except connection.connection_errors:
                self.logger.exception(
                    "Connection error while reading from queue"
                )
