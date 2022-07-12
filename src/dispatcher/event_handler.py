from typing import Union


class EventHandler:
    """Base class for class-based event handler.

    A class-based event-handler is a class that contains methods to handle the
    events for a dispatcher.
    """
    def __init__(self):
        self._dispatcher = None

    def __eq__(self, other):
        return self.__dict__.keys() == other.__dict__.keys()

    def __hash__(self):
        return hash(tuple(self.__dict__.keys()))

    def _set_dispatcher(self, dispatcher):
        self._dispatcher = dispatcher

    def trigger_event(self, event: str, *args, **kwargs):
        """Dispatch an event to the correct handler method.

        :param event: The name of the event to handle.
        :param args: Optionnal arguments to be passed to the event handler.
        :param kwargs: Optionnal key word arguments to be passed to the event
                       handler.
        """
        handler = f"on_{event}"
        if hasattr(self, handler):
            return getattr(self, handler)(*args, **kwargs)
        return "__not_triggered__"

    def emit(
            self,
            namespace: Union[list, str, tuple],
            event: str,
            *args,
            **kwargs
    ) -> None:
        """Emit an event to a single or multiple namespace(s)

        :param namespace: The namespace(s) to which the event will be sent.
        :param event: The event name.
        :param args: Optionnal arguments to be passed to the event handler.
        :param kwargs: Optionnal key word arguments to be passed to the event
                       handler.
        """
        if not self._dispatcher:
            raise RuntimeError(
                "You need to register this EventHandler in order to use it"
            )
        if isinstance(namespace, str):
            namespace = namespace.split(",")
        for n in namespace:
            self._dispatcher.emit(n, event, *args, **kwargs)
