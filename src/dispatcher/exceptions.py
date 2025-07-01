class EventDispatcherError(RuntimeError):
    """Base class for all event dispatcher exceptions."""
    pass


class StopEvent(EventDispatcherError):
    """Raised to stop event propagation."""
    pass


class UnknownEvent(EventDispatcherError):
    """Raised when an unknown event is triggered."""
    pass
