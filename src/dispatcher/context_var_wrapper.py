from contextvars import ContextVar


class ContextVarWrapper:
    __slots__ = ("_storage",)

    def __init__(self) -> None:
        object.__setattr__(self, "_storage", ContextVar("local_context"))

    def __iter__(self):
        return iter(self._storage.get({}).items())

    def __getattr__(self, key: str):
        values = self._storage.get({})
        try:
            return values[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key: str, value) -> None:
        values = self._storage.get({}).copy()
        values[key] = value
        self._storage.set(values)

    def __delattr__(self, key: str) -> None:
        values = self._storage.get({}).copy()
        try:
            del values[key]
            self._storage.set(values)
        except KeyError:
            raise AttributeError(key)

