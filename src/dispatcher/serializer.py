from __future__ import annotations

from datetime import date, datetime, time, timezone
import pickle
from typing import Any
import uuid
import warnings


try:
    import orjson
except ImportError:
    warnings.warn("The dispatcher could be faster if orjson was installed")

    orjson = None
    import json

    def _serializer(o) -> str:
        if isinstance(o, datetime):
            return o.astimezone(tz=timezone.utc).isoformat(timespec="seconds")
        if isinstance(o, date):
            return o.isoformat()
        if isinstance(o, time):
            return (
                datetime.combine(date.today(), o)
                .astimezone(tz=timezone.utc)
                .isoformat(timespec="seconds")
            )
        if isinstance(o, uuid.UUID):
            return str(o)

    def json_dumps(obj: Any) -> bytes:
        str_obj: str = json.dumps(obj, default=_serializer)
        return str_obj.encode("utf8")

    def json_loads(obj: bytes | str) -> Any:
        return json.loads(obj)
else:
    def _orjson_default(o):
        if (
            isinstance(o, tuple)
            and hasattr(o, "_fields")
            # and hasattr(o, "_dict")  # Two first checks should be sufficient
        ):
            return tuple(o)

    def json_dumps(obj: Any) -> bytes:
        return orjson.dumps(obj, default=_orjson_default)

    def json_loads(obj: bytes | str) -> Any:
        return orjson.loads(obj)


class Serializer:
    @staticmethod
    def dumps(obj: Any) -> bytes:
        if isinstance(obj, bytes):
            return pickle.dumps(obj)
        return json_dumps(obj)

    @staticmethod
    def loads(obj: bytes | str) -> Any:
        try:
            return pickle.loads(obj)
        except (pickle.UnpicklingError, TypeError):
            return json_loads(obj)
