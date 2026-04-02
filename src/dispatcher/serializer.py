from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any
import uuid
import warnings


try:
    import orjson
except ImportError:
    warnings.warn("The dispatcher could be faster if orjson was installed")

    orjson = None  # ty: ignore[invalid-assignment]
    import json

    def _serializer(o) -> str:
        if isinstance(o, datetime):
            return o.astimezone(tz=timezone.utc).isoformat(timespec="seconds")
        if isinstance(o, date):
            return o.isoformat()
        if isinstance(o, time):
            return str(o)
        if isinstance(o, uuid.UUID):
            return str(o)
        raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

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
        return json_dumps(obj)

    @staticmethod
    def loads(obj: bytes | str) -> Any:
        return json_loads(obj)
