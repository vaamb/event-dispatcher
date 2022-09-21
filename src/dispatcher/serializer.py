from __future__ import annotations

from datetime import date, datetime, time, timezone
import json
import pickle
from typing import Any
import uuid

try:
    import orjson
except ImportError:
    orjson = None


def _json_serializer(o) -> str:
    if isinstance(o, datetime):
        return o.replace(tzinfo=timezone.utc).isoformat(timespec="seconds")
    if isinstance(o, date):
        return o.isoformat()
    if isinstance(o, time):
        return (
            datetime.combine(date.today(), o).replace(tzinfo=timezone.utc)
            .isoformat(timespec="seconds")
        )
    if isinstance(o, uuid.UUID):
        return str(o)


class Serializer:
    @staticmethod
    def dumps(obj: Any) -> bytes:
        if isinstance(obj, bytes):
            return pickle.dumps(obj)
        if orjson:
            return orjson.dumps(obj)
        else:
            str_obj: str = json.dumps(obj, default=_json_serializer)
            return str_obj.encode("utf8")

    @staticmethod
    def loads(obj: bytes) -> Any:
        try:
            return pickle.loads(obj)
        except pickle.UnpicklingError:
            if orjson:
                return orjson.loads(obj)
            else:
                str_obj = obj.decode("utf8")
                return json.loads(str_obj)
