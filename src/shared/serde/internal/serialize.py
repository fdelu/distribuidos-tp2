from enum import Enum
import json
from typing import Any, get_type_hints

from .util import (
    SIMPLE_TYPES,
    SerdeError,
    Translated,
)


def serialize_item(item: Any) -> Translated:
    if type(item) in SIMPLE_TYPES:
        return item

    if isinstance(item, (list, dict, tuple, set)):
        return serialize_generic(item)
    if isinstance(item, Enum):
        return serialize_enum(item)

    return serialize_object(item)


def serialize_enum(item: Enum) -> Translated:
    if type(item.value) not in SIMPLE_TYPES:
        raise SerdeError(
            f"Enum values must be one of {', '.join(str(x) for x in SIMPLE_TYPES)}"
        )
    return item.value


def serialize_generic(
    collection: list[Any] | dict[Any, Any] | set[Any] | tuple[Any, ...],
) -> Translated:
    if isinstance(collection, (list, set, tuple)):
        return [serialize_item(i) for i in collection]
    if isinstance(collection, dict):
        return [[serialize_item(k), serialize_item(v)] for k, v in collection.items()]
    raise SerdeError(f"Generic type {type(collection)} is not supported")


def serialize_object(data: object) -> Translated:
    type_hints = get_type_hints(type(data))
    out: list[Translated] = [type(data).__name__]
    for name in type_hints:
        out.append(serialize_item(getattr(data, name)))
    return out


def serialize(
    data: object,
    indent: int | None = None,
) -> str:
    return json.dumps(serialize_item(data), indent=indent)
