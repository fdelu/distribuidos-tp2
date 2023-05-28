from enum import EnumType
import json
import types
from typing import Any, get_args, get_origin, get_type_hints

from .util import (
    SIMPLE_TYPES,
    SerializationError,
    Translated,
    get_type_name,
    verify_type,
)


def serialize_item(item: Any, expected_type: Any) -> Translated:
    if type(expected_type) == type and expected_type in SIMPLE_TYPES:
        verify_type(item, expected_type)
        return item

    if isinstance(expected_type, types.GenericAlias):
        return serialize_generic(item, expected_type)
    if isinstance(expected_type, types.UnionType):
        return serialize_union(item, expected_type)
    if isinstance(expected_type, EnumType):
        return serialize_enum(item, expected_type)

    return serialize_object(item, expected_type)


def serialize_enum(item: Any, expected_type: EnumType):
    verify_type(item, expected_type)
    return item.value


def serialize_union(item: Any, type_info: types.UnionType) -> Translated:
    for t in get_args(type_info):
        if isinstance(t, types.GenericAlias):
            try:  # check if it's this generic type
                return [get_type_name(t), serialize_generic(item, t)]
            except SerializationError:
                pass
        elif type(item) == t:
            return [get_type_name(t), serialize_item(item, t)]

    raise SerializationError(
        f"Union type {type_info} has failed to serialize: {type(item)} not in union"
        " type"
    )


def serialize_generic(collection: Any, type_info: types.GenericAlias) -> Translated:
    expected_type: Any = get_origin(type_info)
    verify_type(collection, expected_type)
    if expected_type in (list, set):
        item_type = get_args(type_info)[0]
        return [serialize_item(i, item_type) for i in collection]
    if expected_type == dict:
        key_type, value_type = get_args(type_info)
        return [
            [serialize_item(k, key_type), serialize_item(v, value_type)]
            for k, v in collection.items()
        ]
    if expected_type == tuple:
        item_types = get_args(type_info)
        return [serialize_item(i, t) for i, t in zip(collection, item_types)]
    raise SerializationError(f"Generic type {expected_type} is not supported")


def serialize_object(data: object, expected_type: type) -> Translated:
    verify_type(data, expected_type)
    type_hints = get_type_hints(type(data))
    out = []
    for name, t in type_hints.items():
        out.append(serialize_item(getattr(data, name), t))
    return out


def serialize(
    data: object,
    set_type: Any | None = None,
    indent: int | None = None,
) -> str:
    return json.dumps(
        serialize_item(data, set_type if set_type else type(data)), indent=indent
    )
