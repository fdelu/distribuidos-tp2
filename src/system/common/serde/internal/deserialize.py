from enum import EnumType
import json
import types
from typing import Any, Type, TypeVar, get_args, get_origin, get_type_hints, cast

from .util import (
    SIMPLE_TYPES,
    SerdeError,
    Translated,
    verify_type,
)

T = TypeVar("T")


def deserialize_item(data_type: Any, data: Translated) -> Any:
    if data_type in SIMPLE_TYPES:
        verify_type(data, data_type)
        return data
    if isinstance(data_type, types.GenericAlias):
        return deserialize_generic(data_type, data)
    if isinstance(data_type, types.UnionType):
        return deserialize_union(data_type, data)
    if isinstance(data_type, EnumType):
        return deserialize_enum(data_type, data)
    return deserialize_object(data_type, data)


def deserialize_enum(type_info: EnumType, data: Translated) -> Any:
    return type_info(data)


def deserialize_union(type_info: types.UnionType, data: Translated) -> Any:
    for t in get_args(type_info):
        try:
            return deserialize_item(t, data)
        except SerdeError:
            pass
    raise SerdeError(
        f"Union type {type_info} has failed to deserialize as any of its types."
    )


def deserialize_generic(type_info: types.GenericAlias, data: Translated) -> Any:
    expected_type: Any = get_origin(type_info)
    if not isinstance(data, list):
        raise SerdeError(f"Generic type {expected_type} must be serialized as a list")
    data = cast(list[Translated], data)
    if expected_type in (list, set):
        item_type = get_args(type_info)[0]
        values = expected_type(deserialize_item(item_type, i) for i in data)
        return values
    if expected_type == dict:
        key_type, value_type = get_args(type_info)
        values = {}
        for pair in data:
            if not isinstance(pair, list) or len(pair) != 2:
                raise SerdeError("Dicts must be serialized as lists of pairs")
            pair = cast(list[Translated], pair)
            k, v = pair
            values[deserialize_item(key_type, k)] = deserialize_item(value_type, v)
        return values
    if expected_type == tuple:
        item_types = get_args(type_info)
        values = tuple(deserialize_item(t, d) for t, d in zip(item_types, data))
        return values
    raise SerdeError(f"Generic type {expected_type} is not supported")


def deserialize_object(data_type: Type[T], data: Translated) -> T:
    type_hints = get_type_hints(data_type)
    out = data_type.__new__(data_type)
    if not isinstance(data, list):
        raise SerdeError(f"Object {data_type} must be serialized as a list")
    data = cast(list[Translated], data)
    if len(data) != len(type_hints) + 1:
        raise SerdeError(
            f"Expected {len(type_hints) + 1} items, but got {len(data)} for type"
            f" {data_type}"
        )
    if data[0] != data_type.__name__:
        raise SerdeError(
            f"Object name {data_type.__name__} does not match serialized name {data[0]}"
        )
    for i, (name, item_type) in enumerate(type_hints.items(), start=1):
        item = deserialize_item(item_type, data[i])
        verify_type(item, item_type)
        object.__setattr__(out, name, item)
    return out


def deserialize(data_type: Any, data: str) -> Any:
    return deserialize_item(data_type, json.loads(data))
