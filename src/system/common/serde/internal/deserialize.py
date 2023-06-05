from enum import EnumType
import json
import types
from typing import Any, Type, TypeVar, get_args, get_origin, cast, Union

from .util import SIMPLE_TYPES, SerdeError, Translated, get_object_types

T = TypeVar("T")
TypeVars = dict[str, Type[Any]]


def deserialize_item(data_type: Any, data: Translated) -> Any:
    if data_type in SIMPLE_TYPES:
        return deserialize_simple(data_type, data)

    origin = get_origin(data_type)
    if isinstance(data_type, types.UnionType) or origin is Union:
        return deserialize_union(data_type, data)
    if origin in (list, set, dict, tuple):
        return deserialize_collection(data_type, data)
    if isinstance(data_type, EnumType):
        return deserialize_enum(data_type, data)
    return deserialize_object(data_type, data)


def deserialize_simple(data_type: type, data: Translated) -> Any:
    if data_type == float and isinstance(data, (int, float)) or type(data) == data_type:
        return data
    raise SerdeError(f"Expected {data_type}, got {type(data)}")


def deserialize_enum(type_info: EnumType, data: Translated) -> Any:
    return type_info(data)


def deserialize_union(type_info: Any, data: Translated) -> Any:
    for t in get_args(type_info):
        try:
            return deserialize_item(t, data)
        except SerdeError:
            pass
    raise SerdeError(
        f"Union type {type_info} has failed to deserialize as any of its types."
    )


def deserialize_collection(data_type: type, data: Translated) -> Any:
    origin = get_origin(data_type)
    args = get_args(data_type)
    if not isinstance(data, list):
        raise SerdeError(f"Can't deserialize {origin}: serialized object is not a list")

    data = cast(list[Translated], data)
    if origin in (list, set):
        item_type = args[0]
        values = origin(deserialize_item(item_type, i) for i in data)
        return values
    if origin == dict:
        key_type, value_type = args
        values = {}
        for pair in data:
            if not isinstance(pair, list) or len(pair) != 2:
                raise SerdeError("Dicts must be serialized as lists of pairs")
            pair = cast(list[Translated], pair)
            k, v = pair
            values[deserialize_item(key_type, k)] = deserialize_item(value_type, v)
        return values
    if origin == tuple:
        item_types = args
        values = tuple(deserialize_item(t, d) for t, d in zip(item_types, data))
        return values
    raise SerdeError(f"Unknown collection type {origin}")


def deserialize_object(data_type: Type[T], data: Translated) -> T:
    type_hints = get_object_types(data_type)
    object_cls = get_origin(data_type) or data_type
    out = object_cls.__new__(object_cls)
    if not isinstance(data, list):
        raise SerdeError(f"Object {object_cls} must be serialized as a list")
    data = cast(list[Translated], data)
    if len(data) != len(type_hints) + 1:
        raise SerdeError(
            f"Expected {len(type_hints) + 1} items, but got {len(data)} for type"
            f" {object_cls}"
        )
    if data[0] != object_cls.__name__:
        raise SerdeError(
            f"Object name {object_cls.__name__} does not match serialized name"
            f" {data[0]}"
        )
    for i, (name, item_type) in enumerate(type_hints.items(), start=1):
        item = deserialize_item(item_type, data[i])
        object.__setattr__(out, name, item)
    return out


def deserialize(data_type: Type[Any], data: str) -> Any:
    return deserialize_item(data_type, json.loads(data))
