from enum import EnumType
import json
import types
from typing import Any, Type, TypeVar, get_args, get_origin, get_type_hints, cast, Union

from .util import SIMPLE_TYPES, SerdeError, Translated

T = TypeVar("T")
TypeVars = dict[str, Type[Any]]


def deserialize_item(data_type: Any, data: Translated, type_vars: TypeVars) -> Any:
    if data_type in SIMPLE_TYPES:
        return deserialize_simple(data_type, data)

    origin = get_origin(data_type)
    if isinstance(data_type, types.UnionType) or origin is Union:
        return deserialize_union(data_type, data, type_vars)

    if origin:
        return deserialize_generic(origin, get_args(data_type), data, type_vars)
    if isinstance(data_type, EnumType):
        return deserialize_enum(data_type, data)
    if isinstance(data_type, TypeVar):
        return deserialize_item(type_vars[data_type.__name__], data, type_vars)
    return deserialize_object(data_type, data, type_vars)


def deserialize_simple(data_type: type, data: Translated) -> Any:
    if (
        data_type == float
        and isinstance(data_type, (int, float))
        or type(data) == data_type
    ):
        return data
    raise SerdeError(f"Expected {data_type}, got {type(data)}")


def deserialize_enum(type_info: EnumType, data: Translated) -> Any:
    return type_info(data)


def deserialize_union(type_info: Any, data: Translated, tv: TypeVars) -> Any:
    for t in get_args(type_info):
        try:
            return deserialize_item(t, data, tv)
        except SerdeError:
            pass
    raise SerdeError(
        f"Union type {type_info} has failed to deserialize as any of its types."
    )


def add_type_vars(base: type, args: tuple[Any, ...], tv: TypeVars) -> TypeVars:
    type_vars = tv.copy()
    if not hasattr(base, "__orig_bases__"):
        return type_vars

    for parent in getattr(base, "__orig_bases__"):
        for n, arg in enumerate(get_args(parent)):
            if not isinstance(args[n], TypeVar):
                type_vars[arg.__name__] = args[n]
    return type_vars


def deserialize_generic(
    origin: type, args: tuple[Any, ...], data: Translated, tv: TypeVars
) -> Any:
    updated_tv = add_type_vars(origin, args, tv)
    if origin in (list, set, dict, tuple):
        item = deserialize_collection(origin, args, data, updated_tv)
    else:
        item = deserialize_item(origin, data, updated_tv)
    return item


def deserialize_collection(
    origin: type, args: tuple[Any, ...], data: Translated, tv: TypeVars
) -> Any:
    if not isinstance(data, list):
        raise SerdeError(f"Can't deserialize {origin}: serialized object is not a list")

    data = cast(list[Translated], data)
    if origin in (list, set):
        item_type = args[0]
        values = origin(deserialize_item(item_type, i, tv) for i in data)
        return values
    if origin == dict:
        key_type, value_type = args
        values = {}
        for pair in data:
            if not isinstance(pair, list) or len(pair) != 2:
                raise SerdeError("Dicts must be serialized as lists of pairs")
            pair = cast(list[Translated], pair)
            k, v = pair
            values[deserialize_item(key_type, k, tv)] = deserialize_item(
                value_type, v, tv
            )
        return values
    if origin == tuple:
        item_types = args
        values = tuple(deserialize_item(t, d, tv) for t, d in zip(item_types, data))
        return values
    raise SerdeError(f"Unknown collection type {origin}")


def deserialize_object(data_type: Type[T], data: Translated, type_vars: TypeVars) -> T:
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
        item = deserialize_item(item_type, data[i], type_vars)
        object.__setattr__(out, name, item)
    return out


def deserialize(data_type: Any, data: str) -> Any:
    type_vars: TypeVars = {}
    return deserialize_item(data_type, json.loads(data), type_vars)
