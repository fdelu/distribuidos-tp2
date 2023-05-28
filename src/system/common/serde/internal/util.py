import types
from typing import Callable, Concatenate, ParamSpec, get_args, get_origin

"""
Type alias for the types that can be serialized and deserialized.
"""
Translated = list["Translated"] | str | int | float | bool | None

SIMPLE_TYPES = [str, int, float, types.NoneType, bool]

P = ParamSpec("P")
Serializer = Callable[Concatenate[Translated, P], str]
Deserializer = Callable[Concatenate[str, P], Translated]


def verify_type(item: object, expected_type: type):
    """
    Verifies that the given item is of the expected type.

    Does not verify generic types, only the base type.
    """
    if isinstance(expected_type, types.UnionType):
        for t in get_args(expected_type):
            try:
                verify_type(item, t)
                return
            except SerializationError:
                pass

    if isinstance(expected_type, types.GenericAlias):
        base_type = get_origin(expected_type)
        if base_type == type(item):
            return

    if expected_type == float:
        return isinstance(item, (int, float))

    if type(item) == expected_type:
        return

    raise SerializationError(
        f"Expected type {expected_type} but got {type(item)} (value: {item}))"
    )


def get_type_name(type) -> str:
    if isinstance(type, types.GenericAlias):
        return (
            get_type_name(get_origin(type))
            + f"[{', '.join(get_type_name(x) for x in get_args(type))}]"
        )
    return type.__name__


class SerializationError(Exception):
    """
    Raised when a serialization or deserialization error occurs.
    """

    pass
