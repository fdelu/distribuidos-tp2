from typing import (
    Generic,
    get_args,
    TypeVar,
    get_origin,
    Type,
    Any,
    get_type_hints,
    Union,
)
import types
from functools import reduce

"""
Type alias for the types that can be serialized and deserialized.
"""
Translated = list["Translated"] | str | int | float | bool | None

SIMPLE_TYPES = [str, int, float, types.NoneType, bool]


class SerdeError(Exception):
    """
    Raised when a serialization or deserialization error occurs.
    """

    pass


T = TypeVar("T")
TypeVarMap = dict[TypeVar, Type[Any]]


def map_generics(this: Type[T], args: tuple[Any, ...]) -> TypeVarMap:
    """
    Maps generic TypeVars to their actual types.
    Example usage:
    >>> from typing import Generic, TypeVar
    ...
    >>> A = TypeVar("A")
    >>> B = TypeVar("B")
    ...
    >>> class Example(Generic[A, B]):
    ...     ...
    ...
    >>> map_generics(Example, (int, str))
    {A: int, B: str}
    """
    map: TypeVarMap = {}
    generic = next(
        (t for t in getattr(this, "__orig_bases__", []) if get_origin(t) is Generic),
        None,
    )
    if generic is None:
        return map
    for i, t in enumerate(get_args(generic)):
        map[t] = args[i]
    return map


def map_type(to_map: Type[Any], generic_map: TypeVarMap) -> Any:
    """
    Replace generic types in to_map with their actual types in generic_map.
    Example usage:
    >>> from typing import Generic, TypeVar
    ...
    >>> A = TypeVar("A")
    >>> B = TypeVar("B")
    ...
    >>> map_args(A | B | float, {A: int, B: str})
    int | str | float
    """
    if isinstance(to_map, TypeVar):
        return generic_map[to_map]
    origin = get_origin(to_map)
    if origin is None:
        return to_map
    args = get_args(to_map)
    resolved_args = tuple(map_type(arg, generic_map) for arg in args)
    if origin in (types.UnionType, Union):
        return reduce(lambda x, y: x | y, resolved_args)
    return origin[resolved_args]


def resolve_generic_types(base: Type[Any], parent: Type[Any]) -> tuple[Type[Any], ...]:
    """
    Returns an array of resolved generic types of parent class.
    Args:
        base: class to resolve the generic types of
        parent: parent class of base to search the generic types in
    Example usage:
    >>> from typing import Generic, TypeVar
    ...
    >>> A = TypeVar("A")
    >>> B = TypeVar("B")
    ...
    >>> class Example(Generic[A, B]):
    ...     ...
    ...
    >>> resolve_generic_types(Example[float, str], Example)
    (float, str)
    ...
    >>> class Example2(Example[int, str]):
    ...     ...
    ...
    >>> resolve_generic_types(Example2, Example)
    (int, str)
    """
    return __resolve_generic_types_rec(base, parent, {}) or ()


def get_generic_types(obj: T, parent: Type[Any]) -> tuple[Type[Any], ...]:
    """
    Returns an array of resolved generic types of parent class.
    Does NOT work from within obj.__init__.
    If this object is generic, it must have been instantiated with concrete types
    (e.g. Example[int, str](), not Example()).
    Args:
        obj: object to resolve the generic types of
        parent: parent class of obj to search the generic types in
    Example usage:
    >>> from typing import Generic, TypeVar
    ...
    >>> A = TypeVar("A")
    >>> B = TypeVar("B")
    ...
    >>> class Example(Generic[A, B]):
    ...     ...
    ...
    >>> resolve_generic_types(Example[float, str](), Example)
    (float, str)
    ...
    >>> class Example2(Example[int, str]):
    ...     ...
    ...
    >>> resolve_generic_types(Example2(), Example)
    (int, str)
    """
    return resolve_generic_types(getattr(obj, "__orig_class__", type(obj)), parent)


def get_object_types(object_type: Type[Any]) -> dict[str, Type[Any]]:
    """
    Like typing.get_type_hints, but also resolves generic types.
    Does NOT work from within obj.__init__.
    If this object is generic, it must have been instantiated with concrete types
    (e.g. Example[int, str](), not Example()).
    """
    map: dict[str, Type[Any]] = {}
    __resolve_object_types_rec(object_type, object_type, map)
    return map


def __resolve_generic_types_rec(
    base: Type[T], parent: Type[Any], parent_map: TypeVarMap
) -> tuple[Type[Any], ...] | None:
    args = get_args(base)
    this = get_origin(base) or base

    if not hasattr(this, "__orig_bases__"):
        return None  # not generic

    args = tuple(map_type(x, parent_map) for x in args)
    generic_map = map_generics(this, args)

    if this is parent:
        return args

    for t in getattr(this, "__orig_bases__"):
        rec = __resolve_generic_types_rec(t, parent, generic_map)
        if rec is not None:
            return rec
    return None


def __resolve_object_types_rec(
    base_type: Type[Any], current_type: Type[Any], type_map: dict[str, Type[Any]]
) -> None:
    this = get_origin(current_type) or current_type
    type_hints = get_type_hints(this)

    resolved_args = resolve_generic_types(base_type, this)
    gen_map = map_generics(this, resolved_args)

    for t in getattr(this, "__orig_bases__", []):
        if get_origin(t) == Generic:
            continue
        __resolve_object_types_rec(base_type, t, type_map)

    for name, item_type in type_hints.items():
        if name in type_map:
            continue
        type_map[name] = map_type(item_type, gen_map)
