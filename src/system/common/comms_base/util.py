from typing import get_args, get_origin, TypeVar

T = TypeVar("T")


def get_generic_type(self: T, type: type, index: int) -> type:
    return get_args(
        next(x for x in self.__orig_bases__ if get_origin(x) == type)  # type: ignore
    )[index]
