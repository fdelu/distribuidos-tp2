from typing import get_args, get_origin, TypeVar
import os


T = TypeVar("T")

STATUS_FILE = os.getenv("STATUS_FILE", "status.txt")


def get_generic_type(self: T, type: type, index: int) -> type:
    return get_args(
        next(x for x in self.__orig_bases__ if get_origin(x) == type)  # type: ignore
    )[index]


def set_healthy(message: str) -> None:
    with open(STATUS_FILE, "w") as f:
        f.write(message)
