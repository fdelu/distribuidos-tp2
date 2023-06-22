from typing import TypeVar, Callable, Type

T = TypeVar("T")


def singleton(cls: Type[T]) -> Callable[[], T]:
    instances: dict[Type[T], T] = {}

    def getinstance() -> T:
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance
