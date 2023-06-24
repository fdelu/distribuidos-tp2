from typing import TypeVar, Callable, Type, Protocol
import logging

T = TypeVar("T")


def singleton(cls: Type[T]) -> Callable[[], T]:
    instances: dict[Type[T], T] = {}

    def getinstance() -> T:
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance


class Runner(Protocol):
    def run(self) -> None:
        ...

    def cleanup(self) -> None:
        ...


def process_loop(factory: Callable[[], Runner]) -> None:
    runner = factory()
    while True:
        try:
            runner.run()
            # If we get here, we are exiting gracefully
            break
        except Exception as e:
            logging.error(
                f"Exception in process loop. Restarting process. Details: {e}"
            )
        runner.cleanup()
        runner = factory()
    runner.cleanup()
