from typing import TypeVar, Callable, Type, Protocol
import logging
import os
import random
import math

T = TypeVar("T")
SELF_DESTRUCT_KEY_PREFIX = "SELF_DESTRUCT_"


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
    logging.info("Starting process loop")
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


def register_self_destruct(key: str) -> None:
    """
    Registers a key to be used for self destructing the process. The probability
    is read from the environment variable SELF_DESTRUCT_{key.upper()}.
    """
    probability = os.environ.get(f"{SELF_DESTRUCT_KEY_PREFIX}{key.upper()}", None)
    if probability is None:
        return
    number = random.random()
    if number < float(probability):
        decimals = -math.floor(math.log10(float(probability))) + 1
        logging.critical(
            f"{key} | Randomly self destructing ({number:.{decimals}f} < {probability})"
        )
        os._exit(1)
