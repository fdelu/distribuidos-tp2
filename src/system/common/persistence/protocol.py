from typing import Protocol
from abc import abstractmethod


class WithStateProtocol(Protocol):
    @abstractmethod
    def restore_from(self, key: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def store_to(self, key: str) -> None:
        raise NotImplementedError()
