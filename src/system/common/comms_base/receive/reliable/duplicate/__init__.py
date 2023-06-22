from typing import Generic, TypeVar, Any, Protocol
from abc import abstractmethod, ABC

from common.comms_base.protocol import CommsProtocol
from common.messages.comms import PackageHandler
from common.persistence import StatePersistor


RECEIVED_MESSAGES_KEY = "_received_messages"

IN = TypeVar("IN", covariant=True)
T = TypeVar("T", contravariant=True)


class PackageComms(PackageHandler[T], CommsProtocol, Protocol[T]):
    @property
    def in_type(self) -> Any:
        pass


def x(c: PackageComms[str | int]) -> None:
    y(c)


def y(c: PackageComms[str]) -> None:
    ...


class DuplicateFilter(Generic[IN], ABC):
    received_messages: set[str]
    comms: PackageComms[IN]

    def __init__(self, package_handler: PackageComms[IN]) -> None:
        self.received_messages = (
            StatePersistor().load(RECEIVED_MESSAGES_KEY, set[str]) or set()
        )
        self.comms = package_handler

    def _ack(self, delivery_tag: int | None) -> None:
        if delivery_tag is not None:
            self.comms.channel.basic_ack(delivery_tag)

    @abstractmethod
    def received_message(
        self, message: str, queue: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        ...

    @abstractmethod
    def pending_count(self, queue: str) -> int:
        """
        Returns the amount of pending checks for the given queue.
        """
        ...
