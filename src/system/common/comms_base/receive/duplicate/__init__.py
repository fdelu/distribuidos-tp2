from typing import Generic, TypeVar, Any, Protocol
from abc import abstractmethod, ABC

from shared.serde import get_generic_types
from common.comms_base.protocol import CommsProtocol
from common.messages.comms import PackageHandler
from common.persistence import StatePersistor


RECEIVED_MESSAGES_KEY = "_received_messages"

IN = TypeVar("IN", covariant=True)
T = TypeVar("T", contravariant=True)


class PackageComms(PackageHandler[T], CommsProtocol, Protocol[T]):
    pass


class DuplicateFilter(Generic[IN], ABC):
    received_messages: set[str]
    comms: PackageComms[IN]
    __in_type: Any | None = None

    @property
    def in_type(self) -> Any:
        """
        Input type (resolved IN TypeVar from DuplicateFilter[IN])
        """
        if self.__in_type is None:
            self.__in_type = get_generic_types(self, DuplicateFilter)[0]
        return self.__in_type

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
        self, message: str, delivery_tag: int | None, redelivered: bool
    ) -> None:
        ...
