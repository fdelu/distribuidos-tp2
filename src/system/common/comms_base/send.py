from typing import TypeVar, Generic
from abc import ABC, abstractmethod

from ..messages import Message
from ..serde import serialize
from ..config_base import ConfigBase

from .protocol import CommsProtocol
from .util import get_generic_type

OUT = TypeVar("OUT", contravariant=True)


class CommsSend(CommsProtocol, Generic[OUT], ABC):
    """
    Comms with send capabilities. See protocol.py for more details about the methods.
    """

    out_type: type

    def __init__(self, config: ConfigBase) -> None:
        super().__init__(config)
        self.out_type = get_generic_type(self, CommsSend, 0)

    def send(self, record: Message[OUT]) -> None:
        exchange, routing_key = self._get_routing_details(record)
        self.__send_to(record, exchange, routing_key)

    def __send_to(self, record: Message[OUT], exchange: str, routing_key: str) -> None:
        self.channel.basic_publish(
            exchange,
            routing_key,
            serialize(record).encode(),
        )

    @abstractmethod
    def _get_routing_details(self, record: Message[OUT]) -> tuple[str, str]:
        ...
