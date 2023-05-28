from typing import TypeVar, Generic
from abc import ABC, abstractmethod


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

    def send(self, record: OUT) -> None:
        exchange, routing_key = self._get_routing_details(record)
        self.__send_to(record, exchange, routing_key)

    def _serialize_record(self, message: OUT) -> str:
        return serialize(message, set_type=self.out_type)

    def __send_to(self, record: OUT, exchange: str, routing_key: str) -> None:
        self.channel.basic_publish(
            exchange,
            routing_key,
            self._serialize_record(record).encode(),
        )

    @abstractmethod
    def _get_routing_details(self, record: OUT) -> tuple[str, str]:
        ...
